//! Own the paired WebSocket/TCP and STUN/UDP listener for browser peers.
use ivy::node::{SuppliedDirection, SuppliedStream};
use std::{io, net::SocketAddr};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub(super) struct Listener {
    tcp: TcpListener,
    udp: UdpSocket,
    pub address: SocketAddr,
}
impl Listener {
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        // Like Swift, retry ephemeral TCP choices that are unavailable on UDP.
        let attempts = if address.port() == 0 { 8 } else { 1 };
        for attempt in 0..attempts {
            let tcp = TcpListener::bind(address).await?;
            let actual = tcp.local_addr()?;
            match UdpSocket::bind(actual).await {
                Ok(udp) => {
                    return Ok(Self {
                        tcp,
                        udp,
                        address: actual,
                    });
                }
                Err(error) if attempt + 1 == attempts => return Err(error),
                Err(_) => {}
            }
        }
        unreachable!("at least one bind attempt")
    }
    pub fn start(self, streams: mpsc::Sender<SuppliedStream>) -> Running {
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut upgrades = JoinSet::new();
            let mut stun = JoinSet::new();
            stun.spawn(ivy::stun::serve(self.udp));
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = streams.closed() => break,
                    // A stopped responder must not leave a partial listener running.
                    _ = stun.join_next() => break,
                    _ = upgrades.join_next(), if !upgrades.is_empty() => {},
                    incoming = self.tcp.accept() => {
                        let Ok((socket, observed)) = incoming else { break; };
                        // Match Swift's bound on unauthenticated HTTP upgrades.
                        if upgrades.len() >= 256 { continue; }
                        let streams = streams.clone();
                        let local_host = socket.local_addr().ok().map(|a| a.ip().to_string());
                        upgrades.spawn(async move {
                            if let Ok(stream) = ivy_websocket::accept(socket, "/ivy").await {
                                let _ = streams.send(SuppliedStream {
                                    stream: Box::new(stream),
                                    direction: SuppliedDirection::InboundWithLocal { local_host },
                                    observed_host: Some(observed.ip().to_string()),
                                }).await;
                            }
                        });
                    }
                }
            }
            upgrades.abort_all();
            stun.abort_all();
            while upgrades.join_next().await.is_some() {}
            while stun.join_next().await.is_some() {}
        });
        Running {
            stop: Some(stop),
            task,
        }
    }
}
pub(super) struct Running {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}
impl Running {
    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = (&mut self.task).await;
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::AsyncReadExt,
        net::TcpStream,
        time::{Duration, timeout},
    };

    #[tokio::test]
    async fn paired_listener_authenticates_websocket_answers_stun_and_releases_sockets() {
        timeout(Duration::from_secs(5), async {
            let listener = Listener::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let address = listener.address;
            let (tx, mut rx) = mpsc::channel(1);
            let mut running = listener.start(tx);
            let observed = ivy::stun::query(address).await.unwrap().unwrap();
            assert_eq!(observed.host, "127.0.0.1");
            assert_ne!(observed.port, 0);
            assert!(
                ivy_websocket::connect(&format!("ws://{address}/wrong"))
                    .await
                    .is_err()
            );
            let server_config =
                ivy_core::Config::new(ed25519_dalek::SigningKey::from_bytes(&[11; 32]));
            let server_key = server_config.peer_key();
            let server = tokio::spawn(async move {
                let incoming = rx.recv().await.unwrap();
                assert_eq!(incoming.observed_host.as_deref(), Some("127.0.0.1"));
                let mut peer = ivy::accept(incoming.stream, &server_config, |_| true)
                    .await
                    .unwrap();
                let (topic, bytes) = peer.recv().await.unwrap();
                assert_eq!(topic, "transport-test");
                peer.send("echo", &bytes).await.unwrap();
                rx
            });
            let stream = ivy_websocket::connect(&format!("ws://{address}/ivy"))
                .await
                .unwrap();
            let config = ivy_core::Config::new(ed25519_dalek::SigningKey::from_bytes(&[12; 32]));
            let mut peer = ivy::connect(stream, &config, server_key).await.unwrap();
            peer.send("transport-test", b"authenticated").await.unwrap();
            assert_eq!(
                peer.recv().await.unwrap(),
                ("echo".into(), b"authenticated".to_vec())
            );
            let _rx = server.await.unwrap();
            // A TCP client that never upgrades cannot delay shutdown.
            let mut waiting = TcpStream::connect(address).await.unwrap();
            running.stop().await;
            let mut byte = [0];
            assert!(matches!(waiting.read(&mut byte).await, Ok(0) | Err(_)));
            let _tcp = TcpListener::bind(address).await.unwrap();
            let _udp = UdpSocket::bind(address).await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn udp_conflict_releases_tcp_after_failed_pair_bind() {
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = udp.local_addr().unwrap();
        assert!(Listener::bind(address).await.is_err());
        let _tcp = TcpListener::bind(address).await.unwrap();
    }
}
