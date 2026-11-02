use super::*;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn browser_peer_completes_overlay_hello_and_sees_external_tcp_endpoint() {
    timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut network = Network::bind_with_overlay(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[31; 32]),
            "root".into(),
            storage,
            lattice_tally::Tally::default(),
            vec![],
            OverlayConfig {
                resources: Default::default(),
                websocket: WebSocketListener::Required("127.0.0.1:0".parse().unwrap()),
                external_address: Some("203.0.113.7".parse().unwrap()),
            },
        )
        .await
        .unwrap();
        let ws = network.websocket_address.unwrap();
        let port = network.address.port();
        let key = ivy_core::PeerKey::from_hex(&network.public_key).unwrap();
        let client = async {
            let stream = ivy_websocket::connect(&format!("ws://{ws}/ivy"))
                .await
                .unwrap();
            let config = Config::new(ed25519_dalek::SigningKey::from_bytes(&[32; 32]));
            let mut peer = ivy::connect(stream, &config, key).await.unwrap();
            let advertised = &peer.peer_metadata().listen_addresses;
            assert_eq!(advertised.len(), 1);
            assert_eq!(advertised[0].host, "203.0.113.7");
            assert_eq!(advertised[0].port, port);
            peer.send(HELLO, &ChainHandshake::root("root").encode().unwrap())
                .await
                .unwrap();
            let (topic, bytes) = peer.recv().await.unwrap();
            assert_eq!(topic, HELLO);
            assert!(ChainHandshake::decode(&bytes).unwrap().compatible("root"));
            peer
        };
        let ready = async {
            loop {
                if let Some(NetworkEvent::Ready(peer)) = network.next().await.unwrap() {
                    break peer;
                }
            }
        };
        let (_client, ready) = tokio::join!(client, ready);
        assert_eq!(
            ready.key,
            Config::new(ed25519_dalek::SigningKey::from_bytes(&[32; 32]))
                .peer_key()
                .hex()
        );
        network.stop().await;
        let _tcp = TcpListener::bind(ws).await.unwrap();
        let _udp = tokio::net::UdpSocket::bind(ws).await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn optional_occupied_browser_listener_continues_but_required_refuses() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
    let mut network = Network::bind_with_overlay(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[41; 32]),
        "root".into(),
        storage.clone(),
        lattice_tally::Tally::default(),
        vec![],
        OverlayConfig {
            resources: Default::default(),
            websocket: WebSocketListener::IfAvailable(address),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(network.websocket_address.is_none());
    network.stop().await;
    assert!(
        Network::bind_with_overlay(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[41; 32]),
            "root".into(),
            storage,
            lattice_tally::Tally::default(),
            vec![],
            OverlayConfig {
                resources: Default::default(),
                websocket: WebSocketListener::Required(address),
                ..Default::default()
            },
        )
        .await
        .is_err()
    );
}
