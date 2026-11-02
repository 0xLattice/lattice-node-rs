//! Session-fenced Ivy transport adapter. Delivery credit is acknowledged only
//! after the host has consumed each notification. Outbound sends are bounded
//! and retried only after Ivy reports the same authenticated session writable.
mod read_endpoints;
#[cfg(test)]
mod transport_tests;
mod websocket;
use super::acquisition::{Priority, Queue, Reservation, ResourcePolicy, SESSION_BYTES};
use super::handshake::{ChainHandshake, HELLO};
use crate::{core::sync::PeerId, storage::NodeStorage};
use ivy_core::core::{AuthenticatedPeer, CoreSettings, Effect, Event, Request, RequestResult};
use ivy_core::{Config, Message, PeerEndpoint};
use lattice_cashew::SerializedVolume;
pub use read_endpoints::ReadEndpointConfig;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// Browser listener mode; programmatic hosts opt in, the daemon supplies its default.
#[derive(Clone, Debug, Default)]
pub enum WebSocketListener {
    #[default]
    Off,
    IfAvailable(SocketAddr),
    Required(SocketAddr),
}
#[derive(Clone, Debug, Default)]
pub struct OverlayConfig {
    pub websocket: WebSocketListener,
    pub external_address: Option<std::net::IpAddr>,
    pub resources: ResourcePolicy,
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub enum NetworkEvent {
    ReadEndpoints(
        u64,
        Vec<String>,
        Vec<crate::core::read_endpoints::DeclaredReadEndpoint>,
    ),
    Ready(PeerId),
    Dialed(PeerEndpoint, Vec<Vec<String>>, Option<bool>),
    HostingChanged(PeerId),
    Closed(PeerId),
    Message(PeerId, String, Vec<u8>),
    Sent(u64, bool),
    Fetched(u64, Option<Vec<SerializedVolume>>, Option<Arc<Reservation>>),
    FetchBusy(u64),
}
struct Session {
    peer: AuthenticatedPeer,
    id: PeerId,
    ready: bool,
    updates: bool,
    capabilities: BTreeSet<String>,
    deadline: std::time::Instant,
}
#[derive(Clone)]
struct Outbound {
    peer: PeerId,
    topic: String,
    bytes: Vec<u8>,
    tag: u64,
}
enum Pending {
    ReadProviders {
        lookup: u64,
    },
    ReadMemberships {
        lookup: u64,
    },
    ReadDial {
        lookup: u64,
        key: String,
    },
    Dial {
        endpoint: PeerEndpoint,
        paths: Vec<Vec<String>>,
    },
    Send(Outbound),
    Writable(Outbound),
    Fetch {
        tag: u64,
        root: String,
        reservation: Option<Arc<Reservation>>,
    },
    Retrieve {
        tag: u64,
        root: String,
        reservation: Option<Arc<Reservation>>,
    },
    CancelledAcquisition {
        _reservation: Option<Arc<Reservation>>,
    },
    Discover {
        path: Vec<String>,
    },
    Memberships {
        path: Vec<String>,
    },
}
enum Acquisition {
    Reserve {
        tag: u64,
    },
    Fetch {
        tag: u64,
        root: String,
        bundle: bool,
        peer: Option<PeerId>,
    },
    Retrieve {
        tag: u64,
        root: String,
    },
}
impl Acquisition {
    fn tag(&self) -> u64 {
        match self {
            Self::Reserve { tag } | Self::Fetch { tag, .. } | Self::Retrieve { tag, .. } => *tag,
        }
    }
}
pub struct Network {
    content_jobs: super::content_jobs::ContentJobs,
    acquisitions: Queue<Acquisition>,
    websocket: Option<websocket::Running>,
    pub websocket_address: Option<SocketAddr>,
    read_endpoints: read_endpoints::ReadEndpoints,
    commands: mpsc::Sender<Event>,
    outbound: VecDeque<Event>,
    effects: mpsc::Receiver<Effect>,
    task: JoinHandle<std::io::Result<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    sessions: BTreeMap<String, Session>,
    pending: BTreeMap<u64, Pending>,
    events: VecDeque<NetworkEvent>,
    next: u64,
    next_session: u64,
    genesis: String,
    hosted: BTreeSet<Vec<String>>,
    discovered: BTreeMap<Vec<String>, BTreeMap<String, u64>>,
    storage: Arc<NodeStorage>,
    tally: lattice_tally::Tally,
    pub address: SocketAddr,
    pub public_key: String,
}
impl Network {
    pub async fn bind(
        address: SocketAddr,
        key: ed25519_dalek::SigningKey,
        genesis: String,
        storage: Arc<NodeStorage>,
    ) -> anyhow::Result<Self> {
        Self::bind_with_tally(
            address,
            key,
            genesis,
            storage,
            lattice_tally::Tally::default(),
        )
        .await
    }
    pub async fn bind_with_tally(
        address: SocketAddr,
        key: ed25519_dalek::SigningKey,
        genesis: String,
        storage: Arc<NodeStorage>,
        tally: lattice_tally::Tally,
    ) -> anyhow::Result<Self> {
        Self::bind_with_configured_peers(address, key, genesis, storage, tally, vec![]).await
    }
    /// Explicit operator peers belong to Ivy's kept topology, including initial
    /// dial failure and transport-loss backoff. Conditional discovery seeds do not.
    pub async fn bind_with_configured_peers(
        address: SocketAddr,
        key: ed25519_dalek::SigningKey,
        genesis: String,
        storage: Arc<NodeStorage>,
        tally: lattice_tally::Tally,
        bootstrap_peers: Vec<PeerEndpoint>,
    ) -> anyhow::Result<Self> {
        Self::bind_with_overlay(
            address,
            key,
            genesis,
            storage,
            tally,
            bootstrap_peers,
            OverlayConfig::default(),
        )
        .await
    }
    pub async fn bind_with_overlay(
        address: SocketAddr,
        key: ed25519_dalek::SigningKey,
        genesis: String,
        storage: Arc<NodeStorage>,
        tally: lattice_tally::Tally,
        bootstrap_peers: Vec<PeerEndpoint>,
        overlay: OverlayConfig,
    ) -> anyhow::Result<Self> {
        overlay.resources.validate()?;
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let mut config = Config::new(key);
        let public_key = config.peer_key().hex();
        if !address.ip().is_unspecified() {
            config
                .metadata
                .listen_addresses
                .push(ivy_core::ListenAddress {
                    host: address.ip().to_string(),
                    port: address.port(),
                });
        }
        let browser = match overlay.websocket {
            WebSocketListener::Off => None,
            WebSocketListener::Required(address) => Some(websocket::Listener::bind(address).await?),
            WebSocketListener::IfAvailable(address) => {
                match websocket::Listener::bind(address).await {
                    Ok(listener) => Some(listener),
                    Err(error) => {
                        eprintln!(
                            "lattice-node: not listening for WebSocket and STUN on {address}: {error}"
                        );
                        None
                    }
                }
            }
        };
        let websocket_address = browser.as_ref().map(|listener| listener.address);
        let settings = CoreSettings {
            listen_port: Some(address.port()),
            external_address: overlay.external_address.map(|host| {
                ivy_core::stun::ObservedAddress {
                    host: host.to_string(),
                    port: address.port(),
                }
            }),
            relay_enabled: browser.is_some(),
            bootstrap_peers,
            max_concurrent_content_requests: overlay.resources.serving_max_concurrent,
            max_in_flight_volume_bytes: overlay.resources.serving_max_in_flight_volume_bytes,
            ..CoreSettings::default()
        };
        settings.validate(&config).map_err(anyhow::Error::msg)?;
        let (commands, input) = mpsc::channel(128);
        let (output, effects) = mpsc::channel(128);
        let (shutdown, shutdown_requested) = oneshot::channel();
        let (streams, supplied) = mpsc::channel(16);
        let websocket = browser.map(|listener| listener.start(streams));
        let task = tokio::spawn(ivy::punch_node::run_application_with_shutdown(
            listener,
            config,
            settings,
            input,
            output,
            Some(supplied),
            ivy_tally::TallyLedger(tally.clone()),
            async move {
                let _ = shutdown_requested.await;
            },
        ));
        commands.send(Event::SetContentSource(true)).await?;
        Ok(Self {
            content_jobs: super::content_jobs::ContentJobs::new(
                storage.clone(),
                overlay.resources.serving_max_concurrent,
            ),
            acquisitions: Queue::new(overlay.resources.max_acquisition_bytes),
            websocket,
            websocket_address,
            read_endpoints: Default::default(),
            commands,
            outbound: VecDeque::new(),
            effects,
            task,
            shutdown: Some(shutdown),
            sessions: BTreeMap::new(),
            pending: BTreeMap::new(),
            events: VecDeque::new(),
            next: 1,
            next_session: 1,
            genesis,
            hosted: BTreeSet::new(),
            discovered: BTreeMap::new(),
            storage,
            tally,
            address,
            public_key,
        })
    }
    fn emit(&mut self, event: Event) -> anyhow::Result<()> {
        anyhow::ensure!(self.outbound.len() < 4096, "Ivy command queue exhausted");
        self.outbound.push_back(event);
        Ok(())
    }
    async fn request(&mut self, request: Request, pending: Option<Pending>) -> anyhow::Result<()> {
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("request IDs exhausted"))?;
        if let Some(pending) = pending {
            self.pending.insert(id, pending);
        }
        self.emit(Event::Request(id, request))?;
        Ok(())
    }
    pub fn connecting(&self) -> bool {
        self.pending
            .values()
            .any(|p| matches!(p, Pending::Dial { .. }))
    }
    pub fn connected_keys(&self) -> BTreeSet<String> {
        self.sessions.keys().cloned().collect()
    }
    pub async fn connect(&mut self, endpoint: PeerEndpoint) -> anyhow::Result<()> {
        self.read_endpoints.keep.insert(endpoint.public_key.clone());
        self.connect_for_paths(endpoint, vec![vec!["Nexus".into()]])
            .await
    }
    pub(crate) async fn connect_for_paths(
        &mut self,
        endpoint: PeerEndpoint,
        paths: Vec<Vec<String>>,
    ) -> anyhow::Result<()> {
        self.read_endpoints.keep.insert(endpoint.public_key.clone());
        if self.sessions.contains_key(&endpoint.public_key) {
            self.events
                .push_back(NetworkEvent::Dialed(endpoint, paths, None));
            return Ok(());
        }
        self.request(
            Request::Connect(endpoint.clone()),
            Some(Pending::Dial { endpoint, paths }),
        )
        .await
    }
    fn hello_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut hello = ChainHandshake::root(&self.genesis);
        let capabilities = hello.capabilities.as_mut().unwrap();
        capabilities.extend(self.hosted.iter().map(|p| format!("host:{}", p.join("/"))));
        capabilities.sort();
        Ok(hello.encode()?)
    }
    pub fn peers_hosting(&self, path: &[String]) -> Vec<PeerId> {
        if path == ["Nexus"] {
            return self.peers();
        }
        let capability = format!("host:{}", path.join("/"));
        self.sessions
            .values()
            .filter(|s| {
                s.ready
                    && (s.capabilities.contains(&capability)
                        || self.discovered.get(path).is_some_and(|keys| {
                            keys.get(&s.id.key)
                                .is_some_and(|expiry| *expiry > unix_seconds())
                        }))
            })
            .map(|s| s.id.clone())
            .collect()
    }
    pub async fn set_hosted(&mut self, paths: &[Vec<String>]) -> anyhow::Result<()> {
        let hosted: BTreeSet<_> = paths.iter().cloned().collect();
        for path in &hosted {
            let context = lattice_primitives::ChainRuntimeContext::new(path.clone(), None)?;
            anyhow::ensure!(!context.is_root(), "hosted claims name child levels only");
        }
        let old = std::mem::replace(&mut self.hosted, hosted);
        let bytes = match self.hello_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.hosted = old;
                return Err(error);
            }
        };
        self.discovered.retain(|path, _| self.hosted.contains(path));
        let cancelled: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                matches!(pending, Pending::Discover { path } | Pending::Memberships { path } if !self.hosted.contains(path))
                    .then_some(*id)
            })
            .collect();
        for id in cancelled {
            self.pending.remove(&id);
            self.emit(Event::Cancel(id))?;
        }
        let peers: Vec<_> = self
            .sessions
            .values()
            .filter(|s| s.ready && s.updates)
            .map(|s| s.id.clone())
            .collect();
        for peer in peers {
            self.send(peer, HELLO.into(), bytes.clone(), 0).await?;
        }
        Ok(())
    }
    pub fn peers(&self) -> Vec<PeerId> {
        self.sessions
            .values()
            .filter(|p| p.ready)
            .map(|p| p.id.clone())
            .collect()
    }
    pub fn peer_summaries(&self) -> Vec<(String, String)> {
        self.sessions
            .values()
            .filter(|s| s.ready)
            .map(|s| {
                (
                    s.id.key.clone(),
                    match s.peer.role {
                        ivy_core::core::AuthenticatedPeerRole::Endpoint => "endpoint",
                        ivy_core::core::AuthenticatedPeerRole::Carrier => "carrier",
                    }
                    .into(),
                )
            })
            .collect()
    }
    fn session(&self, id: &PeerId) -> Option<&Session> {
        self.sessions.get(&id.key).filter(|p| p.id == *id)
    }
    pub async fn disconnect(&mut self, id: &PeerId) -> anyhow::Result<()> {
        if let Some(session) = self.session(id) {
            self.request(Request::Disconnect(session.peer.clone()), None)
                .await?;
        }
        Ok(())
    }
    pub async fn disconnect_invalid_proof(&mut self, id: &PeerId) -> anyhow::Result<()> {
        if self.session(id).is_some() {
            self.tally
                .record_protocol_violation(&lattice_tally::PeerId::new(&id.key));
        }
        self.disconnect(id).await
    }
    pub async fn send(
        &mut self,
        peer: PeerId,
        topic: String,
        bytes: Vec<u8>,
        tag: u64,
    ) -> anyhow::Result<()> {
        let queued = self
            .pending
            .values()
            .filter(|p| matches!(p,Pending::Send(v)|Pending::Writable(v) if v.peer==peer))
            .count();
        if queued >= 64 || bytes.len() > 4 * 1024 * 1024 {
            self.events.push_back(NetworkEvent::Sent(tag, false));
            return Ok(());
        }
        self.send_item(Outbound {
            peer,
            topic,
            bytes,
            tag,
        })
        .await
    }
    async fn send_item(&mut self, item: Outbound) -> anyhow::Result<()> {
        let Some(session) = self.session(&item.peer) else {
            self.events.push_back(NetworkEvent::Sent(item.tag, false));
            return Ok(());
        };
        self.request(
            Request::Send {
                peer: session.peer.key,
                session_id: Some(session.peer.session_id),
                message: Message::PeerMessage {
                    topic: item.topic.clone(),
                    payload: item.bytes.clone(),
                },
            },
            Some(Pending::Send(item)),
        )
        .await
    }
    pub async fn fetch(
        &mut self,
        tag: u64,
        root: String,
        bundle: bool,
        peer: Option<&PeerId>,
    ) -> anyhow::Result<()> {
        self.acquisitions.push(
            SESSION_BYTES,
            Priority::OwnWork,
            Acquisition::Fetch {
                tag,
                root,
                bundle,
                peer: peer.cloned(),
            },
        );
        Ok(())
    }
    /// A dependency fetch within a worker's already reserved acquisition session.
    pub(crate) async fn fetch_reserved(
        &mut self,
        tag: u64,
        root: String,
        reservation: Arc<Reservation>,
    ) -> anyhow::Result<()> {
        let peers = self
            .sessions
            .values()
            .filter(|s| s.ready)
            .map(|s| s.peer.clone())
            .collect::<Vec<_>>();
        if peers.is_empty() {
            self.events
                .push_back(NetworkEvent::Fetched(tag, None, None));
            return Ok(());
        }
        self.request(
            Request::FetchFromSessions {
                root_cid: root.clone(),
                bundle: false,
                peers,
                maximum_archive_bytes: SESSION_BYTES,
                maximum_entries: u16::MAX,
            },
            Some(Pending::Fetch {
                tag,
                root,
                reservation: Some(reservation),
            }),
        )
        .await
    }
    pub(crate) fn reserve_worker(&mut self, tag: u64) {
        self.acquisitions.push(
            SESSION_BYTES,
            Priority::OwnWork,
            Acquisition::Reserve { tag },
        );
    }
    pub async fn fetch_body(
        &mut self,
        tag: u64,
        root: String,
        depth: usize,
        peer: Option<&PeerId>,
    ) -> anyhow::Result<()> {
        self.acquisitions.push(
            SESSION_BYTES,
            Priority::Body {
                cid: root.clone(),
                depth,
            },
            Acquisition::Fetch {
                tag,
                root,
                bundle: true,
                peer: peer.cloned(),
            },
        );
        Ok(())
    }
    pub fn prioritize_acquisitions(&mut self, next: BTreeSet<String>) {
        self.acquisitions.prioritize(next);
    }
    pub fn requeue_unavailable(&mut self, tag: u64) {
        self.events
            .push_back(NetworkEvent::Fetched(tag, None, None));
    }
    pub async fn retrieve(&mut self, tag: u64, root: String) -> anyhow::Result<()> {
        // Operator reads that are already local never wait behind network work.
        if let Some(volume) = self.storage.serving_volume(&root)? {
            self.events
                .push_back(NetworkEvent::Fetched(tag, Some(vec![volume]), None));
        } else {
            self.acquisitions.push(
                SESSION_BYTES,
                Priority::OwnWork,
                Acquisition::Retrieve { tag, root },
            );
        }
        Ok(())
    }
    async fn dispatch_acquisitions(&mut self) -> anyhow::Result<()> {
        while self.outbound.len() < 4096 {
            let Some((request, reservation)) = self.acquisitions.pop() else {
                break;
            };
            match request {
                Acquisition::Reserve { tag } => self.events.push_back(NetworkEvent::Fetched(
                    tag,
                    Some(vec![]),
                    Some(Arc::new(reservation)),
                )),
                Acquisition::Retrieve { tag, root } => {
                    self.request(
                        Request::RetrieveVolume {
                            root_cid: root.clone(),
                            maximum_archive_bytes: SESSION_BYTES,
                            maximum_entries: u16::MAX,
                        },
                        Some(Pending::Retrieve {
                            tag,
                            root,
                            reservation: Some(Arc::new(reservation)),
                        }),
                    )
                    .await?;
                }
                Acquisition::Fetch {
                    tag,
                    root,
                    bundle,
                    peer,
                } => {
                    let peers = if let Some(peer) = peer {
                        self.session(&peer)
                            .filter(|s| s.ready)
                            .map(|s| vec![s.peer.clone()])
                            .unwrap_or_default()
                    } else {
                        self.sessions
                            .values()
                            .filter(|s| s.ready)
                            .map(|s| s.peer.clone())
                            .collect()
                    };
                    if peers.is_empty() {
                        self.events
                            .push_back(NetworkEvent::Fetched(tag, None, None));
                        continue;
                    }
                    self.request(
                        Request::FetchFromSessions {
                            root_cid: root.clone(),
                            bundle,
                            peers,
                            maximum_archive_bytes: SESSION_BYTES,
                            maximum_entries: u16::MAX,
                        },
                        Some(Pending::Fetch {
                            tag,
                            root,
                            reservation: Some(Arc::new(reservation)),
                        }),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
    pub fn searching_hosted(&self, path: &[String]) -> bool {
        self.pending
            .values()
            .any(|p| matches!(p, Pending::Discover { path: p } | Pending::Memberships { path: p } if p == path))
    }
    /// One lookup per hosted path and at most 64 overall. Completion only
    /// schedules dials; neither lookup nor dial waits on the host actor.
    pub async fn discover_hosted(&mut self, path: Vec<String>) -> anyhow::Result<()> {
        if !self.hosted.contains(&path)
            || self
                .pending
                .values()
                .any(|p| matches!(p, Pending::Discover { path: p } | Pending::Memberships { path: p } if p == &path))
            || self
                .pending
                .values()
                .filter(|p| matches!(p, Pending::Discover { .. } | Pending::Memberships { .. }))
                .count()
                >= 64
        {
            return Ok(());
        }
        let key = crate::core::peer_search::rendezvous(&self.genesis, &path);
        self.request(
            Request::DiscoverProviders { root_cid: key },
            Some(Pending::Discover { path }),
        )
        .await
    }
    pub fn announce_provider(&mut self, root: String, expires_at: u64) -> anyhow::Result<()> {
        self.emit(Event::AnnounceProvider {
            root_cid: root,
            expires_at,
        })
    }
    pub fn stop_providing(&mut self, root: String) -> anyhow::Result<()> {
        self.emit(Event::StopProviding { root_cid: root })
    }
    pub async fn cancel_fetch(&mut self, tag: u64) -> anyhow::Result<()> {
        self.acquisitions.retain(|request| request.tag() != tag);
        self.events
            .retain(|event| !matches!(event, NetworkEvent::Fetched(id,_,_) if *id==tag));
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, p)| {
                matches!(p,Pending::Fetch{tag:t,..} | Pending::Retrieve{tag:t,..} if *t==tag)
                    .then_some(*id)
            })
            .collect();
        for id in ids {
            let reservation = match self.pending.remove(&id).unwrap() {
                Pending::Fetch { reservation, .. } | Pending::Retrieve { reservation, .. } => {
                    reservation
                }
                _ => unreachable!(),
            };
            self.pending.insert(
                id,
                Pending::CancelledAcquisition {
                    _reservation: reservation,
                },
            );
            self.emit(Event::Cancel(id))?;
        }
        Ok(())
    }
    pub async fn tick(&mut self) -> anyhow::Result<()> {
        self.tick_read_endpoints().await?;
        let now = unix_seconds();
        let mut changed = BTreeSet::new();
        for keys in self.discovered.values_mut() {
            keys.retain(|key, expiry| {
                if *expiry <= now {
                    changed.insert(key.clone());
                    false
                } else {
                    true
                }
            });
        }
        for key in changed {
            if let Some(session) = self.sessions.get(&key).filter(|s| s.ready) {
                self.events
                    .push_back(NetworkEvent::HostingChanged(session.id.clone()));
            }
        }
        let expired: Vec<_> = self
            .sessions
            .values()
            .filter(|s| !s.ready && s.deadline <= std::time::Instant::now())
            .map(|s| s.id.clone())
            .collect();
        for peer in expired {
            self.disconnect(&peer).await?;
        }
        Ok(())
    }
    pub async fn next(&mut self) -> anyhow::Result<Option<NetworkEvent>> {
        loop {
            self.dispatch_acquisitions().await?;
            while !self.outbound.is_empty() {
                match self.commands.try_reserve() {
                    Ok(permit) => permit.send(self.outbound.pop_front().unwrap()),
                    Err(mpsc::error::TrySendError::Full(_)) => break,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(anyhow::anyhow!("Ivy command channel closed"));
                    }
                }
            }
            if let Some(event) = self.events.pop_front() {
                if let NetworkEvent::Message(peer, topic, payload) = &event
                    && matches!(
                        topic.as_str(),
                        crate::core::read_endpoints::REQUEST
                            | crate::core::read_endpoints::RESPONSE
                            | crate::core::read_endpoints::RESPONSE_V2
                    )
                {
                    self.read_endpoint_message(peer.clone(), topic, payload)
                        .await?;
                    continue;
                }
                return Ok(Some(event));
            }
            // Commands and notifications are both bounded. Keep draining
            // effects while waiting for command capacity to avoid a cycle.
            // Pop only after reservation: cancellation leaves commands queued.
            let wake = self.acquisitions.wakeup();
            let mut job_finished = None;
            let incoming = tokio::select! {
                _=wake.notified()=>continue,
                done=self.content_jobs.next()=>{ job_finished = Some(done); None },
                permit=self.commands.reserve(), if !self.outbound.is_empty()=>{permit?.send(self.outbound.pop_front().unwrap());continue},
                effect=self.effects.recv()=>effect,
            };
            if let Some((id, result)) = job_finished {
                self.emit(Event::JobFinished(id, result))?;
                continue;
            }
            let Some(effect) = incoming else {
                return Ok(None);
            };
            match effect {
                Effect::PeerConnected(token, peer) => {
                    let id = PeerId {
                        key: peer.key.hex(),
                        session: self.next_session,
                    };
                    self.next_session = self
                        .next_session
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("session IDs exhausted"))?;
                    if let Some(old) = self.sessions.insert(
                        id.key.clone(),
                        Session {
                            peer,
                            id: id.clone(),
                            ready: false,
                            updates: false,
                            capabilities: BTreeSet::new(),
                            deadline: std::time::Instant::now()
                                + std::time::Duration::from_secs(30),
                        },
                    ) && old.ready
                    {
                        self.events.push_back(NetworkEvent::Closed(old.id));
                    }
                    let bytes = self.hello_bytes()?;
                    self.send(id, HELLO.into(), bytes, 0).await?;
                    self.emit(Event::Delivered(token))?;
                }
                Effect::PeerDisconnected(key) => {
                    if let Some(old) = self.sessions.remove(&key.hex())
                        && old.ready
                    {
                        self.events.push_back(NetworkEvent::Closed(old.id));
                    }
                }
                Effect::Deliver(token, peer, message) => {
                    if let Some(session) = self
                        .sessions
                        .get_mut(&peer.key.hex())
                        .filter(|s| s.peer.session_id == peer.session_id)
                        && let Message::PeerMessage { topic, payload } = message
                    {
                        if topic == HELLO {
                            if !session.ready || session.updates {
                                match ChainHandshake::decode(&payload)
                                    .ok()
                                    .filter(|h| h.compatible(&self.genesis))
                                {
                                    Some(hello) => {
                                        let capabilities: BTreeSet<String> = hello
                                            .capabilities
                                            .clone()
                                            .unwrap_or_default()
                                            .into_iter()
                                            .collect();
                                        let hosting_changed = session.ready
                                            && session
                                                .capabilities
                                                .iter()
                                                .filter(|s| s.starts_with("host:"))
                                                .collect::<Vec<_>>()
                                                != capabilities
                                                    .iter()
                                                    .filter(|s| s.starts_with("host:"))
                                                    .collect::<Vec<_>>();
                                        session.capabilities = capabilities;
                                        if hosting_changed {
                                            self.events.push_back(NetworkEvent::HostingChanged(
                                                session.id.clone(),
                                            ));
                                        }
                                        if !session.ready {
                                            session.ready = true;
                                            session.updates =
                                                hello.capabilities.as_ref().is_some_and(|c| {
                                                    c.iter().any(|s| s == "hello-updates")
                                                });
                                            self.events
                                                .push_back(NetworkEvent::Ready(session.id.clone()));
                                        }
                                    }
                                    None if !session.ready => {
                                        self.outbound.push_back(Event::DisconnectPeer(peer.key));
                                    }
                                    _ => {}
                                }
                            }
                        } else if session.ready {
                            self.events.push_back(NetworkEvent::Message(
                                session.id.clone(),
                                topic,
                                payload,
                            ));
                        }
                    }
                    self.emit(Event::Delivered(token))?;
                }
                Effect::Completed(id, result) => {
                    if let Some(pending) = self.pending.remove(&id) {
                        match pending {
                            Pending::CancelledAcquisition { .. } => {}
                            Pending::ReadProviders { lookup } => {
                                self.read_providers(lookup, result).await?
                            }
                            Pending::ReadMemberships { lookup } => {
                                self.read_memberships(lookup, result).await?
                            }
                            Pending::ReadDial { lookup, key } => {
                                self.read_dialed(lookup, key, result).await?
                            }
                            Pending::Dial { endpoint, paths } => {
                                let result = match result {
                                    RequestResult::Connected => {
                                        self.sessions.get(&endpoint.public_key).map(|s| {
                                            matches!(
                                                s.peer.route,
                                                ivy_core::core::AuthenticatedRoute::Direct
                                            )
                                        })
                                    }
                                    RequestResult::ConnectionInProgress => None,
                                    _ => Some(false),
                                };
                                self.events
                                    .push_back(NetworkEvent::Dialed(endpoint, paths, result));
                            }
                            Pending::Send(item) => match result {
                                RequestResult::Backpressured => {
                                    if let Some(session) = self.session(&item.peer) {
                                        self.request(
                                            Request::WaitUntilWritable {
                                                peer: session.peer.key,
                                                session_id: Some(session.peer.session_id),
                                            },
                                            Some(Pending::Writable(item)),
                                        )
                                        .await?;
                                    } else {
                                        self.events.push_back(NetworkEvent::Sent(item.tag, false));
                                    }
                                }
                                other => self.events.push_back(NetworkEvent::Sent(
                                    item.tag,
                                    matches!(other, RequestResult::Enqueued),
                                )),
                            },
                            Pending::Writable(item) => {
                                if matches!(result, RequestResult::Writable(true)) {
                                    self.send_item(item).await?
                                } else {
                                    self.events.push_back(NetworkEvent::Sent(item.tag, false));
                                }
                            }
                            Pending::Discover { path } => {
                                if self.hosted.contains(&path) {
                                    let endpoints = match result {
                                        RequestResult::Endpoints(endpoints) => {
                                            endpoints.into_iter().take(256).collect()
                                        }
                                        _ => Vec::new(),
                                    };
                                    let key =
                                        crate::core::peer_search::rendezvous(&self.genesis, &path);
                                    self.request(
                                        Request::ProviderMemberships { root_cid: key },
                                        Some(Pending::Memberships { path: path.clone() }),
                                    )
                                    .await?;
                                    let mut candidates: Vec<_> = endpoints
                                        .into_iter()
                                        .filter(|e| {
                                            e.public_key != self.public_key
                                                && !self.sessions.contains_key(&e.public_key)
                                        })
                                        .collect();
                                    // Random draws prevent a provider's answer order from
                                    // choosing all four dial slots. Rejection sampling
                                    // avoids modulo bias without another RNG dependency.
                                    use rand_core::RngCore;
                                    let mut rng = rand_core::OsRng;
                                    for _ in 0..4 {
                                        if candidates.is_empty() {
                                            break;
                                        }
                                        let n = candidates.len() as u64;
                                        let limit = u64::MAX - u64::MAX % n;
                                        let mut draw = rng.next_u64();
                                        while draw >= limit {
                                            draw = rng.next_u64();
                                        }
                                        let endpoint = candidates.swap_remove((draw % n) as usize);
                                        self.connect_for_paths(
                                            endpoint,
                                            (1..=path.len()).map(|n| path[..n].to_vec()).collect(),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Pending::Memberships { path } => {
                                if self.hosted.contains(&path) {
                                    let keys = match result {
                                        RequestResult::ProviderMemberships(keys) => keys
                                            .into_iter()
                                            .take(256)
                                            .map(|(key, expiry)| (key.hex(), expiry))
                                            .collect(),
                                        _ => BTreeMap::new(),
                                    };
                                    self.discovered.insert(path, keys);
                                    for peer in self.peers() {
                                        self.events.push_back(NetworkEvent::HostingChanged(peer));
                                    }
                                }
                            }
                            Pending::Fetch {
                                tag,
                                root,
                                reservation,
                            }
                            | Pending::Retrieve {
                                tag,
                                root,
                                reservation,
                            } => {
                                if matches!(&result, RequestResult::Volume(response) if response.failure == Some(ivy_core::VolumeReceiveError::LocalCapacityUnavailable))
                                {
                                    self.events.push_back(NetworkEvent::FetchBusy(tag));
                                    continue;
                                }
                                let volumes = if let RequestResult::Volume(response) = result {
                                    let volumes: Vec<_> = response
                                        .volumes
                                        .into_iter()
                                        .map(|v| {
                                            SerializedVolume::new(
                                                v.root_cid,
                                                v.entries
                                                    .into_iter()
                                                    .map(|e| (e.cid, e.data))
                                                    .collect(),
                                            )
                                        })
                                        .collect();
                                    (response.failure.is_none()
                                        && response.ended
                                        && volumes.iter().any(|v| v.root == root)
                                        && volumes.iter().all(|v| v.validate().is_ok()))
                                    .then_some(volumes)
                                } else {
                                    None
                                };
                                self.events.push_back(NetworkEvent::Fetched(
                                    tag,
                                    volumes,
                                    reservation,
                                ));
                            }
                        }
                    }
                }
                Effect::StartJob(id, job) => self.content_jobs.submit(id, job),
                Effect::CancelJob(id) => self.content_jobs.cancel(id),
                _ => {}
            }
        }
    }
    pub async fn stop(&mut self) {
        self.content_jobs.shutdown().await;
        if let Some(mut websocket) = self.websocket.take() {
            websocket.stop().await;
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
            let _ = (&mut self.task).await;
        }
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn acquisition_budget_survives_cancel_and_completed_host_handoff() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut network = Network::bind(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[9; 32]),
            "root".into(),
            storage,
        )
        .await
        .unwrap();
        network.stop().await;
        network.acquisitions = Queue::new(SESSION_BYTES);
        let (commands, mut input) = mpsc::channel(32);
        let (output, effects) = mpsc::channel(32);
        network.commands = commands;
        network.effects = effects;
        let old = network.next;
        network.retrieve(1, "remote".into()).await.unwrap();
        network.dispatch_acquisitions().await.unwrap();
        network.cancel_fetch(1).await.unwrap();
        network.retrieve(2, "remote".into()).await.unwrap();
        network
            .retrieve(99, "cancel-before-grant".into())
            .await
            .unwrap();
        network.cancel_fetch(99).await.unwrap();
        network.dispatch_acquisitions().await.unwrap();
        assert_eq!(network.acquisitions.used(), SESSION_BYTES);
        assert_eq!(network.acquisitions.waiting(), 1);
        let bytes = b"local-only budget bypass".to_vec();
        let cid = lattice_cashew::cid_for_bytes(0x55, &bytes);
        let volume = SerializedVolume::new(cid.clone(), BTreeMap::from([(cid.clone(), bytes)]));
        network
            .storage
            .cache_volumes("local-test", &[volume])
            .unwrap();
        network.retrieve(100, cid).await.unwrap();
        assert!(matches!(
            network.events.pop_front(),
            Some(NetworkEvent::Fetched(100, Some(_), None))
        ));
        assert_eq!(network.acquisitions.used(), SESSION_BYTES);
        assert!(matches!(
            network.pending.get(&old),
            Some(Pending::CancelledAcquisition { .. })
        ));
        let replacement = network.next;
        let driver = tokio::spawn(async move {
            assert!(matches!(input.recv().await, Some(Event::Request(id, _)) if id == old));
            assert!(matches!(input.recv().await, Some(Event::Cancel(id)) if id == old));
            assert!(
                input.try_recv().is_err(),
                "replacement waits for cancellation acknowledgment"
            );
            // An already completed request may race cancellation; either terminal
            // result retires the tombstone without publishing obsolete content.
            output
                .send(Effect::Completed(old, RequestResult::Failed))
                .await
                .unwrap();
            assert!(matches!(input.recv().await, Some(Event::Request(id, _)) if id == replacement));
            output
                .send(Effect::Completed(replacement, RequestResult::Failed))
                .await
                .unwrap();
            input.recv().await;
        });
        let completed = tokio::time::timeout(std::time::Duration::from_secs(2), network.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &completed,
            Some(NetworkEvent::Fetched(2, None, Some(_)))
        ));
        assert_eq!(
            network.acquisitions.used(),
            SESSION_BYTES,
            "completed response stays reserved through consumption"
        );
        network.retrieve(3, "remote".into()).await.unwrap();
        network.dispatch_acquisitions().await.unwrap();
        assert_eq!(network.acquisitions.waiting(), 1);
        let wake = network.acquisitions.wakeup();
        // Consume previous notifications before proving the handoff drop wakes.
        while tokio::time::timeout(std::time::Duration::from_millis(1), wake.notified())
            .await
            .is_ok()
        {}
        drop(completed);
        tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified())
            .await
            .unwrap();
        assert_eq!(network.acquisitions.used(), 0);
        network.dispatch_acquisitions().await.unwrap();
        assert_eq!(network.acquisitions.waiting(), 0);
        assert_eq!(network.acquisitions.used(), SESSION_BYTES);
        driver.abort();
        let _ = driver.await;
    }
    #[tokio::test]
    async fn cancelled_retrieval_precedes_replacement_and_late_completion_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut network = Network::bind(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[9; 32]),
            "root".into(),
            storage,
        )
        .await
        .unwrap();
        network.stop().await;
        let (commands, mut input) = mpsc::channel(1);
        let (output, effects) = mpsc::channel(1);
        network.commands = commands;
        network.effects = effects;
        let old = network.next;
        network.retrieve(41, "root".into()).await.unwrap();
        network.dispatch_acquisitions().await.unwrap();
        network.cancel_fetch(41).await.unwrap();
        let replacement = network.next;
        network.retrieve(42, "root".into()).await.unwrap();
        network.dispatch_acquisitions().await.unwrap();
        let driver = tokio::spawn(async move {
            assert!(
                matches!(input.recv().await,Some(Event::Request(id,Request::RetrieveVolume{..})) if id==old)
            );
            assert!(matches!(input.recv().await,Some(Event::Cancel(id)) if id==old));
            assert!(
                matches!(input.recv().await,Some(Event::Request(id,Request::RetrieveVolume{..})) if id==replacement)
            );
            output
                .send(Effect::Completed(old, RequestResult::Failed))
                .await
                .unwrap();
            output
                .send(Effect::Completed(replacement, RequestResult::Failed))
                .await
                .unwrap();
            input.recv().await;
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), network.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Some(NetworkEvent::Fetched(42, None, _))));
        assert!(network.pending.is_empty());
        driver.abort();
        let _ = driver.await;
    }
    #[tokio::test]
    async fn provider_completion_bounds_dials_and_unhosting_cancels_late_results() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut network = Network::bind(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[3; 32]),
            "root".into(),
            storage,
        )
        .await
        .unwrap();
        // Exercise the real adapter with a controlled Ivy result channel. Its
        // transport task is stopped, so synthetic public endpoints are never dialled.
        network.task.abort();
        let (commands, mut input) = mpsc::channel(128);
        let (output, effects) = mpsc::channel(128);
        network.commands = commands;
        network.effects = effects;
        let path = vec!["Nexus".into(), "Alpha".into()];
        network
            .set_hosted(std::slice::from_ref(&path))
            .await
            .unwrap();
        let id = network.next;
        network.discover_hosted(path.clone()).await.unwrap();
        network.discover_hosted(path.clone()).await.unwrap();
        let endpoints: Vec<_> = (1..=12)
            .map(|n| PeerEndpoint {
                public_key: ed25519_dalek::SigningKey::from_bytes(&[n; 32])
                    .verifying_key()
                    .to_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect(),
                host: format!("8.8.8.{n}"),
                port: 7447,
            })
            .collect();
        output
            .send(Effect::Completed(
                id,
                RequestResult::Endpoints(endpoints.clone()),
            ))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), network.next())
                .await
                .is_err()
        );
        let mut lookups = 0;
        let mut dials = BTreeSet::new();
        let mut membership_id = None;
        while let Ok(event) = input.try_recv() {
            match event {
                Event::Request(_, Request::DiscoverProviders { .. }) => lookups += 1,
                Event::Request(id, Request::ProviderMemberships { .. }) => membership_id = Some(id),
                Event::Request(_, Request::Connect(endpoint)) => {
                    assert!(endpoints.contains(&endpoint));
                    dials.insert(endpoint.public_key);
                }
                _ => {}
            }
        }
        assert_eq!(lookups, 1);
        assert_eq!(dials.len(), 4);
        output
            .send(Effect::Completed(
                membership_id.unwrap(),
                RequestResult::ProviderMemberships(vec![]),
            ))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), network.next())
                .await
                .is_err()
        );
        let id = network.next;
        network.discover_hosted(path.clone()).await.unwrap();
        network.set_hosted(&[]).await.unwrap();
        output
            .send(Effect::Completed(id, RequestResult::Endpoints(endpoints)))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), network.next())
                .await
                .is_err()
        );
        assert!(!network.searching_hosted(&path));
        assert!(network.discovered.is_empty());
        let mut cancelled = false;
        while let Ok(event) = input.try_recv() {
            assert!(!matches!(event, Event::Request(_, Request::Connect(_))));
            cancelled |= matches!(event, Event::Cancel(request) if request == id);
        }
        assert!(cancelled);
    }
    #[tokio::test]
    async fn cancelled_poll_retains_commands_and_full_duplex_queues_make_progress() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut network = Network::bind(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
            "root".into(),
            storage,
        )
        .await
        .unwrap();
        network.task.abort();
        let (commands, mut input) = mpsc::channel(1);
        let (output, effects) = mpsc::channel(1);
        network.commands = commands;
        network.effects = effects;
        network.commands.send(Event::Tick).await.unwrap();
        network.emit(Event::Delivered(7)).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), network.next())
                .await
                .is_err()
        );
        assert!(matches!(
            network.outbound.front(),
            Some(Event::Delivered(7))
        ));
        output.send(Effect::CancelJob(1)).await.unwrap();
        let mut budget = Queue::new(SESSION_BYTES);
        budget.push(SESSION_BYTES, Priority::OwnWork, ());
        let (_, reservation) = budget.pop().unwrap();
        network.pending.insert(
            99,
            Pending::Fetch {
                reservation: Some(Arc::new(reservation)),
                tag: 99,
                root: "root".into(),
            },
        );
        let driver = tokio::spawn(async move {
            // Models Ivy blocked publishing an effect before reading commands.
            output.send(Effect::CancelJob(2)).await.unwrap();
            assert!(matches!(input.recv().await, Some(Event::Tick)));
            assert!(matches!(input.recv().await, Some(Event::Delivered(7))));
            output
                .send(Effect::Completed(99, RequestResult::Failed))
                .await
                .unwrap();
            // Keep the command receiver alive until the host consumes completion.
            input.recv().await;
        });
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), network.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, Some(NetworkEvent::Fetched(99, None, _))));
        assert!(network.outbound.is_empty());
        driver.abort();
    }
}
