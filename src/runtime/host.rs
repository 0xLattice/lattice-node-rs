//! Root platform host: bounded command ingress, durable core execution and Ivy I/O.
mod discovery;
mod fetches;
mod operator;
pub use discovery::DiscoveryConfig;
mod child_author;
mod read_endpoints;
pub use read_endpoints::ReadEndpointConfig;
mod child_mining;
mod children;
mod configuration;
mod proofs;
mod reads;
mod retention;
mod validation;
mod health;
use super::network::{Network, NetworkEvent};
use crate::{
    core::{
        body::BodyEffect,
        chain::{ChainEffects, RootChainCore, WireDispatch},
        headers::HeaderEffect,
        mining::*,
        stream::{StreamConfig, SyncEffect},
        sync::*,
        wire,
    },
    storage::NodeStorage,
};
use lattice_cashew::{Header, SerializedVolume, Volume};
use lattice_primitives::{Block, Transaction, UInt256, content_id};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
pub struct HostConfig {
    pub directory: PathBuf,
    pub genesis_cid: String,
    pub bootstrap_volumes: Vec<SerializedVolume>,
    pub signing_key: ed25519_dalek::SigningKey,
    pub listen: SocketAddr,
    pub min_relay_fee: u64,
    pub seeds: Vec<ivy_core::PeerEndpoint>,
}
#[derive(Clone)]
pub struct HostHandle {
    health: health::PublishedHealth,
    commands: mpsc::Sender<Command>,
}
pub struct RunningHost {
    pub handle: HostHandle,
    pub overlay_addr: SocketAddr,
    pub public_key: String,
    pub task: tokio::task::JoinHandle<anyhow::Result<()>>,
}
enum Command {
    Request(String, Value, oneshot::Sender<(u16, Value)>),
    Volume(String, oneshot::Sender<Option<Vec<u8>>>),
    Shutdown,
}
impl HostHandle {
    pub fn health(&self, route: &str) -> (u16, Value) {
        self.health.read(route)
    }
    pub async fn request(&self, route: &str, body: Value) -> anyhow::Result<(u16, Value)> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Request(route.into(), body, tx))
            .await?;
        Ok(tokio::time::timeout(std::time::Duration::from_secs(60), rx).await??)
    }
    pub async fn volume(&self, cid: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::Volume(cid.into(), tx)).await?;
        Ok(tokio::time::timeout(std::time::Duration::from_secs(30), rx).await??)
    }
    pub async fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown).await;
    }
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn error(message: impl ToString) -> (u16, Value) {
    (400, json!({"error":{"message":message.to_string()}}))
}
enum Fetch {
    Body(String),
    Index(PeerId, String),
    Transaction(PeerId, String),
    Operator(String),
    ChildBody(Vec<String>, String),
    ChildIndex(Vec<String>, PeerId, String),
}
enum Work {
    ReadEndpoints(u64, Vec<String>),
    Chain(ChainEffects),
    Child(Vec<String>, ChainEffects),
    ChildMining(Vec<String>, Box<MiningEffect>),
    HostLevels(u64, configuration::HostedConfiguration),
    OperatorFetch(u64, String),
    Mining(Box<MiningEffect>),
}
struct MinedReply {
    id: u64,
    cid: String,
    children: Vec<(String, String)>,
}
struct Host {
    health: health::PublishedHealth,
    read_endpoint_config: ReadEndpointConfig,
    chain: RootChainCore,
    levels: crate::core::levels::HostedLevels,
    child_mining: BTreeMap<Vec<String>, child_mining::ChildMining>,
    child_settings: BTreeMap<Vec<String>, configuration::ChildSettings>,
    child_served: BTreeMap<u64, (Vec<String>, PeerId, u64)>,
    mining: DurableMiningCore,
    storage: Arc<NodeStorage>,
    network: Network,
    genesis: String,
    ledger: super::serving::ServingLedger,
    last_ledger_save: std::time::Instant,
    fetches: BTreeMap<u64, Fetch>,
    served: BTreeMap<u64, (PeerId, u64)>,
    replies: BTreeMap<u64, oneshot::Sender<(u16, Value)>>,
    next: u64,
    queue: VecDeque<Work>,
    mined_replies: Vec<MinedReply>,
    transient: Vec<String>,
    needs_fetch: BTreeSet<String>,
    executed_children: BTreeSet<String>,
    discovery: discovery::Discovery,
}
pub async fn start(config: HostConfig) -> anyhow::Result<RunningHost> {
    start_with_retention(config, crate::storage::RetentionPolicy::default()).await
}
pub async fn start_with_retention(
    config: HostConfig,
    retention: crate::storage::RetentionPolicy,
) -> anyhow::Result<RunningHost> {
    start_with_policies(config, retention, DiscoveryConfig::default()).await
}
pub async fn start_with_policies(
    config: HostConfig,
    retention: crate::storage::RetentionPolicy,
    discovery_config: DiscoveryConfig,
) -> anyhow::Result<RunningHost> {
    start_with_read_endpoints(
        config,
        retention,
        discovery_config,
        ReadEndpointConfig::default(),
    )
    .await
}
pub async fn start_with_read_endpoints(
    config: HostConfig,
    retention: crate::storage::RetentionPolicy,
    discovery_config: DiscoveryConfig,
    read_endpoint_config: ReadEndpointConfig,
) -> anyhow::Result<RunningHost> {
    start_inner(
        config,
        retention,
        discovery_config,
        read_endpoint_config,
        None,
    )
    .await
}
/// Parse the initial operator hosting configuration before opening the node, so
/// child-only discovery and peer books are initialized before maintenance runs.
pub async fn start_with_hosted_configuration(
    config: HostConfig,
    retention: crate::storage::RetentionPolicy,
    discovery_config: DiscoveryConfig,
    read_endpoint_config: ReadEndpointConfig,
    hosting: Value,
) -> anyhow::Result<RunningHost> {
    let hosting = configuration::parse(&hosting)?;
    start_inner(
        config,
        retention,
        discovery_config,
        read_endpoint_config,
        Some(hosting),
    )
    .await
}
async fn start_inner(
    config: HostConfig,
    retention: crate::storage::RetentionPolicy,
    discovery_config: DiscoveryConfig,
    read_endpoint_config: ReadEndpointConfig,
    initial_hosting: Option<configuration::HostedConfiguration>,
) -> anyhow::Result<RunningHost> {
    read_endpoint_config.validate()?;
    discovery_config.validate()?;
    let storage = Arc::new(NodeStorage::open_with_retention(
        &config.directory,
        &config.genesis_cid,
        "node",
        retention,
    )?);
    storage.reconcile_cached_volumes()?;
    storage.cache_volumes("bootstrap", &config.bootstrap_volumes)?;
    let header = Volume(Header::from_cid(&config.genesis_cid)?);
    let genesis = lattice_validation::validate_root_genesis(&header, storage.as_ref(), now())?;
    let chain = RootChainCore::open(
        storage.clone(),
        &genesis,
        &format!("{}-{}", now(), std::process::id()),
        StreamConfig::default(),
        64,
    )?;
    storage.release_cached_volumes("bootstrap")?;
    let state = MiningState::new(
        chain.tree().executed_prefix().unwrap().into(),
        Some(genesis.spec().0.node.as_deref().unwrap().clone()),
        MiningConfig {
            mempool: crate::core::mempool::MempoolLimits {
                min_relay_fee: config.min_relay_fee,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let (mining, restored) =
        DurableMiningCore::restore(state, storage.clone(), &["Nexus".into()], now())?;
    let ledger = super::serving::ServingLedger::open(&config.directory)?;
    let mut network = Network::bind_with_tally(
        config.listen,
        config.signing_key,
        config.genesis_cid.clone(),
        storage.clone(),
        ledger.tally(),
    )
    .await?;
    network.configure_read_endpoints(
        read_endpoint_config.clone(),
        config.seeds.iter().map(|s| s.public_key.clone()),
    )?;
    let initial_paths: Vec<_> = std::iter::once(vec!["Nexus".into()])
        .chain(
            initial_hosting
                .iter()
                .flat_map(|hosting| hosting.children.keys().cloned()),
        )
        .collect();
    let child_seeds = initial_hosting.as_ref().is_some_and(|hosting| {
        hosting
            .children
            .values()
            .any(|settings| !settings.peers.is_empty())
    });
    let discovery = discovery::Discovery::new(
        &config.directory,
        network.public_key.clone(),
        config.seeds.clone(),
        discovery_config,
        &initial_paths,
        child_seeds,
    );
    for seed in config.seeds {
        network.connect(seed).await?;
    }
    let executed_children = storage
        .journal()
        .executed_child_directories(&["Nexus".into()])?;
    let overlay_addr = network.address;
    let public_key = network.public_key.clone();
    let (commands, input) = mpsc::channel(128);
    let health = health::PublishedHealth::default();
    let mut host = Host {
        health: health.clone(),
        read_endpoint_config,
        chain,
        levels: Default::default(),
        child_settings: Default::default(),
        child_mining: Default::default(),
        child_served: Default::default(),
        mining,
        storage,
        network,
        genesis: config.genesis_cid,
        ledger,
        last_ledger_save: std::time::Instant::now(),
        fetches: BTreeMap::new(),
        served: BTreeMap::new(),
        replies: BTreeMap::new(),
        next: 1,
        mined_replies: vec![],
        transient: vec![],
        needs_fetch: BTreeSet::new(),
        executed_children,
        discovery,
        queue: restored
            .into_iter()
            .map(|effect| Work::Mining(Box::new(effect)))
            .collect(),
    };
    if let Some(hosting) = initial_hosting
        && let Err(error) = host.configure_children(hosting).await
    {
        host.network.stop().await;
        return Err(error);
    }
    host.publish_health();
    let task = tokio::spawn(host.run(input));
    Ok(RunningHost {
        handle: HostHandle { commands, health },
        overlay_addr,
        public_key,
        task,
    })
}
impl Host {
    fn id(&mut self) -> anyhow::Result<u64> {
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("host IDs exhausted"))?;
        Ok(id)
    }
    fn enqueue(&mut self, effects: Vec<MiningEffect>) {
        self.queue.extend(
            effects
                .into_iter()
                .map(|effect| Work::Mining(Box::new(effect))),
        );
    }
    fn event(&mut self, event: MiningEvent) -> anyhow::Result<()> {
        let effects = self.mining.step(event, now())?;
        self.enqueue(effects);
        Ok(())
    }
    fn reply(&mut self, id: u64, value: (u16, Value)) {
        if let Some(reply) = self.replies.remove(&id) {
            let _ = reply.send(value);
        }
    }
    fn block(&self, cid: &str) -> anyhow::Result<Block> {
        Ok(Header::<Block>::from_cid(cid)?
            .resolve(self.storage.as_ref(), None)?
            .node
            .as_deref()
            .unwrap()
            .clone())
    }
    fn transactions(&self, cid: &str) -> anyhow::Result<Vec<Transaction>> {
        Ok(lattice_validation::resolve_transaction_content(
            &self.block(cid)?,
            self.storage.as_ref(),
            i64::MAX,
        )?
        .transactions)
    }
    fn tip_changed(&mut self) -> anyhow::Result<()> {
        let tip = self
            .chain
            .tree()
            .executed_prefix()
            .unwrap_or(&self.genesis)
            .to_owned();
        let old = self.mining.state().tip_cid().to_owned();
        if tip == old {
            return Ok(());
        }
        let mut old_path = BTreeSet::new();
        let mut at = Some(old.as_str());
        while let Some(cid) = at {
            old_path.insert(cid.to_owned());
            at = self.chain.tree().parent(cid);
        }
        let mut entered = vec![];
        let mut at = tip.clone();
        while !old_path.contains(&at) {
            entered.push(at.clone());
            let Some(parent) = self.chain.tree().parent(&at) else {
                break;
            };
            at = parent.into();
        }
        let common = at;
        let mut left = vec![];
        let mut at = old;
        while at != common {
            left.push(at.clone());
            let Some(parent) = self.chain.tree().parent(&at) else {
                break;
            };
            at = parent.into();
        }
        let mut confirmed = BTreeSet::new();
        for cid in entered {
            for tx in self.transactions(&cid)? {
                confirmed.insert(content_id(&tx)?);
            }
        }
        let mut returned = vec![];
        for cid in left.iter().rev() {
            returned.extend(
                self.transactions(cid)?
                    .into_iter()
                    .filter(|tx| content_id(tx).is_ok_and(|cid| !confirmed.contains(&cid))),
            );
        }
        self.event(MiningEvent::TipMoved(TipMove {
            tip_cid: tip,
            confirmed,
            returned,
            left,
        }))
    }
    async fn run(mut self, mut input: mpsc::Receiver<Command>) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            self.cancel_abandoned_fetches().await?;
            self.drain().await?;
            self.publish_health();
            self.maintain_retention();
            self.maintain_discovery().await?;
            tokio::select! {
             command=input.recv()=>match command {Some(Command::Request(route,body,reply))=>{self.api(route,body,reply)?},Some(Command::Volume(cid,reply))=>{let _=reply.send(self.storage.fetch_volume(&cid).and_then(operator::archive));},Some(Command::Shutdown)|None=>{let _=self.ledger.save();self.network.stop().await;return Ok(())}},
             event=self.network.next()=>match event?{Some(event)=>self.network_event(event).await?,None=>return Err(anyhow::anyhow!("Ivy stopped"))},
             _=timer.tick()=>{if self.last_ledger_save.elapsed()>=std::time::Duration::from_secs(30){let _=self.ledger.save();self.last_ledger_save=std::time::Instant::now();}self.network.tick().await?;let effects=self.chain.tick(now())?;self.queue.push_back(Work::Chain(effects));self.child_tick()?;}
            }
        }
    }
    async fn network_event(&mut self, event: NetworkEvent) -> anyhow::Result<()> {
        match event {
            NetworkEvent::ReadEndpoints(id, path, endpoints) => {
                let response = self.read_endpoint_result(&path, endpoints);
                self.reply(id, response);
            }
            NetworkEvent::Dialed(endpoint, paths, result) => {
                self.discovery.dialed(endpoint, &paths, result, now())
            }
            NetworkEvent::Ready(peer) => {
                let effects = self.chain.peer_ready(peer.clone(), now());
                self.child_peer_changed(&peer);
                self.queue.push_back(Work::Chain(effects));
            }
            NetworkEvent::HostingChanged(peer) => self.child_peer_changed(&peer),
            NetworkEvent::Closed(peer) => {
                self.discovery.session_lost(now());
                self.chain.peer_closed(&peer);
                for path in self.levels.ordered() {
                    self.levels.get_mut(&path).unwrap().peer_closed(&peer);
                }
            }
            NetworkEvent::Message(peer, topic, bytes) => {
                if topic == super::handshake::TRANSACTION_AVAILABLE {
                    if let Ok(value) = wire::decode_json_value(&bytes)
                        && let Some(cid) = value
                            .get("volumeRootCID")
                            .and_then(Value::as_str)
                            .filter(|cid| lattice_cashew::canonical_cid(cid).is_ok())
                    {
                        let cid = cid.to_owned();
                        let id = self.id()?;
                        self.fetches
                            .insert(id, Fetch::Transaction(peer.clone(), cid.clone()));
                        self.network.fetch(id, cid, false, Some(&peer)).await?;
                    }
                } else {
                    match self.chain.received_wire(&peer, &topic, &bytes, now()) {
                        Ok(WireDispatch::Applied(effects)) => {
                            self.queue.push_back(Work::Chain(effects))
                        }
                        Ok(WireDispatch::OtherLevel { path, message }) => {
                            if self.levels.get(&path).is_some() {
                                self.child_message(&path, &peer, message)?;
                                return Ok(());
                            }
                            let response = match message {
                                SyncMessage::GetStream { request_id, .. } => {
                                    Some(SyncMessage::Stream(StreamPage {
                                        request_id,
                                        log_id: String::new(),
                                        entries: vec![],
                                        has_more: false,
                                    }))
                                }
                                SyncMessage::GetData { request_id, .. }
                                | SyncMessage::GetAncestors { request_id, .. } => {
                                    Some(SyncMessage::Headers(HeadersResponse {
                                        request_id,
                                        entries: vec![],
                                        has_more: false,
                                    }))
                                }
                                _ => None,
                            };
                            if let Some(response) = response {
                                let (topic, bytes) = wire::encode(&response, &path)?;
                                self.network.send(peer, topic.into(), bytes, 0).await?;
                            }
                        }
                        Ok(WireDispatch::UnknownTopic) => {}
                        Err(crate::core::chain::CoreWireError::Decode(_)) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            NetworkEvent::Sent(id, ok) => {
                if let Some((path, peer, token)) = self.child_served.remove(&id)
                    && ok
                    && let Some(core) = self.levels.get_mut(&path)
                {
                    let effects = core.headers_served(&peer, token, now());
                    self.queue.push_back(Work::Child(path, effects));
                }
                if let Some((peer, token)) = self.served.remove(&id)
                    && ok
                {
                    let effects = self.chain.headers_served(&peer, token, now());
                    self.queue.push_back(Work::Chain(effects));
                }
            }
            NetworkEvent::FetchBusy(id) => {
                if matches!(self.fetches.get(&id), Some(Fetch::Operator(_))) {
                    self.fetches.remove(&id);
                    self.reply(id, (503, json!({"error":{"message":"busy"}})));
                } else {
                    // Reuse the ordinary unavailable-content transition without
                    // creating a consensus refusal or discarding a pending job.
                    self.network.requeue_unavailable(id);
                }
            }
            NetworkEvent::Fetched(id, volumes) => {
                if let Some(fetch) = self.fetches.remove(&id) {
                    if let Fetch::Operator(root) = fetch {
                        use base64::Engine;
                        let volume = volumes.and_then(|vs| vs.into_iter().find(|v| v.root == root));
                        let response = match volume {
                            Some(v) => (
                                200,
                                json!({"cid":v.root,"entries":v.entries.into_iter().map(|(cid,bytes)|(cid,base64::engine::general_purpose::STANDARD.encode(bytes))).collect::<BTreeMap<_,_>>()}),
                            ),
                            None => (404, json!({"error":{"message":"notServed"}})),
                        };
                        self.reply(id, response);
                        return Ok(());
                    }

                    if let Some(volumes) = &volumes {
                        let scope = format!("network-{id}");
                        self.storage.cache_volumes(&scope, volumes)?;
                        self.transient.push(scope);
                    }
                    match fetch {
                        Fetch::Operator(_) => unreachable!(),
                        Fetch::ChildBody(path, cid) => {
                            if let Some(core) = self.levels.get_mut(&path) {
                                let effects = if volumes.is_some() {
                                    core.body_arrived(&cid, now())
                                } else {
                                    core.body_unavailable(&cid, now())
                                };
                                self.queue.push_back(Work::Child(path, effects));
                            }
                        }
                        Fetch::ChildIndex(path, peer, cid) => {
                            if let Some(core) = self.levels.get_mut(&path) {
                                let bytes = volumes
                                    .as_ref()
                                    .and_then(|vs| vs.iter().find_map(|v| v.entries.get(&cid)));
                                let effects = core.child_index_fetched(
                                    &peer,
                                    &cid,
                                    bytes.map(Vec::as_slice),
                                    now(),
                                )?;
                                self.queue.push_back(Work::Child(path, effects));
                            }
                        }
                        Fetch::Body(cid) => {
                            let effects = if volumes.is_some() {
                                self.chain.body_arrived(&cid, now())
                            } else {
                                self.chain.body_unavailable(&cid, now())
                            };
                            self.queue.push_back(Work::Chain(effects));
                        }
                        Fetch::Index(peer, cid) => {
                            let bytes = volumes
                                .as_ref()
                                .and_then(|vs| vs.iter().find_map(|v| v.entries.get(&cid)));
                            let effects = self.chain.child_index_fetched(
                                &peer,
                                &cid,
                                bytes.map(Vec::as_slice),
                                now(),
                            )?;
                            self.queue.push_back(Work::Chain(effects));
                        }
                        Fetch::Transaction(peer, cid) => {
                            if volumes.is_some()
                                && let Ok(header) = Header::<Transaction>::from_cid(&cid)
                                    .and_then(|h| h.resolve(self.storage.as_ref(), None))
                            {
                                let mut tx = header.node.as_deref().unwrap().clone();
                                if let Ok(body) = tx.body.resolve(self.storage.as_ref(), None) {
                                    tx.body = body;
                                    self.receive_transaction(tx, TransactionOrigin::Peer(peer))?;
                                }
                            }
                        }
                    }
                }
            }
        };
        Ok(())
    }
    async fn drain(&mut self) -> anyhow::Result<()> {
        while let Some(work) = self.queue.pop_front() {
            match work {
   Work::ReadEndpoints(id,path)=>self.network.lookup_read_endpoints(id,path).await?,
   Work::ChildMining(path,effect)=>self.child_mining_effect(path,*effect).await?,
   Work::HostLevels(id,paths)=>{let response=match self.configure_children(paths).await{Ok(())=>(200,json!({"chains":self.levels.ordered().iter().map(|p|p.join("/")).collect::<Vec<_>>()})),Err(e)=>error(e)};self.reply(id,response);},
   Work::Child(path,effects)=>self.child_effects(path,effects).await?,
   Work::OperatorFetch(id,cid)=>{if self.fetches.contains_key(&id){self.network.retrieve(id,cid).await?;}},
   Work::Chain(effects)=>{
    for effect in effects.sync {match effect {
     SyncEffect::Send(peer,message)=>{let(topic,bytes)=wire::encode(&message,&["Nexus".into()])?;self.network.send(peer,topic.into(),bytes,0).await?;},
     SyncEffect::DisconnectStalled(peer)=>self.network.disconnect(&peer).await?,
     SyncEffect::ServeHeaders{peer,token,request_id,cids,has_more}=>{
      let mut entries=vec![];for cid in cids {if let Some((block,children))=self.storage.journal().header(&cid)?{entries.push(HeaderEntry{block,children:Some(children),proofs:vec![],spec:None});}}
      let mut response=HeadersResponse{request_id,entries,has_more};
      let(topic,bytes)=loop {match wire::encode(&SyncMessage::Headers(response.clone()),&["Nexus".into()]){Ok(value)=>break value,Err(wire::WireError::Oversized)if !response.entries.is_empty()=>{response.entries.pop();response.has_more=true;},Err(error)=>return Err(error.into())}};
      let id=self.id()?;self.served.insert(id,(peer.clone(),token));self.network.send(peer,topic.into(),bytes,id).await?;
     }
    }}
    for effect in effects.headers {match effect {HeaderEffect::DisconnectInvalidProof(peer)=>self.network.disconnect_invalid_proof(&peer).await?,HeaderEffect::FetchChildIndex{peer,cid}=>{let id=self.id()?;self.fetches.insert(id,Fetch::Index(peer.clone(),cid.clone()));self.network.fetch(id,cid,false,Some(&peer)).await?;}}}
    for effect in effects.bodies {match effect {
     BodyEffect::Fetch(cid)=>{if !self.needs_fetch.contains(&cid)&&self.storage.fetch_volume(&cid).is_some(){let effects=self.chain.body_arrived(&cid,now());self.queue.push_back(Work::Chain(effects));}else{let id=self.id()?;self.fetches.insert(id,Fetch::Body(cid.clone()));self.network.fetch(id,cid,true,None).await?;}},
     BodyEffect::Cancel(cid)=>{let ids:Vec<_>=self.fetches.iter().filter_map(|(id,f)|matches!(f,Fetch::Body(root) if root==&cid).then_some(*id)).collect();for id in ids {self.fetches.remove(&id);self.network.cancel_fetch(id).await?;}self.needs_fetch.remove(&cid);},
     BodyEffect::Connect(job)=>{
      let effect=match self.chain.tree().execute_root_connection(&job,self.storage.as_ref(),now())? {
       lattice_block_tree::RootConnectionExecution::Accepted(verified)=>{self.needs_fetch.remove(job.block_hash());let effects=self.chain.connected_root(&verified,now())?;if let Some(block)=verified.header().0.node.as_ref()&& let Some(children)=block.children.node.as_ref(){self.executed_children.extend(children.keys().cloned());}effects},
       lattice_block_tree::RootConnectionExecution::Rejected(rejected)=>self.chain.connected_rejected(rejected,now())?,
       lattice_block_tree::RootConnectionExecution::Unavailable=>{self.needs_fetch.insert(job.block_hash().into());self.chain.body_unavailable(job.block_hash(),now())},
      };self.queue.push_back(Work::Chain(effect));self.tip_changed()?;self.wake_children()?;
     }
    }}
   },
   Work::Mining(effect)=>match *effect {
    MiningEffect::Preflight(job)=>if let Some(completion)=self.mining.execute_root_preflight(&job,self.chain.tree(),now())?{let effects=self.mining.complete_root_preflight(completion,now())?;self.enqueue(effects);},
    MiningEffect::BuildTemplate(job)=>{
     let anchor=self.chain.tree().difficulty_anchor(&job.tip_cid).map(|a|lattice_validation::DifficultyAnchor{block_height:a.block_height,timestamp:a.timestamp,target:UInt256(a.target)});
     let contexts=self.child_templates(&job.tip_cid)?;
     match super::merged_template::build_merged_template(&job,self.storage.as_ref(),now(),anchor,&contexts){Ok(output)=>{self.child_policy_refused(&output.refused)?;self.event(MiningEvent::PolicyRefused{tip_cid:job.tip_cid.clone(),cids:output.refused.get(&vec!["Nexus".into()]).cloned().unwrap_or_default()})?;self.event(MiningEvent::TemplateBuilt(Box::new(job),Some(Box::new(output.build))))?;},Err(_)=>self.event(MiningEvent::TemplateBuilt(Box::new(job),None))?}
    },
    MiningEffect::TemplateIssued(id,t)=>self.reply(id,(200,json!({"workID":t.work_id,"block":t.block,"searchTarget":t.search_target,"targets":t.targets,"chainPath":["Nexus"],"expiresInMilliseconds":t.expires_at.saturating_sub(now()).max(0),"templateDigest":t.digest,"chains":t.chains.iter().map(|c|json!({"chainPath":c.chain_path,"target":c.target,"height":c.height.to_string(),"rewardRecipient":c.reward_recipient})).collect::<Vec<_>>()}))),
    MiningEffect::TemplateRefused(id,error)|MiningEffect::WorkRefused(id,error)=>self.reply(id,error_fn(error)),
    MiningEffect::TransactionRefused(id,e)=>self.reply(id,error_fn(e)),
    MiningEffect::TransactionAdmitted{reply_id,cid,count,bytes}=>self.reply(reply_id,(200,json!({"transactionCID":cid,"mempoolCount":count,"mempoolBytes":bytes}))),
    MiningEffect::AnnounceTransaction(cid)=>{let bytes=serde_json::to_vec(&json!({"volumeRootCID":cid}))?;for peer in self.network.peers(){self.network.send(peer,super::handshake::TRANSACTION_AVAILABLE.into(),bytes.clone(),0).await?;}},
    MiningEffect::Mined(id,block)=>{
     let cid=content_id(&block)?;let header=Volume::new(block.clone())?;let volumes=lattice_cashew::volume::serialize_recursive(&header,&())?;let scope=format!("mined-{cid}");self.storage.cache_volumes(&scope,&volumes)?;self.transient.push(scope);
     let children=block.children.resolve(self.storage.as_ref(),None)?.node.as_deref().unwrap().clone();
     let effects=self.chain.admit_header(&block,&children,now())?;self.queue.push_back(Work::Chain(effects));
     let child_proofs=self.mined_children(&block)?;
     // Reply is deferred until the queued body import crosses the journal barrier.
     self.mined_replies.push(MinedReply{id,cid,children:child_proofs});
    },
    MiningEffect::ReturnTransactions{left,carried}=>{for cid in left {for tx in self.transactions(&cid)?{if !carried.contains(&content_id(&tx)?){self.event(MiningEvent::TransactionReceived(tx,TransactionOrigin::Returned))?;}}}},
    MiningEffect::PoolChanged(_)=>unreachable!("durable mining consumes pool deltas")
   }
  }
        }
        for MinedReply { id, cid, children } in std::mem::take(&mut self.mined_replies) {
            let accepted = self.chain.tree().is_executed(&cid);
            let disposition = if self.chain.tree().executed_prefix() == Some(cid.as_str()) {
                "canonicalized"
            } else if accepted {
                "acceptedSide"
            } else {
                "invalid"
            };
            let mut durable_children = vec![];
            for (directory, child_cid) in children {
                let path = vec!["Nexus".into(), directory.clone()];
                let proofs = self
                    .storage
                    .journal()
                    .saved_proofs(&path, std::slice::from_ref(&child_cid))?;
                if proofs
                    .get(&child_cid)
                    .is_some_and(|ps| ps.iter().any(|p| p.root_cid == cid))
                {
                    durable_children.push(json!({"directory":directory,"childCID":child_cid}));
                }
            }
            self.reply(id,(200,json!({"accepted":accepted,"disposition":disposition,"tipCID":self.chain.tree().executed_prefix(),"durableChildProofs":durable_children})));
        }
        for scope in std::mem::take(&mut self.transient) {
            self.storage.release_cached_volumes(&scope)?;
        }
        Ok(())
    }
    fn api(
        &mut self,
        route: String,
        body: Value,
        reply: oneshot::Sender<(u16, Value)>,
    ) -> anyhow::Result<()> {
        match self.endpoint_path(&route) {
            Ok(Some(path)) => {
                if self.replies.len() >= 128 {
                    let _ = reply.send((503, json!({"error":{"message":"busy"}})));
                } else if path.len() > 2 && self.levels.get(&path[..path.len() - 1]).is_none() {
                    let _ = reply.send((404, json!({"error":{"message":"unknownChain"}})));
                } else {
                    let id = self.id()?;
                    self.replies.insert(id, reply);
                    self.queue.push_back(Work::ReadEndpoints(id, path));
                }
                return Ok(());
            }
            Err(e) => {
                let _ = reply.send(error(e));
                return Ok(());
            }
            Ok(None) => {}
        }
        if let Some(response) = self.child_author(&route, &body) {
            let _ = reply.send(response);
            return Ok(());
        }
        if route == "POST /sweep" {
            self.maintain_retention();
            let response = match self.storage.sweep() {
                Ok(removed) => (200, json!({"removed":removed})),
                Err(e) => error(e),
            };
            let _ = reply.send(response);
            return Ok(());
        }
        let route = if let Some(query) = route.strip_prefix("/status?") {
            let chain = form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "chainPath")
                .map(|(_, value)| value.into_owned());
            if chain.as_deref().is_none_or(|path| path == "Nexus") {
                "/status".to_owned()
            } else {
                route
            }
        } else {
            route
        };
        if route == "POST /hosted-chains" {
            let paths = configuration::parse(&body);
            match paths {
                Ok(paths) => {
                    if self.replies.len() >= 128 {
                        let _ = reply.send((503, json!({"error":{"message":"busy"}})));
                    } else {
                        let id = self.id()?;
                        self.replies.insert(id, reply);
                        self.queue.push_back(Work::HostLevels(id, paths));
                    }
                }
                Err(e) => {
                    let _ = reply.send(error(e));
                }
            }
            return Ok(());
        }
        if let Some(response) = self.read(&route) {
            let _ = reply.send(response);
            return Ok(());
        }
        if let Some(cid) = route.strip_prefix("/fetch/") {
            if lattice_cashew::canonical_cid(cid).is_err() {
                let _ = reply.send(error("invalidCID"));
                return Ok(());
            }
            if self.replies.len() >= 128 {
                let _ = reply.send((503, json!({"error":{"message":"busy"}})));
                return Ok(());
            }
            let id = self.id()?;
            self.replies.insert(id, reply);
            self.fetches.insert(id, Fetch::Operator(cid.into()));
            self.queue.push_back(Work::OperatorFetch(id, cid.into()));
            return Ok(());
        }
        if let Some(response) = self.operator(&route, &body)? {
            let _ = reply.send(response);
            return Ok(());
        }
        if let Some(peer) = route.strip_prefix("/serving-credit/") {
            let response = match self.ledger.standing(peer) {
                Ok(standing) => (200, json!({"peer":peer,"standing":standing})),
                Err(e) => error(e),
            };
            let _ = reply.send(response);
            return Ok(());
        }
        if route == "/serving-credit" {
            let response = match body
                .get("peer")
                .and_then(Value::as_str)
                .zip(body.get("bytes").and_then(Value::as_i64))
            {
                Some((peer, bytes)) => match self.ledger.credit(peer, bytes) {
                    Ok(standing) => (200, json!({"peer":peer,"standing":standing})),
                    Err(super::serving::ServingError::NotSaved) => {
                        (503, json!({"error":{"message":"notSaved"}}))
                    }
                    Err(e) => error(e),
                },
                None => error("invalid serving credit request"),
            };
            let _ = reply.send(response);
            return Ok(());
        }
        if route == "/status" {
            let tip = self.mining.state().tip_cid();
            let txs = self
                .mining
                .state()
                .mempool()
                .items()
                .into_iter()
                .map(|i| i.transaction)
                .collect::<Vec<_>>();
            let _=reply.send((200,json!({"phase":"active","chainPath":["Nexus"],"nexusGenesisCID":self.genesis,"tipCID":tip,"height":self.chain.tree().height(tip).unwrap_or(0).to_string(),"revision":self.chain.tree().current_revision().to_string(),"mempoolCount":self.mining.state().mempool().count(),"mempoolBytes":self.mining.state().mempool().byte_count(),"templateDigest":super::merged_template::merged_template_digest(tip,&txs,&self.child_templates(tip)?),"bestHeaderHeight":self.chain.tree().highest_block_height().to_string()})));
            return Ok(());
        }
        if self.replies.len() >= 128 {
            let _ = reply.send((503, json!({"error":{"message":"busy"}})));
            return Ok(());
        }
        let id = self.id()?;
        let event: Result<MiningEvent, String> = match route.as_str() {
            "/mining/templates" => parse_template(&body, &self.levels.ordered())
                .map(|request| MiningEvent::TemplateRequested(id, Box::new(request))),
            "/mining/work" => body
                .get("workID")
                .and_then(Value::as_str)
                .zip(body.get("nonce").and_then(Value::as_u64))
                .map(|(work_id, nonce)| MiningEvent::SubmitWork {
                    reply_id: id,
                    work_id: work_id.into(),
                    nonce,
                })
                .ok_or("invalid work request".into()),
            "/transactions" => body
                .get("transaction")
                .cloned()
                .ok_or("missing transaction".into())
                .and_then(super::transaction::decode)
                .map(|tx| MiningEvent::TransactionReceived(tx, TransactionOrigin::Submitted(id))),
            _ => Err("unknown route".into()),
        };
        match event {
            Ok(event) => {
                self.replies.insert(id, reply);
                match event {
                    MiningEvent::TransactionReceived(tx, origin) => {
                        self.receive_transaction(tx, origin)?
                    }
                    other => self.event(other)?,
                };
            }
            Err(message) => {
                let _ = reply.send(error(message));
            }
        }
        Ok(())
    }
}
fn error_fn(cause: impl ToString) -> (u16, Value) {
    error(cause.to_string())
}
fn parse_template(body: &Value, hosted: &[Vec<String>]) -> Result<TemplateRequest, String> {
    let mut request = TemplateRequest::default();
    if !body.is_object() {
        return Err("invalid template request".into());
    }
    if body.get("rewards").is_some() {
        return Err("rewards was replaced by recipients".into());
    }
    if body.get("mode").is_some() {
        return Err("mining template modes do not exist".into());
    }
    let empty = Vec::new();
    let recipients = match body.get("recipients").filter(|v| !v.is_null()) {
        Some(value) => value.as_array().ok_or("invalid recipients")?,
        None => &empty,
    };
    for recipient in recipients {
        let path: Vec<String> = serde_json::from_value(
            recipient
                .get("chainPath")
                .cloned()
                .ok_or("missing chain path")?,
        )
        .map_err(|e| e.to_string())?;
        if path != ["Nexus"] && !hosted.contains(&path) {
            return Err("host does not serve requested chain".into());
        }
        let address = recipient
            .get("address")
            .and_then(Value::as_str)
            .ok_or("missing address")?;
        let duplicate = if path == ["Nexus"] {
            request.reward_recipient.replace(address.into()).is_some()
        } else {
            request
                .child_recipients
                .insert(path, address.into())
                .is_some()
        };
        if duplicate {
            return Err("duplicate recipient".into());
        }
    }
    if let Some(minimum) = body.get("minimumWork").filter(|v| !v.is_null()) {
        for entry in minimum.as_array().ok_or("invalid minimum work")? {
            let path: Vec<String> = serde_json::from_value(
                entry
                    .get("chainPath")
                    .cloned()
                    .ok_or("missing chain path")?,
            )
            .map_err(|e| e.to_string())?;
            if path != ["Nexus"] && !hosted.contains(&path) {
                return Err("host does not serve requested chain".into());
            }
            let work = serde_json::from_value(entry.get("work").cloned().ok_or("missing work")?)
                .map_err(|e| e.to_string())?;
            if request.minimum_work.insert(path, work).is_some() {
                return Err("duplicate minimum work".into());
            }
        }
    }
    Ok(request)
}
