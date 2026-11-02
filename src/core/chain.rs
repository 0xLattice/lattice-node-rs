//! Root-chain orchestration over the actual durable store. Events return network
//! and body effects only after any resulting cursor/fact transaction succeeds.
use super::{
    body::{BodyEffect, BodyPipeline},
    headers::{HeaderEffect, HeaderTurn, RootHeaderPool},
    stream::*,
    sync::*,
};
use crate::storage::*;
use lattice_block_tree::ChainTree;
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader, ChainRuntimeContext};
use std::sync::Arc;
#[derive(Debug, Default)]
pub struct ChainEffects {
    pub sync: Vec<SyncEffect>,
    pub bodies: Vec<BodyEffect>,
    pub headers: Vec<HeaderEffect>,
}
pub struct RootChainCore {
    pub(super) storage: Arc<NodeStorage>,
    pub(super) tree: ChainTree,
    pub(super) sync: HeaderSync,
    bodies: BodyPipeline,
    body_window: usize,
    headers: RootHeaderPool,
}
impl RootChainCore {
    /// Bootstrap or recover using a composed validated root capability. Existing
    /// journal history is restored before importing the idempotent genesis.
    pub fn open(
        storage: Arc<NodeStorage>,
        genesis: &lattice_validation::VerifiedRootGenesis,
        new_log_id: &str,
        config: StreamConfig,
        body_window: usize,
    ) -> Result<Self, StoreError> {
        let context = ChainRuntimeContext::new(vec!["Nexus".into()], Some(genesis.block_cid()))
            .map_err(|e| StoreError::InvalidConfiguration(e.to_string()))?;
        let spec = genesis
            .spec()
            .0
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("genesis spec not materialized".into()))?
            .clone();
        let snapshot = storage.journal().recovery_snapshot(context.path())?;
        let mut tree = if snapshot.batches.is_empty() {
            let mut tree = ChainTree::empty(context);
            if !tree.hold_spec(spec, &genesis.spec().0.raw_cid) {
                return Err(StoreError::Corrupt("genesis spec content mismatch".into()));
            }
            tree
        } else {
            ChainTree::restore_with_parent(
                &snapshot.batches,
                snapshot.revision_floor,
                context,
                None,
                vec![spec],
            )?
        };
        let log_id = snapshot.log_id.unwrap_or_else(|| new_log_id.into());
        storage.import_root_genesis(&mut tree, genesis, &log_id)?;
        let snapshot = storage.journal().recovery_snapshot(&["Nexus".into()])?;
        let log = WeighLog::restoring(log_id, log_entries(&snapshot.batches, true))
            .map_err(|e| StoreError::Corrupt(e.into()))?;
        Ok(Self {
            storage,
            tree,
            sync: HeaderSync::new(log, snapshot.cursors, config),
            bodies: Default::default(),
            body_window,
            headers: Default::default(),
        })
    }
    pub fn set_finality_window(&mut self, window: Option<u64>) {
        self.headers.config.finality = window.filter(|n| *n > 0);
        self.headers.declined = DeclinedSet::default();
        self.headers.refresh_finality(&self.tree);
    }
    pub(crate) fn acquisition_frontier(&self) -> std::collections::BTreeSet<String> {
        super::body::next_to_connect(&self.tree, self.body_window)
            .into_iter()
            .collect()
    }
    pub fn tree(&self) -> &ChainTree {
        &self.tree
    }
    pub fn sync(&self) -> &HeaderSync {
        &self.sync
    }
    pub fn peer_ready(&mut self, peer: PeerId, now: i64) -> ChainEffects {
        let turn = self.sync.peer_ready(peer, now);
        self.finish(turn, now)
    }
    pub fn peer_closed(&mut self, peer: &PeerId) {
        self.sync.drop_peer(peer);
        self.headers.drop_peer(peer);
    }
    fn finish(&mut self, turn: SyncTurn, now: i64) -> ChainEffects {
        self.headers.refresh_finality(&self.tree);
        ChainEffects {
            sync: turn.effects,
            headers: vec![],
            bodies: self.bodies.schedule(&self.tree, self.body_window, now),
        }
    }
    /// Stage a cloned sync turn before swapping it into the live core. A failed
    /// cursor transaction neither advances session state nor releases effects.
    pub fn stream_page(
        &mut self,
        peer: &PeerId,
        page: StreamPage,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let mut next = self.sync.clone();
        let applied = |e: &LogEntry| {
            self.headers.declined.contains(&e.block)
                || (e.kind == LogKind::Header && self.tree.contains(&e.block))
        };
        let turn = next.receive_page(peer, page, now, applied, |e| !applied(e));
        if !turn.cursors.is_empty() {
            self.storage.journal().stage_node_facts(
                &[NodeFactBatch {
                    path: vec!["Nexus".into()],
                    facts: vec![],
                    headers: vec![],
                    proofs: vec![],
                    volume_roots: vec![],
                    cursors: turn.cursors.clone(),
                }],
                &next.log.id,
            )?;
        }
        self.sync = next;
        Ok(self.finish(turn, now))
    }
    pub fn request(&mut self, peer: &PeerId, message: SyncMessage, now: i64) -> ChainEffects {
        let turn = self.sync.receive_request(peer, message, &self.tree, now);
        self.finish(turn, now)
    }
    pub fn headers_served(&mut self, peer: &PeerId, token: u64, now: i64) -> ChainEffects {
        let turn = self.sync.headers_served(peer, token, &self.tree);
        self.finish(turn, now)
    }
    pub fn admit_header(
        &mut self,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let admitted = self.storage.import_root_header(
            &mut self.tree,
            block,
            children,
            now,
            &self.sync.log.id,
        )?;
        let mut turn = SyncTurn::default();
        if let HeaderImport::Applied(update) = admitted {
            turn = self
                .sync
                .publish_durable(&update.batches, true)
                .map_err(|e| StoreError::Corrupt(e.into()))?;
        }
        Ok(self.finish(turn, now))
    }
    pub fn connected_root(
        &mut self,
        verified: &lattice_validation::VerifiedRootTransition,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        if self.bodies.connecting.as_deref() != Some(verified.block_cid()) {
            return Err(StoreError::InvalidConfiguration(
                "stale or unsolicited connect completion".into(),
            ));
        }
        self.connected_issued_root(verified, now)
    }
    pub(crate) fn connected_issued_root(
        &mut self,
        verified: &lattice_validation::VerifiedRootTransition,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let admitted =
            self.storage
                .import_root_transition(&mut self.tree, verified, &self.sync.log.id)?;
        let turn = if let HeaderImport::Applied(update) = admitted {
            self.sync
                .publish_durable(&update.batches, true)
                .map_err(|e| StoreError::Corrupt(e.into()))?
        } else {
            SyncTurn::default()
        };
        self.bodies.completed(verified.block_cid());
        Ok(self.finish(turn, now))
    }
    pub fn received_headers(
        &mut self,
        peer: &PeerId,
        response: HeadersResponse,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let mut sync = self.sync.clone();
        let mut headers = self.headers.clone();
        let mut tree = self.tree.clone();
        let Some(mut work) = sync.begin_headers_response(peer, response) else {
            return Ok(ChainEffects::default());
        };
        let mut turn = HeaderTurn::default();
        let mut received = std::collections::BTreeSet::new();
        for entry in std::mem::take(&mut work.entries) {
            match headers.accept(entry, peer, &tree, &mut sync, now, &mut turn)? {
                Some(cid) => {
                    received.insert(cid);
                }
                None => break,
            }
        }
        if let Some(asked) = &work.asked_parent
            && !received.contains(asked)
        {
            headers.missing_ancestor_response(asked, peer);
        }
        let response = sync.finish_headers_response(peer, work, &received, now, |e| {
            !tree.contains(&e.block) && !headers.declined.contains(&e.block)
        });
        turn.sync.effects.extend(response.effects);
        headers.drain(&mut tree, &mut sync, now, &mut turn)?;
        self.commit_header_turn(tree, sync, headers, turn, now)
    }
    pub fn child_index_fetched(
        &mut self,
        peer: &PeerId,
        cid: &str,
        bytes: Option<&[u8]>,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let mut sync = self.sync.clone();
        let mut headers = self.headers.clone();
        let mut tree = self.tree.clone();
        let mut turn = HeaderTurn::default();
        headers.child_index_response(&mut sync, peer, cid, bytes)?;
        headers.drain(&mut tree, &mut sync, now, &mut turn)?;
        self.commit_header_turn(tree, sync, headers, turn, now)
    }
    fn commit_header_turn(
        &mut self,
        tree: ChainTree,
        mut sync: HeaderSync,
        mut headers: RootHeaderPool,
        mut turn: HeaderTurn,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        // A shorter canonical tip reopens the window before any cursor decision.
        headers.refresh_finality(&tree);
        for peer in sync.peers.keys().cloned().collect::<Vec<_>>() {
            sync.advance_cursor(
                &peer,
                |e| tree.contains(&e.block) || headers.declined.contains(&e.block),
                &mut turn.sync,
            );
        }
        if !turn.facts.is_empty() || !turn.headers.is_empty() || !turn.sync.cursors.is_empty() {
            self.storage.journal().stage_node_facts(
                &[NodeFactBatch {
                    path: vec!["Nexus".into()],
                    facts: turn.facts.clone(),
                    headers: turn.headers,
                    proofs: vec![],
                    volume_roots: vec![],
                    cursors: turn.sync.cursors.clone(),
                }],
                &sync.log.id,
            )?;
        }
        let pushed = sync
            .publish_durable(&turn.facts, true)
            .map_err(|e| StoreError::Corrupt(e.into()))?;
        turn.sync.effects.splice(0..0, pushed.effects);
        self.tree = tree;
        self.sync = sync;
        self.headers = headers;
        let mut effects = self.finish(turn.sync, now);
        effects.headers = turn.effects;
        Ok(effects)
    }
    pub fn tick(&mut self, now: i64) -> Result<ChainEffects, StoreError> {
        let mut sync = self.sync.clone();
        let mut headers = self.headers.clone();
        let mut tree = self.tree.clone();
        let mut turn = HeaderTurn {
            sync: sync.tick(now, |e| {
                e.kind != LogKind::Header
                    || (!tree.contains(&e.block) && !headers.declined.contains(&e.block))
            }),
            ..Default::default()
        };
        for effect in &turn.sync.effects {
            if let SyncEffect::DisconnectStalled(peer) = effect {
                headers.drop_peer(peer);
            }
        }
        headers.drain(&mut tree, &mut sync, now, &mut turn)?;
        self.commit_header_turn(tree, sync, headers, turn, now)
    }
    pub fn body_arrived(&mut self, cid: &str, now: i64) -> ChainEffects {
        self.bodies.body_fetched(cid);
        self.finish(SyncTurn::default(), now)
    }
    pub(crate) fn wanted_root_bodies(&self) -> std::collections::BTreeSet<String> {
        let mut wanted: std::collections::BTreeSet<_> =
            super::body::body_window(&self.tree, self.body_window)
                .into_iter()
                .collect();
        wanted.extend(self.bodies.connecting.iter().cloned());
        wanted
    }
    pub fn owns_root_connection(&self, cid: &str) -> bool {
        self.bodies.connecting.as_deref() == Some(cid)
    }
    /// A completed worker whose evidence changed must free its slot and let
    /// the ordinary pipeline decide whether to connect again or fetch/wait.
    pub fn root_connection_superseded(&mut self, cid: &str, now: i64) -> ChainEffects {
        if self.owns_root_connection(cid) {
            self.bodies.completed(cid);
        }
        self.finish(SyncTurn::default(), now)
    }
    pub fn body_unavailable(&mut self, cid: &str, now: i64) -> ChainEffects {
        self.bodies.park_unavailable(cid, now, 1000, 60000);
        self.finish(SyncTurn::default(), now)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoreWireError {
    #[error(transparent)]
    Decode(#[from] super::wire::WireError),
    #[error(transparent)]
    Storage(#[from] StoreError),
}
/// A level never consumes another level's message. The host retains its path
/// and can route it to a hosted child or answer using its unhosted-level policy.
#[derive(Debug)]
pub enum WireDispatch {
    UnknownTopic,
    OtherLevel {
        path: Vec<String>,
        message: SyncMessage,
    },
    Applied(ChainEffects),
}
impl RootChainCore {
    /// Decode before entering the deterministic core; resulting facts/cursors
    /// cross the same durability barrier as equivalent decoded events.
    pub fn received_wire(
        &mut self,
        peer: &PeerId,
        topic: &str,
        payload: &[u8],
        now: i64,
    ) -> Result<WireDispatch, CoreWireError> {
        let Some((path, message)) = super::wire::decode(topic, payload)? else {
            return Ok(WireDispatch::UnknownTopic);
        };
        if self
            .tree
            .context()
            .is_none_or(|context| context.path() != path)
        {
            return Ok(WireDispatch::OtherLevel { path, message });
        }
        let effects = match message {
            SyncMessage::Stream(page) => self.stream_page(peer, page, now)?,
            SyncMessage::Headers(response) => self.received_headers(peer, response, now)?,
            request => self.request(peer, request, now),
        };
        Ok(WireDispatch::Applied(effects))
    }
}

impl RootChainCore {
    pub fn connected_rejected(
        &mut self,
        rejection: lattice_block_tree::VerifiedRootRejection,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let cid = rejection.block_hash().to_owned();
        if self.bodies.connecting.as_deref() != Some(&cid) {
            return Err(StoreError::InvalidConfiguration(
                "stale or unsolicited rejection completion".into(),
            ));
        }
        self.connected_issued_rejection(rejection, now)
    }
    pub(crate) fn connected_issued_rejection(
        &mut self,
        rejection: lattice_block_tree::VerifiedRootRejection,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let cid = rejection.block_hash().to_owned();
        let update =
            self.storage
                .import_root_rejection(&mut self.tree, rejection, &self.sync.log.id)?;
        let turn = self
            .sync
            .publish_durable(&update.batches, true)
            .map_err(|e| StoreError::Corrupt(e.into()))?;
        self.bodies.completed(&cid);
        Ok(self.finish(turn, now))
    }
}

/// Regression helper for the real durable cursor boundary with an already
/// prepared shorter-tip graph. Admission validity is tested independently.
#[cfg(test)]
pub(crate) fn assert_shorter_tip_reopens_before_cursor(long: ChainTree, short: ChainTree) {
    use super::sync::{LogEntry, StreamEntry};
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(
        NodeStorage::open(
            directory.path(),
            long.genesis_roots()[0].clone(),
            "finality",
        )
        .unwrap(),
    );
    let mut core = RootChainCore {
        storage: storage.clone(),
        tree: long,
        sync: HeaderSync::new(
            WeighLog::new("finality"),
            Default::default(),
            Default::default(),
        ),
        bodies: Default::default(),
        body_window: 8,
        headers: Default::default(),
    };
    core.set_finality_window(Some(2));
    let cid = lattice_cashew::cid_for_bytes(0x71, b"reopened-header");
    let peer = PeerId {
        key: "window-peer".into(),
        session: 1,
    };
    let ready = core.peer_ready(peer.clone(), 0);
    let request_id = ready
        .sync
        .iter()
        .find_map(|effect| match effect {
            SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => Some(*request_id),
            _ => None,
        })
        .unwrap();
    core.stream_page(
        &peer,
        StreamPage {
            request_id,
            log_id: "remote".into(),
            has_more: false,
            entries: vec![StreamEntry {
                position: 1,
                entry: LogEntry::header(&cid),
            }],
        },
        0,
    )
    .unwrap();
    core.headers.declined.insert(cid.clone());
    core.commit_header_turn(
        short,
        core.sync.clone(),
        core.headers.clone(),
        HeaderTurn::default(),
        1,
    )
    .unwrap();
    assert!(!core.headers.declined.contains(&cid));
    let snapshot = storage
        .journal()
        .recovery_snapshot(&["Nexus".into()])
        .unwrap();
    assert_eq!(
        snapshot
            .cursors
            .get(&peer.key)
            .map_or(0, |cursor| cursor.position),
        0,
        "the reopened entry must not be durably acknowledged using its stale declined mark"
    );
}
