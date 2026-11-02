//! Hosted child levels over durable facts. Parent observations are borrowed for
//! one turn; proof of work never substitutes for executed parent continuity.
use super::{
    body::BodyPipeline,
    chain::{ChainEffects, RootChainCore},
    stream::*,
    sync::*,
};
use crate::storage::*;
use lattice_block_tree::{
    ChainTree, ChildBlockProof, CrossChainEvidenceRequirement, ParentAnchorFailure,
};
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader, ChainRuntimeContext, ChainSpec};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

mod derivation;
mod ingestion;

pub struct ChildChainCore {
    derivations: BTreeMap<(String, String), super::child_proofs::ChildDeriveJob>,
    derivation_parent_revision: Option<u64>,
    proofs: super::child_proofs::ChildProofSync,
    pending: super::pending::PendingQueue,
    proof_config: super::child_proofs::ChildProofConfig,
    header_config: super::headers::HeaderConfig,
    declined: DeclinedSet,
    finality_height: u64,
    storage: Arc<NodeStorage>,
    tree: ChainTree,
    sync: HeaderSync,
    bodies: BodyPipeline,
    body_window: usize,
    journal_log_id: String,
}
impl ChildChainCore {
    pub fn open(
        storage: Arc<NodeStorage>,
        context: ChainRuntimeContext,
        parent: &ChainTree,
        mut specs: Vec<ChainSpec>,
        new_log_id: &str,
        config: StreamConfig,
        body_window: usize,
    ) -> Result<Self, StoreError> {
        if context.is_root()
            || parent
                .context()
                .is_none_or(|p| p.path() != &context.path()[..context.path().len() - 1])
        {
            return Err(StoreError::InvalidConfiguration(
                "hosted child requires its immediate parent".into(),
            ));
        }
        let snapshot = storage.journal().recovery_snapshot(context.path())?;
        // Genesis admission durably retains the authenticated spec before its
        // journal facts. Recover every branch's spec, not just the current tip.
        let mut known_specs = specs
            .iter()
            .map(lattice_primitives::content_id)
            .collect::<Result<BTreeSet<_>, _>>()?;
        for fact in snapshot.batches.iter().flat_map(|batch| batch.facts()) {
            if let lattice_block_tree::ChainFact::Block(block) = fact
                && block.parent_block_hash.is_none()
                && known_specs.insert(block.spec_cid.clone())
            {
                let spec = lattice_cashew::Header::<ChainSpec>::from_cid(&block.spec_cid)?
                    .resolve(storage.as_ref(), None)?;
                specs.push(
                    spec.node
                        .as_deref()
                        .ok_or_else(|| {
                            StoreError::Corrupt("recovered child spec was not materialized".into())
                        })?
                        .clone(),
                );
            }
        }
        let tree = if snapshot.batches.is_empty() {
            let mut tree = ChainTree::empty(context.clone());
            for spec in specs {
                let cid = lattice_primitives::content_id(&spec)?;
                if !tree.hold_spec(spec, &cid) {
                    return Err(StoreError::Corrupt("invalid child spec".into()));
                }
            }
            tree
        } else {
            ChainTree::restore_with_parent(
                &snapshot.batches,
                snapshot.revision_floor,
                context.clone(),
                Some(parent),
                specs,
            )?
        };
        let journal_log_id = snapshot.log_id.unwrap_or_else(|| new_log_id.into());
        let log = WeighLog::restoring(
            format!("{}/{}", journal_log_id, context.path().join("/")),
            log_entries(&snapshot.batches, false),
        )
        .map_err(|e| StoreError::Corrupt(e.into()))?;
        Ok(Self {
            derivations: BTreeMap::new(),
            derivation_parent_revision: None,
            proofs: Default::default(),
            pending: Default::default(),
            proof_config: Default::default(),
            header_config: Default::default(),
            declined: Default::default(),
            finality_height: 0,
            storage,
            tree,
            sync: HeaderSync::new(log, snapshot.cursors, config),
            bodies: Default::default(),
            body_window,
            journal_log_id,
        })
    }
    /// Read-only operator status; does not confer proof or execution authority.
    pub fn header_status(&self, cid: &str) -> &'static str {
        if self.tree.contains(cid) {
            "tree"
        } else if self.pending.entries().contains_key(cid) {
            "pending"
        } else if self.proofs.awaiting.contains_key(cid) {
            "awaiting"
        } else {
            "dropped"
        }
    }
    pub fn set_finality_window(&mut self, window: Option<u64>) {
        let window = window.filter(|n| *n > 0);
        if self.header_config.finality != window {
            self.declined = DeclinedSet::default();
        }
        self.header_config.finality = window;
        self.finality_height = super::finality::height(&self.tree);
    }
    fn finality_done(&self, entry: &LogEntry) -> bool {
        self.declined.contains(&entry.block)
            || (entry.kind == LogKind::Proof
                && self.tree.height(&entry.block).is_some_and(|height| {
                    super::finality::past(&self.tree, height, self.header_config.finality)
                }))
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
    pub fn awaiting_parent(&self) -> &BTreeMap<String, CrossChainEvidenceRequirement> {
        &self.bodies.awaiting_parent
    }
    fn refresh_finality(&mut self) {
        let height = super::finality::height(&self.tree);
        if height < self.finality_height {
            self.declined = DeclinedSet::default();
        }
        self.finality_height = height;
    }
    fn finish(&mut self, sync: SyncTurn, now: i64) -> ChainEffects {
        self.refresh_finality();
        ChainEffects {
            sync: sync.effects,
            headers: vec![],
            bodies: self.bodies.schedule(&self.tree, self.body_window, now),
        }
    }
    fn published(&mut self, admitted: HeaderImport, now: i64) -> Result<ChainEffects, StoreError> {
        self.refresh_finality();
        let mut next = self.sync.clone();
        let mut turn = match admitted {
            HeaderImport::Applied(update) => next
                .publish_durable(&update.batches, false)
                .map_err(|e| StoreError::Corrupt(e.into()))?,
            HeaderImport::Duplicate => SyncTurn::default(),
        };
        let log = next.log.clone();
        for peer in next.peers.keys().cloned().collect::<Vec<_>>() {
            next.advance_cursor(
                &peer,
                |e| log.contains(e) || self.finality_done(e),
                &mut turn,
            );
        }
        self.persist_cursors(&turn.cursors)?;
        self.sync = next;
        Ok(self.finish(turn, now))
    }
    pub fn peer_ready(&mut self, peer: PeerId, now: i64) -> ChainEffects {
        let turn = self.sync.peer_ready(peer, now);
        self.finish(turn, now)
    }
    pub fn peer_closed(&mut self, peer: &PeerId) {
        self.sync.drop_peer(peer);
        self.proofs.drop_peer(peer);
        for cid in self.pending.entries().keys().cloned().collect::<Vec<_>>() {
            let h = self.pending.metadata_mut(&cid).unwrap();
            if h.source() == Some(peer) {
                h.asked_parent = false;
            }
            h.announcers.retain(|p| p != peer);
        }
    }
    pub fn request(&mut self, peer: &PeerId, message: SyncMessage, now: i64) -> ChainEffects {
        let turn = self.sync.receive_request(peer, message, &self.tree, now);
        self.finish(turn, now)
    }
    /// Cursors acknowledge durable weigh-log entries or entries declined by
    /// the configured finality policy. Otherwise a held child header does not
    /// acknowledge an unseen carrier proof naming that header.
    pub fn stream_page(
        &mut self,
        peer: &PeerId,
        page: StreamPage,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let mut next = self.sync.clone();
        let applied = |entry: &LogEntry| self.sync.log.contains(entry) || self.finality_done(entry);
        let turn = next.receive_page(peer, page, now, applied, |e| !applied(e));
        self.persist_cursors(&turn.cursors)?;
        self.sync = next;
        Ok(self.finish(turn, now))
    }
    fn advance_finality_cursors(&mut self, now: i64) -> Result<ChainEffects, StoreError> {
        self.refresh_finality();
        let mut next = self.sync.clone();
        let log = next.log.clone();
        let mut turn = SyncTurn::default();
        for peer in next.peers.keys().cloned().collect::<Vec<_>>() {
            next.advance_cursor(
                &peer,
                |e| log.contains(e) || self.finality_done(e),
                &mut turn,
            );
        }
        self.persist_cursors(&turn.cursors)?;
        self.sync = next;
        Ok(self.finish(turn, now))
    }
    fn persist_cursors(&self, cursors: &BTreeMap<String, StreamCursor>) -> Result<(), StoreError> {
        if !cursors.is_empty() {
            self.storage.journal().stage_node_facts(
                &[NodeFactBatch {
                    path: self.tree.context().unwrap().path().to_vec(),
                    facts: vec![],
                    headers: vec![],
                    proofs: vec![],
                    volume_roots: vec![],
                    cursors: cursors.clone(),
                }],
                &self.journal_log_id,
            )?;
        }
        Ok(())
    }
    /// Only held headers may be served. A child's securing proof and genesis
    /// spec accompany its bytes; a header alone cannot establish child work.
    pub fn served_entries(&self, cids: &[String]) -> Result<Vec<HeaderEntry>, StoreError> {
        let path = self.tree.context().unwrap().path();
        let proofs = self.storage.journal().saved_proofs(path, cids)?;
        let mut entries = Vec::new();
        for cid in cids {
            if !self.tree.contains(cid) {
                continue;
            }
            let Some((block, children)) = self.storage.journal().header(cid)? else {
                continue;
            };
            let spec = if block.height == 0 {
                Some(
                    block
                        .spec
                        .0
                        .resolve(self.storage.as_ref(), None)?
                        .node
                        .as_deref()
                        .ok_or_else(|| {
                            StoreError::Corrupt("served child spec was not materialized".into())
                        })?
                        .clone(),
                )
            } else {
                None
            };
            entries.push(HeaderEntry {
                block,
                children: Some(children),
                proofs: proofs.get(cid).cloned().unwrap_or_default(),
                spec,
            });
        }
        Ok(entries)
    }
    pub fn headers_served(&mut self, peer: &PeerId, token: u64, now: i64) -> ChainEffects {
        let turn = self.sync.headers_served(peer, token, &self.tree);
        self.finish(turn, now)
    }
    pub fn tick(&mut self, now: i64) -> Result<ChainEffects, StoreError> {
        let log = self.sync.log.clone();
        let mut next = self.sync.clone();
        let turn = next.tick(now, |e| !log.contains(e) && !self.finality_done(e));
        self.sync = next;
        let mut effects = self.finish(turn, now);
        let pending = self.drain_proven_headers(now)?;
        effects.sync.extend(pending.sync);
        effects.bodies.extend(pending.bodies);
        effects.headers.extend(pending.headers);
        Ok(effects)
    }
    /// Complete proof-backed headers use the same store barrier as root imports.
    /// Missing indexes/specs and asynchronous proof downloads remain shell work.
    pub fn admit_header(
        &mut self,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        spec: Option<&ChainSpec>,
        proof: &ChildBlockProof,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let admitted = if block.parent.is_none() {
            self.storage.import_child_genesis_header(
                &mut self.tree,
                block,
                children,
                spec.ok_or_else(|| {
                    StoreError::InvalidConfiguration(
                        "child genesis requires its authenticated spec".into(),
                    )
                })?,
                proof,
                &self.journal_log_id,
            )?
        } else {
            self.storage.import_child_header(
                &mut self.tree,
                block,
                children,
                proof,
                now,
                &self.journal_log_id,
            )?
        };
        self.published(admitted, now)
    }
    fn connecting(&self, cid: &str) -> Result<(), StoreError> {
        if self.bodies.connecting.as_deref() != Some(cid) {
            return Err(StoreError::InvalidConfiguration(
                "stale or unsolicited child completion".into(),
            ));
        }
        Ok(())
    }
    fn execution_result(
        &mut self,
        cid: &str,
        result: Result<HeaderImport, StoreError>,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        match result {
            Ok(admitted) => {
                self.bodies.completed(cid);
                self.published(admitted, now)
            }
            Err(StoreError::ParentAnchor(ParentAnchorFailure::Required(link))) => {
                self.bodies.await_parent(
                    cid.into(),
                    CrossChainEvidenceRequirement::ParentStateContinuity(link),
                );
                Ok(self.finish(SyncTurn::default(), now))
            }
            Err(error) => Err(error),
        }
    }
    pub fn connected_genesis(
        &mut self,
        parent: &ChainTree,
        verified: &lattice_validation::VerifiedChildGenesis,
        proof: &ChildBlockProof,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        self.connecting(verified.block_cid())?;
        let result = self.storage.import_child_genesis(
            &mut self.tree,
            parent,
            verified,
            proof,
            &self.journal_log_id,
        );
        self.execution_result(verified.block_cid(), result, now)
    }
    pub fn connected_transition(
        &mut self,
        parent: &ChainTree,
        verified: &lattice_validation::VerifiedChildTransition,
        proof: &ChildBlockProof,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        self.connecting(verified.block_cid())?;
        let result = self.storage.import_child_transition(
            &mut self.tree,
            parent,
            verified,
            proof,
            &self.journal_log_id,
        );
        self.execution_result(verified.block_cid(), result, now)
    }
    pub fn body_arrived(&mut self, cid: &str, now: i64) -> ChainEffects {
        self.bodies.body_fetched(cid);
        self.finish(SyncTurn::default(), now)
    }
    pub fn body_unavailable(&mut self, cid: &str, now: i64) -> ChainEffects {
        self.bodies.park_unavailable(cid, now, 1000, 60000);
        self.finish(SyncTurn::default(), now)
    }
    /// Re-derive work from the current parent and wake only genuinely satisfied
    /// requirements, including executed side branches. No new durable fact is
    /// invented: these weights are rebuilt from parent journals on recovery.
    pub fn parent_advanced(
        &mut self,
        parent: &ChainTree,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let context = self.tree.context().unwrap();
        if parent
            .context()
            .is_none_or(|p| p.path() != &context.path()[..context.path().len() - 1])
        {
            return Err(StoreError::InvalidConfiguration(
                "wrong hosted parent".into(),
            ));
        }
        let directory = context.path().last().unwrap().clone();
        let mut next = self.tree.clone();
        next.apply_parent_run(parent, &directory, None, &BTreeSet::new())?;
        let answered: Vec<_> = self
            .bodies
            .awaiting_parent
            .iter()
            .filter_map(|(cid, need)| match need {
                CrossChainEvidenceRequirement::ParentStateContinuity(link)
                    if parent.has_parent_continuity(link) =>
                {
                    Some(cid.clone())
                }
                _ => None,
            })
            .collect();
        self.tree = next;
        self.schedule_derivations(parent, &[]);
        self.bodies.parent_facts_present(&answered);
        Ok(self.finish(SyncTurn::default(), now))
    }
}

/// Registry order is parent before child. Unhosting drops runtime state only;
/// durable facts, proofs and cursors survive and are restored when rehosted.
#[derive(Default)]
pub struct HostedLevels {
    children: BTreeMap<Vec<String>, ChildChainCore>,
}
impl HostedLevels {
    pub fn ordered(&self) -> Vec<Vec<String>> {
        let mut paths: Vec<_> = self.children.keys().cloned().collect();
        paths.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        paths
    }
    pub fn get(&self, path: &[String]) -> Option<&ChildChainCore> {
        self.children.get(path)
    }
    pub fn get_mut(&mut self, path: &[String]) -> Option<&mut ChildChainCore> {
        self.children.get_mut(path)
    }
    pub fn host(
        &mut self,
        root: &mut RootChainCore,
        path: Vec<String>,
        specs: Vec<ChainSpec>,
        config: StreamConfig,
        body_window: usize,
    ) -> Result<bool, StoreError> {
        let context = ChainRuntimeContext::new(path.clone(), None)
            .map_err(|e| StoreError::InvalidConfiguration(e.to_string()))?;
        if context.is_root() {
            return Err(StoreError::InvalidConfiguration(
                "cannot host root as child".into(),
            ));
        }
        if self.children.contains_key(&path) {
            return Ok(false);
        }
        let parent_path = &path[..path.len() - 1];
        let parent = if parent_path == ["Nexus"] {
            root.tree()
        } else {
            self.children
                .get(parent_path)
                .ok_or_else(|| {
                    StoreError::InvalidConfiguration("parent level is not hosted".into())
                })?
                .tree()
        };
        // Prepare a parent clone first: a failed child restore does not mutate
        // the live registry or its parent's derived run configuration.
        let mut parent = parent.clone();
        parent.serve_runs(path.last().unwrap());
        let core = ChildChainCore::open(
            root.storage.clone(),
            context,
            &parent,
            specs,
            &root.sync.log.id,
            config,
            body_window,
        )?;
        if parent_path == ["Nexus"] {
            root.tree = parent;
        } else {
            self.children.get_mut(parent_path).unwrap().tree = parent;
        }
        self.children.insert(path, core);
        Ok(true)
    }
    pub fn unhost(&mut self, path: &[String]) -> Vec<Vec<String>> {
        if path.len() < 2 {
            return vec![];
        }
        let removed: Vec<_> = self
            .ordered()
            .into_iter()
            .filter(|p| p.starts_with(path))
            .collect();
        for p in &removed {
            self.children.remove(p);
        }
        removed
    }
    pub fn parent_advanced(
        &mut self,
        root: &RootChainCore,
        now: i64,
    ) -> Result<Vec<(Vec<String>, ChainEffects)>, StoreError> {
        let mut effects = vec![];
        for path in self.ordered() {
            let parent_path = &path[..path.len() - 1];
            let parent = if parent_path == ["Nexus"] {
                root.tree().clone()
            } else {
                self.children[parent_path].tree.clone()
            };
            let turn = self
                .children
                .get_mut(&path)
                .unwrap()
                .parent_advanced(&parent, now)?;
            if !turn.sync.is_empty() || !turn.bodies.is_empty() || !turn.headers.is_empty() {
                effects.push((path, turn));
            }
        }
        Ok(effects)
    }
}

impl ChildChainCore {
    /// Synchronous worker adapter over the current executed parent graph. The
    /// returned effects remain behind the same durable barrier as root work.
    pub fn run_connection(
        &mut self,
        job: &lattice_block_tree::ConnectJob,
        parent: &ChainTree,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        use lattice_block_tree::ChildConnectionExecution;
        self.connecting(job.block_hash())?;
        let path = self.tree.context().unwrap().path().to_vec();
        let mut saved = self
            .storage
            .journal()
            .saved_proofs(&path, &[job.block_hash().into()])?;
        let proof = saved
            .remove(job.block_hash())
            .unwrap_or_default()
            .into_iter()
            .find(|p| p.root_cid == job.contribution().id());
        let Some(proof) = proof else {
            self.bodies.await_parent(
                job.block_hash().into(),
                CrossChainEvidenceRequirement::ChildProof {
                    chain_path: path,
                    child_cid: job.block_hash().into(),
                },
            );
            self.proofs.require_proof(job.block_hash().into());
            self.evidence_changed(&[job.block_hash().into()]);
            return Ok(self.finish(SyncTurn::default(), now));
        };
        match self
            .tree
            .execute_child_connection(job, parent, &proof, self.storage.as_ref(), now)?
        {
            ChildConnectionExecution::Genesis(verified) => {
                self.connected_genesis(parent, &verified, &proof, now)
            }
            ChildConnectionExecution::Transition(verified) => {
                self.connected_transition(parent, &verified, &proof, now)
            }
            ChildConnectionExecution::Unavailable => {
                Ok(self.body_unavailable(job.block_hash(), now))
            }
            ChildConnectionExecution::AwaitingParent(link) => {
                self.bodies.await_parent(
                    job.block_hash().into(),
                    CrossChainEvidenceRequirement::ParentStateContinuity(link),
                );
                Ok(self.finish(SyncTurn::default(), now))
            }
            ChildConnectionExecution::Rejected(rejection) => {
                let cid = rejection.block_hash().to_owned();
                let update = self.storage.import_child_rejection(
                    &mut self.tree,
                    rejection,
                    &self.journal_log_id,
                )?;
                self.bodies.completed(&cid);
                self.published(HeaderImport::Applied(update), now)
            }
        }
    }
}
