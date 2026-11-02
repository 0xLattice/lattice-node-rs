//! Root header ingestion and bounded pending-evidence scheduling. Runs on a
//! cloned turn; its authenticated fact batches must commit before publication.
use super::{pending::*, stream::*, sync::*};
use crate::storage::{StoreError, StoredHeader};
use lattice_block_tree::{
    AdmissionError, BlockImportBatch, ChainTree, HeaderPreparation, TreeHeaderDeferred,
};
use lattice_cashew::{FlatDictionary, cbor};
use lattice_primitives::{UInt256, content_id};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone)]
pub struct HeaderConfig {
    pub max_child_index_bytes: usize,
    pub pending_bytes: usize,
    pub max_future_drift: i64,
    pub finality: Option<u64>,
}
impl Default for HeaderConfig {
    fn default() -> Self {
        Self {
            max_child_index_bytes: 1 << 20,
            pending_bytes: 16 << 20,
            max_future_drift: 7_200_000,
            finality: None,
        }
    }
}
#[derive(Debug, Clone)]
pub enum HeaderEffect {
    FetchChildIndex { peer: PeerId, cid: String },
    DisconnectInvalidProof(PeerId),
}
#[derive(Debug, Clone, Default)]
pub struct RootHeaderPool {
    pub pending: PendingQueue,
    pub declined: DeclinedSet,
    pub config: HeaderConfig,
    finality_height: u64,
}
#[derive(Debug, Default)]
pub struct HeaderTurn {
    pub facts: Vec<BlockImportBatch>,
    pub headers: Vec<StoredHeader>,
    pub effects: Vec<HeaderEffect>,
    pub sync: SyncTurn,
}
impl RootHeaderPool {
    fn past_finality(&self, tree: &ChainTree, height: u64) -> bool {
        super::finality::past(tree, height, self.config.finality)
    }
    pub(crate) fn refresh_finality(&mut self, tree: &ChainTree) {
        let height = super::finality::height(tree);
        if height < self.finality_height {
            self.declined = DeclinedSet::default();
        }
        self.finality_height = height;
    }
    pub fn drop_peer(&mut self, peer: &PeerId) {
        for cid in self.pending.entries().keys().cloned().collect::<Vec<_>>() {
            let h = self.pending.metadata_mut(&cid).unwrap();
            let first = h.source() == Some(peer);
            h.announcers.retain(|p| p != peer);
            if first {
                h.asked_parent = false;
            }
        }
    }
    fn announce(&mut self, cid: &str, peer: &PeerId) {
        let mut at = Some(cid.to_owned());
        let mut seen = BTreeSet::new();
        while let Some(cid) = at {
            if !seen.insert(cid.clone()) {
                break;
            }
            let Some(h) = self.pending.metadata_mut(&cid) else {
                break;
            };
            if h.announcers.contains(peer) {
                break;
            }
            if h.announcers.is_empty() {
                h.asked_parent = false;
            }
            h.announcers.push(peer.clone());
            at = h.parent().map(String::from);
        }
    }
    fn disconnect(&mut self, peer: &PeerId, sync: &mut HeaderSync, turn: &mut HeaderTurn) {
        sync.drop_peer(peer);
        self.drop_peer(peer);
        turn.effects
            .push(HeaderEffect::DisconnectInvalidProof(peer.clone()));
    }
    pub fn accept(
        &mut self,
        mut entry: HeaderEntry,
        peer: &PeerId,
        tree: &ChainTree,
        sync: &mut HeaderSync,
        now: i64,
        turn: &mut HeaderTurn,
    ) -> Result<Option<String>, StoreError> {
        let hash = match content_id(&entry.block) {
            Ok(h) => h,
            Err(_) => {
                self.disconnect(peer, sync, turn);
                return Ok(None);
            }
        };
        if tree.contains(&hash) || self.declined.contains(&hash) {
            return Ok(Some(hash));
        }
        if !entry.block.has_valid_pow() {
            self.disconnect(peer, sync, turn);
            return Ok(None);
        }
        let mut index_size = 0;
        if let Some(children) = &entry.children {
            index_size = cbor::encode(children)?.len();
            if index_size > self.config.max_child_index_bytes {
                entry.children = None;
                index_size = 0;
            } else if content_id(children)? != entry.block.children.raw_cid {
                return Ok(Some(hash));
            }
        }
        if entry.block.parent.is_none() {
            return Ok(Some(hash));
        }
        if now
            .checked_add(self.config.max_future_drift)
            .is_some_and(|h| entry.block.timestamp > h)
        {
            return Ok(Some(hash));
        }
        if self.past_finality(tree, entry.block.height)
            || entry
                .block
                .parent
                .as_ref()
                .is_some_and(|p| self.declined.contains(&p.0.raw_cid))
        {
            self.declined.insert(hash.clone());
            return Ok(Some(hash));
        }
        if self.pending.entries().contains_key(&hash) {
            self.announce(&hash, peer);
            if let Some(children) = entry.children {
                self.pending
                    .set_children(&hash, children, index_size)
                    .map_err(|e| StoreError::Corrupt(e.into()))?;
            }
            return Ok(Some(hash));
        }
        let mut announcers = vec![peer.clone()];
        for child in self.pending.children_of(&hash) {
            for other in &self.pending.entries()[&child].announcers {
                if !announcers.contains(other) {
                    announcers.push(other.clone());
                }
            }
        }
        let bytes = cbor::encode(&entry.block)?
            .len()
            .checked_add(index_size)
            .ok_or_else(|| StoreError::Corrupt("header size overflow".into()))?;
        let parent = entry.block.parent.as_ref().unwrap().0.raw_cid.clone();
        let work = UInt256(entry.block.pow_hash());
        self.pending
            .insert(PendingHeader {
                block_cid: hash.clone(),
                block: entry.block,
                children: entry.children,
                spec: None,
                hash: work,
                bytes,
                announcers,
                asked_parent: false,
                not_before: None,
                evidence: BTreeMap::new(),
                proofs: BTreeMap::new(),
            })
            .map_err(|e| StoreError::Corrupt(e.into()))?;
        self.announce(&parent, peer);
        Ok(Some(hash))
    }
    pub fn missing_ancestor_response(&mut self, asked: &str, peer: &PeerId) {
        for cid in self.pending.children_of(asked) {
            let h = self.pending.metadata_mut(&cid).unwrap();
            if h.source() == Some(peer) {
                h.announcers.remove(0);
                h.asked_parent = false;
            }
        }
    }
    pub fn drain(
        &mut self,
        tree: &mut ChainTree,
        sync: &mut HeaderSync,
        now: i64,
        turn: &mut HeaderTurn,
    ) -> Result<(), StoreError> {
        loop {
            let mut changed = false;
            let mut ordered = self
                .pending
                .priorities()
                .iter()
                .map(|(h, p)| (*p, h.clone()))
                .collect::<Vec<_>>();
            ordered.sort();
            for (_, cid) in ordered {
                let Some(header) = self.pending.entries().get(&cid).cloned() else {
                    continue;
                };
                if header.not_before.is_some_and(|t| t > now) {
                    continue;
                }
                let Some(parent) = header.parent() else {
                    self.pending.remove(&cid);
                    changed = true;
                    continue;
                };
                if !tree.contains(parent) {
                    if !self.pending.entries().contains_key(parent)
                        && !header.asked_parent
                        && let Some(peer) = header.source().filter(|p| sync.peers.contains_key(*p))
                    {
                        let request = sync.request_ancestors(peer, parent.into(), now);
                        if !request.effects.is_empty() {
                            self.pending.metadata_mut(&cid).unwrap().asked_parent = true;
                            turn.sync.effects.extend(request.effects);
                        }
                    }
                    continue;
                }
                if self.past_finality(tree, header.block.height) {
                    self.pending.remove(&cid);
                    self.declined.insert(cid);
                    changed = true;
                    continue;
                }
                let Some(children) = &header.children else {
                    let linked = match tree.header_linkage(&header.block, now) {
                        Ok(
                            lattice_validation::HeaderAdmission::Linked
                            | lattice_validation::HeaderAdmission::Excluded,
                        ) => true,
                        Ok(lattice_validation::HeaderAdmission::OffSchedule) => {
                            if let Some(peer) = header.source() {
                                self.disconnect(peer, sync, turn);
                            }
                            self.pending.remove(&cid);
                            changed = true;
                            false
                        }
                        Err(TreeHeaderDeferred::Schedule(
                            lattice_validation::HeaderDeferred::NotYetValid,
                        )) => {
                            self.pending.metadata_mut(&cid).unwrap().not_before =
                                Some(header.block.timestamp);
                            false
                        }
                        Err(TreeHeaderDeferred::SpecUnavailable) => true,
                        _ => {
                            self.pending.remove(&cid);
                            changed = true;
                            false
                        }
                    };
                    if linked
                        && let Some(peer) = header
                            .source()
                            .filter(|p| sync.peers.get(*p).is_some_and(|s| s.child_index.is_none()))
                    {
                        sync.peers.get_mut(peer).unwrap().child_index = Some(InFlightFetch {
                            cid: header.block.children.raw_cid.clone(),
                            deadline: now.saturating_add(sync.config.timeout),
                        });
                        turn.effects.push(HeaderEffect::FetchChildIndex {
                            peer: peer.clone(),
                            cid: header.block.children.raw_cid.clone(),
                        });
                    }
                    continue;
                };
                match tree.prepare_root_header(&header.block, children, now) {
                    Ok(HeaderPreparation::Prepared(prepared)) => {
                        turn.headers
                            .push(StoredHeader::from_nodes(&header.block, children)?);
                        turn.facts.extend(prepared.batches().iter().cloned());
                        tree.apply_prepared(*prepared)?;
                        self.pending.remove(&cid);
                        changed = true;
                    }
                    Ok(HeaderPreparation::Duplicate) => {
                        self.pending.remove(&cid);
                        changed = true;
                    }
                    Err(AdmissionError::NotYetValid) => {
                        self.pending.metadata_mut(&cid).unwrap().not_before =
                            Some(header.block.timestamp)
                    }
                    Err(AdmissionError::ProofOfWorkInvalid) => {
                        self.pending.remove(&cid);
                        changed = true;
                        if let Some(peer) = header.source() {
                            self.disconnect(peer, sync, turn);
                        }
                    }
                    Err(
                        AdmissionError::RevisionExhausted
                        | AdmissionError::LocalVerificationFailure
                        | AdmissionError::StalePreparation,
                    ) => {
                        return Err(StoreError::Corrupt(
                            "pending header local admission failure".into(),
                        ));
                    }
                    Err(_) => {
                        self.pending.remove(&cid);
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        self.pending.evict(self.config.pending_bytes);
        self.pending.compact();
        Ok(())
    }
    pub fn child_index_response(
        &mut self,
        sync: &mut HeaderSync,
        peer: &PeerId,
        cid: &str,
        bytes: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        let Some(state) = sync
            .peers
            .get_mut(peer)
            .filter(|s| s.child_index.as_ref().is_some_and(|f| f.cid == cid))
        else {
            return Ok(());
        };
        state.child_index = None;
        let children = bytes
            .filter(|b| b.len() <= self.config.max_child_index_bytes)
            .and_then(|b| {
                lattice_cashew::validate_entry(cid, b).ok().and_then(|_| {
                    cbor::decode::<FlatDictionary<lattice_primitives::BlockHeader>>(b).ok()
                })
            });
        let waiting = self
            .pending
            .committing(cid)
            .iter()
            .map(|h| h.block_cid.clone())
            .collect::<Vec<_>>();
        for hash in waiting {
            if let Some(children) = &children {
                self.pending
                    .set_children(&hash, children.clone(), bytes.unwrap().len())
                    .map_err(|e| StoreError::Corrupt(e.into()))?;
            } else {
                let h = self.pending.metadata_mut(&hash).unwrap();
                if h.source() == Some(peer) {
                    h.announcers.remove(0);
                    h.asked_parent = false;
                }
            }
        }
        Ok(())
    }
}
