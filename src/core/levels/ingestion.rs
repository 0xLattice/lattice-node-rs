use super::*;
use crate::core::{child_proofs::*, headers::HeaderEffect, pending::PendingHeader};
use lattice_block_tree::AdmissionError;
use lattice_cashew::cbor;
use lattice_primitives::content_id;
fn append(to: &mut ChainEffects, from: ChainEffects) {
    to.sync.extend(from.sync);
    to.bodies.extend(from.bodies);
    to.headers.extend(from.headers);
}
impl ChildChainCore {
    pub fn received_headers(
        &mut self,
        peer: &PeerId,
        response: HeadersResponse,
        parent: Option<&ChainTree>,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let mut next = self.sync.clone();
        let Some(mut work) = next.begin_headers_response(peer, response) else {
            return Ok(ChainEffects::default());
        };
        let mut received = BTreeSet::new();
        for entry in std::mem::take(&mut work.entries) {
            let cid = content_id(&entry.block)?;
            self.queue_header(entry, Some(peer.clone()), parent, now)?;
            received.insert(cid);
        }
        if let Some(asked) = &work.asked_parent
            && !received.contains(asked)
        {
            for cid in self.pending.children_of(asked) {
                let header = self.pending.metadata_mut(&cid).unwrap();
                if header.source() == Some(peer) {
                    header.announcers.remove(0);
                    header.asked_parent = false;
                }
            }
        }
        let log = next.log.clone();
        let mut turn = next.finish_headers_response(peer, work, &received, now, |e| {
            !log.contains(e) && !self.finality_done(e)
        });
        for peer in next.peers.keys().cloned().collect::<Vec<_>>() {
            next.advance_cursor(
                &peer,
                |e| log.contains(e) || self.finality_done(e),
                &mut turn,
            );
        }
        self.persist_cursors(&turn.cursors)?;
        self.sync = next;
        let mut effects = self.finish(turn, now);
        append(&mut effects, self.drain_proven_headers(now)?);
        Ok(effects)
    }
    fn past_finality(&self, height: u64) -> bool {
        super::super::finality::past(&self.tree, height, self.header_config.finality)
    }
    /// Header receipt only queues bounded proof checks. No peer claim advances
    /// the weigh log before the proof result crosses actual admission/storage.
    pub fn queue_header(
        &mut self,
        entry: HeaderEntry,
        peer: Option<PeerId>,
        parent: Option<&ChainTree>,
        now: i64,
    ) -> Result<bool, StoreError> {
        let cid = content_id(&entry.block)?;
        let accepted = self.queue_header_inner(entry, peer, parent, now)?;
        if accepted && let Some(parent) = parent {
            self.schedule_derivations(parent, &[cid]);
        }
        Ok(accepted)
    }
    fn queue_header_inner(
        &mut self,
        mut entry: HeaderEntry,
        peer: Option<PeerId>,
        parent: Option<&ChainTree>,
        now: i64,
    ) -> Result<bool, StoreError> {
        let cid = content_id(&entry.block)?;
        if self.past_finality(entry.block.height)
            || entry
                .block
                .parent
                .as_ref()
                .is_some_and(|p| self.declined.contains(&p.0.raw_cid))
        {
            self.declined.insert(cid);
            return Ok(false);
        }
        if self.tree.contains(&cid) {
            self.proofs.offer(
                entry.proofs,
                &cid,
                peer,
                &self.proof_config,
                &self.pending,
                &self.tree,
            );
            return Ok(true);
        }
        if let Some(children) = &entry.children {
            if cbor::encode(children)?.len() > self.header_config.max_child_index_bytes {
                entry.children = None;
            } else if content_id(children)? != entry.block.children.raw_cid {
                return Ok(false);
            }
        }
        if now
            .checked_add(self.header_config.max_future_drift)
            .is_some_and(|h| entry.block.timestamp > h)
        {
            return Ok(false);
        }
        if self.pending.entries().contains_key(&cid) {
            if let Some(children) = entry.children {
                let bytes = cbor::encode(&children)?.len();
                self.pending
                    .set_children(&cid, children, bytes)
                    .map_err(|e| StoreError::Corrupt(e.into()))?;
            }
            let held = self.pending.metadata_mut(&cid).unwrap();
            if let Some(peer) = &peer
                && !held.announcers.contains(peer)
            {
                held.announcers.push(peer.clone());
            }
            if held.spec.is_none()
                && entry.block.parent.is_none()
                && entry.spec.as_ref().is_some_and(|s| {
                    content_id(s).ok().as_deref() == Some(entry.block.spec.0.raw_cid.as_str())
                })
            {
                held.spec = entry.spec;
            }
            self.proofs.offer(
                entry.proofs,
                &cid,
                peer,
                &self.proof_config,
                &self.pending,
                &self.tree,
            );
            return Ok(true);
        }
        Ok(self.proofs.await_header(
            entry,
            peer,
            &self.proof_config,
            &self.pending,
            &self.tree,
            parent,
        ))
    }
    pub fn proof_work(&mut self) -> ProofWork {
        self.proofs.work(
            self.tree.context().unwrap().path(),
            &self.proof_config,
            &self.pending,
            &self.tree,
            &[],
        )
    }
    pub fn proofs_found(&mut self, cid: &str, proofs: Vec<ChildBlockProof>) {
        if self.proofs.awaiting.contains_key(cid)
            || self.pending.entries().contains_key(cid)
            || self.tree.contains(cid)
        {
            self.proofs.offer(
                proofs,
                cid,
                None,
                &self.proof_config,
                &self.pending,
                &self.tree,
            );
        }
    }
    pub fn evidence_changed(&mut self, cids: &[String]) {
        self.proofs
            .evidence_changed(cids, &self.proof_config, &self.pending, &self.tree);
    }
    pub fn proof_completed(
        &mut self,
        completion: ChildProofCompletion,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        // Missing local content is not evidence against the sender. Remove the
        // in-flight check, then schedule the same index retry as Swift skipped().
        if completion.missing_block().is_some() {
            self.proofs
                .drop_missing(completion, &self.pending, &self.tree);
            return Ok(ChainEffects::default());
        }
        let checked = self.proofs.completed(completion);
        let Some(checked) = checked else {
            return Ok(ChainEffects::default());
        };
        let mut effects = self.apply_checked_proof(checked, now)?;
        append(&mut effects, self.advance_finality_cursors(now)?);
        Ok(effects)
    }
    pub(super) fn apply_checked_proof(
        &mut self,
        checked: CheckedProof,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        match checked {
            CheckedProof::Rejected { source, .. } => {
                let mut effects = ChainEffects::default();
                if let Some(peer) = source {
                    self.peer_closed(&peer);
                    effects
                        .headers
                        .push(HeaderEffect::DisconnectInvalidProof(peer));
                }
                Ok(effects)
            }
            CheckedProof::Accepted {
                child_cid,
                proof,
                evidence,
            } => {
                if self.tree.contains(&child_cid) {
                    let Some((block, children)) = self.storage.journal().header(&child_cid)? else {
                        return Ok(ChainEffects::default());
                    };
                    if self.past_finality(block.height) {
                        return Ok(ChainEffects::default());
                    }
                    let spec = self.tree.held_spec(&block.spec.0.raw_cid).cloned();
                    let mut turn =
                        self.admit_header(&block, &children, spec.as_ref(), &proof, now)?;
                    self.proofs.proof_repaired(&child_cid);
                    if matches!(
                        self.bodies.awaiting_parent.get(&child_cid),
                        Some(CrossChainEvidenceRequirement::ChildProof { .. })
                    ) {
                        self.bodies
                            .parent_facts_present(std::slice::from_ref(&child_cid));
                        turn.bodies
                            .extend(self.bodies.schedule(&self.tree, self.body_window, now));
                    }
                    return Ok(turn);
                }
                let mut header = if let Some(held) = self.pending.entries().get(&child_cid) {
                    if held.evidence.contains_key(evidence.grind_id()) {
                        return Ok(ChainEffects::default());
                    }
                    held.clone()
                } else if let Some(waiting) = self.proofs.awaiting.remove(&child_cid) {
                    let bytes = cbor::encode(&waiting.entry.block)?.len()
                        + waiting
                            .entry
                            .children
                            .as_ref()
                            .map(cbor::encode)
                            .transpose()?
                            .map_or(0, |b| b.len());
                    PendingHeader {
                        block_cid: child_cid.clone(),
                        block: waiting.entry.block,
                        children: waiting.entry.children,
                        spec: waiting.entry.spec,
                        hash: evidence.root_hash(),
                        bytes,
                        announcers: waiting.announcers,
                        asked_parent: false,
                        not_before: None,
                        evidence: BTreeMap::new(),
                        proofs: BTreeMap::new(),
                    }
                } else {
                    return Ok(ChainEffects::default());
                };
                header.bytes = header
                    .bytes
                    .checked_add(proof.serialize()?.len())
                    .ok_or_else(|| StoreError::Corrupt("proof byte overflow".into()))?;
                header.hash = header.hash.min(evidence.root_hash());
                header.proofs.insert(evidence.grind_id().into(), proof);
                header
                    .evidence
                    .insert(evidence.grind_id().into(), *evidence);
                self.pending.remove(&child_cid);
                self.pending
                    .insert(header)
                    .map_err(|e| StoreError::Corrupt(e.into()))?;
                self.drain_proven_headers(now)
            }
        }
    }
    pub fn drain_proven_headers(&mut self, now: i64) -> Result<ChainEffects, StoreError> {
        let mut effects = ChainEffects::default();
        loop {
            let mut changed = false;
            let mut order: Vec<_> = self
                .pending
                .priorities()
                .iter()
                .map(|(c, p)| (*p, c.clone()))
                .collect();
            order.sort();
            for (_, cid) in order {
                let Some(header) = self.pending.entries().get(&cid).cloned() else {
                    continue;
                };
                if header.not_before.is_some_and(|t| t > now) {
                    continue;
                }
                if self.past_finality(header.block.height) {
                    self.declined.insert(cid.clone());
                    self.pending.remove(&cid);
                    changed = true;
                    continue;
                }
                if let Some(parent) = header.parent()
                    && !self.tree.contains(parent)
                {
                    if !self.pending.entries().contains_key(parent)
                        && !header.asked_parent
                        && let Some(peer) =
                            header.source().filter(|p| self.sync.peers.contains_key(*p))
                    {
                        let turn = self.sync.request_ancestors(peer, parent.into(), now);
                        if !turn.effects.is_empty() {
                            self.pending.metadata_mut(&cid).unwrap().asked_parent = true;
                            effects.sync.extend(turn.effects);
                        }
                    }
                    continue;
                }
                if header.children.is_none() && header.parent().is_some() {
                    match self.tree.header_linkage(&header.block, now) {
                        Ok(
                            lattice_validation::HeaderAdmission::Linked
                            | lattice_validation::HeaderAdmission::Excluded,
                        )
                        | Err(lattice_block_tree::TreeHeaderDeferred::SpecUnavailable) => {}
                        Ok(lattice_validation::HeaderAdmission::OffSchedule) => {
                            if let Some(peer) = header.source() {
                                self.peer_closed(peer);
                                effects
                                    .headers
                                    .push(HeaderEffect::DisconnectInvalidProof(peer.clone()));
                            }
                            self.pending.remove(&cid);
                            changed = true;
                            continue;
                        }
                        Err(lattice_block_tree::TreeHeaderDeferred::Schedule(
                            lattice_validation::HeaderDeferred::NotYetValid,
                        )) => {
                            self.pending.metadata_mut(&cid).unwrap().not_before =
                                Some(header.block.timestamp);
                            continue;
                        }
                        _ => {
                            self.pending.remove(&cid);
                            changed = true;
                            continue;
                        }
                    }
                }
                let Some(children) = &header.children else {
                    if let Some(peer) = header.source().filter(|p| {
                        self.sync
                            .peers
                            .get(*p)
                            .is_some_and(|s| s.child_index.is_none())
                    }) {
                        self.sync.peers.get_mut(peer).unwrap().child_index = Some(InFlightFetch {
                            cid: header.block.children.raw_cid.clone(),
                            deadline: now.saturating_add(self.sync.config.timeout),
                        });
                        effects.headers.push(HeaderEffect::FetchChildIndex {
                            peer: peer.clone(),
                            cid: header.block.children.raw_cid.clone(),
                        });
                    }
                    continue;
                };
                if header.block.parent.is_none() && header.spec.is_none() {
                    continue;
                }
                let mut proofs: Vec<_> = header.evidence.iter().collect();
                proofs.sort_by(|(a, x), (b, y)| {
                    x.root_hash().cmp(&y.root_hash()).then_with(|| a.cmp(b))
                });
                let mut complete = true;
                for (id, _) in proofs {
                    match self.admit_header(
                        &header.block,
                        children,
                        header.spec.as_ref(),
                        &header.proofs[id],
                        now,
                    ) {
                        Ok(turn) => append(&mut effects, turn),
                        Err(StoreError::Admission(AdmissionError::NotYetValid)) => {
                            self.pending.metadata_mut(&cid).unwrap().not_before =
                                Some(header.block.timestamp);
                            complete = false;
                            break;
                        }
                        Err(StoreError::Admission(AdmissionError::ProofOfWorkInvalid)) => {
                            if let Some(peer) = header.source() {
                                self.peer_closed(peer);
                                effects
                                    .headers
                                    .push(HeaderEffect::DisconnectInvalidProof(peer.clone()));
                            }
                            break;
                        }
                        Err(StoreError::Admission(
                            AdmissionError::ProtocolInvalid
                            | AdmissionError::NotAcceptedAtCurrentChain
                            | AdmissionError::ConflictingWorkLocation,
                        )) => break,
                        Err(error) => return Err(error),
                    }
                }
                if complete {
                    self.pending.remove(&cid);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        self.pending.evict(self.header_config.pending_bytes);
        self.pending.compact();
        append(&mut effects, self.advance_finality_cursors(now)?);
        Ok(effects)
    }
    pub fn child_index_fetched(
        &mut self,
        peer: &PeerId,
        cid: &str,
        bytes: Option<&[u8]>,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let Some(state) = self
            .sync
            .peers
            .get_mut(peer)
            .filter(|p| p.child_index.as_ref().is_some_and(|f| f.cid == cid))
        else {
            return Ok(ChainEffects::default());
        };
        state.child_index = None;
        let children = bytes
            .filter(|b| b.len() <= self.header_config.max_child_index_bytes)
            .filter(|b| lattice_cashew::validate_entry(cid, b).is_ok())
            .and_then(|b| cbor::decode::<FlatDictionary<BlockHeader>>(b).ok());
        let waiting: Vec<_> = self
            .pending
            .committing(cid)
            .iter()
            .map(|h| h.block_cid.clone())
            .collect();
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
        self.drain_proven_headers(now)
    }
}
