//! Bounded child proof scheduling from ChildProofs.swift. Proof completion is a
//! private result of the real verifier, not a caller-supplied validity boolean.
use super::{
    pending::PendingQueue,
    sync::{HeaderEntry, PeerId},
};
use lattice_block_tree::{ChainTree, ChildBlockProof, ChildProofFailure, VerifiedChildEvidence};
use lattice_primitives::{Block, ChainSpec, content_id};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone)]
pub struct ChildProofConfig {
    pub max_checks: usize,
    pub index_checks: usize,
    pub max_per_header: usize,
    pub max_proof_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_awaiting: usize,
    pub max_wanted: usize,
}
impl Default for ChildProofConfig {
    fn default() -> Self {
        Self {
            max_checks: 256,
            index_checks: 32,
            max_per_header: 8,
            max_proof_bytes: 64 * 1024,
            max_queued_bytes: 16 * 1024 * 1024,
            max_awaiting: 1024,
            max_wanted: 4096,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChildProofKey {
    pub child_cid: String,
    pub proof_id: String,
}
#[derive(Debug, Clone)]
struct QueuedProof {
    key: ChildProofKey,
    proof: ChildBlockProof,
    source: Option<PeerId>,
    bytes: usize,
}
#[derive(Debug, Clone)]
pub struct AwaitingChildProof {
    pub entry: HeaderEntry,
    pub announcers: Vec<PeerId>,
    pub looked_up: bool,
    sequence: u64,
}
#[derive(Debug, Clone)]
pub struct ChildProofJob {
    key: ChildProofKey,
    proof: ChildBlockProof,
    path: Vec<String>,
    block: Option<Block>,
}
impl ChildProofJob {
    pub fn key(&self) -> &ChildProofKey {
        &self.key
    }
    pub fn block(&self) -> Option<&Block> {
        self.block.as_ref()
    }
    pub fn proof(&self) -> &ChildBlockProof {
        &self.proof
    }
    /// A stored header may be supplied for an already weighed block. Its CID
    /// must match this job before the expensive proof path is walked.
    pub fn run(self, stored: Option<&Block>) -> ChildProofCompletion {
        let result = self.block.as_ref().or(stored).map(|block| {
            if content_id(block).ok().as_deref() != Some(self.key.child_cid.as_str()) {
                return Err(ChildProofFailure::MalformedEvidence);
            }
            let root = self
                .proof
                .entries
                .iter()
                .find(|e| e.0 == self.proof.root_cid)
                .and_then(|e| lattice_cashew::cbor::decode::<Block>(&e.1).ok())
                .ok_or(ChildProofFailure::MalformedEvidence)?;
            if root.pow_hash() > block.target.0 {
                return Err(ChildProofFailure::MalformedEvidence);
            }
            self.proof.verify_securing_work(block, &self.path)
        });
        ChildProofCompletion {
            key: self.key,
            result,
        }
    }
}
#[derive(Debug)]
pub struct ChildProofCompletion {
    key: ChildProofKey,
    result: Option<Result<VerifiedChildEvidence, ChildProofFailure>>,
}
impl ChildProofCompletion {
    pub(crate) fn missing_block(&self) -> Option<&str> {
        self.result.is_none().then_some(self.key.child_cid.as_str())
    }
}
#[derive(Debug)]
pub enum CheckedProof {
    Accepted {
        child_cid: String,
        proof: ChildBlockProof,
        evidence: Box<VerifiedChildEvidence>,
    },
    Rejected {
        child_cid: String,
        source: Option<PeerId>,
    },
}
#[derive(Debug, Default)]
pub struct ProofWork {
    pub checks: Vec<ChildProofJob>,
    pub lookup: Vec<String>,
}
#[derive(Debug, Clone, Default)]
pub struct ChildProofSync {
    pub awaiting: BTreeMap<String, AwaitingChildProof>,
    queued: VecDeque<QueuedProof>,
    verifying: BTreeMap<ChildProofKey, QueuedProof>,
    required: BTreeSet<String>,
    relookup: BTreeSet<String>,
    wanted: BTreeSet<String>,
    wanted_order: VecDeque<String>,
    deriving: BTreeMap<String, usize>,
    sequence: u64,
}
fn credits(tree: &ChainTree, pending: &PendingQueue, root: &str, cid: &str) -> bool {
    pending
        .entries()
        .get(cid)
        .is_some_and(|h| h.evidence.contains_key(root))
        || tree
            .consensus_block(cid)
            .is_some_and(|m| m.work_contributions.contains_key(root))
}
fn bound(spec: Option<ChainSpec>, block: &Block) -> Option<ChainSpec> {
    spec.filter(|s| {
        block.parent.is_none()
            && content_id(s).ok().as_deref() == Some(block.spec.0.raw_cid.as_str())
    })
}
impl ChildProofSync {
    /// A graph contribution whose persisted proof bytes need repair is still
    /// checked, even though its work was already credited.
    pub fn proof_repaired(&mut self, cid: &str) {
        self.required.remove(cid);
    }
    pub fn require_proof(&mut self, cid: String) {
        self.required.insert(cid.clone());
        self.relookup.insert(cid);
    }
    pub fn queued_bytes(&self) -> usize {
        self.queued
            .iter()
            .chain(self.verifying.values())
            .map(|p| p.bytes)
            .sum()
    }
    pub fn checks(&self, index: bool) -> usize {
        self.verifying
            .values()
            .filter(|p| p.source.is_none() == index)
            .count()
    }
    pub fn holds(&self, cid: &str) -> bool {
        self.queued.iter().any(|p| p.key.child_cid == cid)
            || self.verifying.keys().any(|p| p.child_cid == cid)
            || self.deriving.contains_key(cid)
    }
    pub fn start_deriving(&mut self, cid: String) {
        let count = self.deriving.entry(cid).or_default();
        *count = count.saturating_add(1);
    }
    pub fn finish_deriving(&mut self, cid: &str) {
        if let Some(count) = self.deriving.get_mut(cid) {
            if *count <= 1 {
                self.deriving.remove(cid);
            } else {
                *count -= 1;
            }
        }
    }
    pub fn drop_peer(&mut self, peer: &PeerId) {
        for waiting in self.awaiting.values_mut() {
            waiting.announcers.retain(|p| p != peer);
        }
        self.queued.retain(|p| p.source.as_ref() != Some(peer));
        self.verifying
            .retain(|_, p| p.source.as_ref() != Some(peer));
    }
    fn skipped(&mut self, cid: &str, pending: &PendingQueue, tree: &ChainTree) {
        if let Some(waiting) = self.awaiting.get_mut(cid) {
            waiting.looked_up = false;
        } else if pending.entries().contains_key(cid) || tree.contains(cid) {
            self.relookup.insert(cid.into());
        }
    }
    pub fn evidence_changed(
        &mut self,
        cids: &[String],
        config: &ChildProofConfig,
        pending: &PendingQueue,
        tree: &ChainTree,
    ) {
        for cid in cids.iter().cloned().collect::<BTreeSet<_>>() {
            if self.awaiting.contains_key(&cid)
                || pending.entries().contains_key(&cid)
                || tree.contains(&cid)
            {
                self.skipped(&cid, pending, tree);
            } else if self.wanted.insert(cid.clone()) {
                self.wanted_order.push_back(cid);
                while self.wanted_order.len() > config.max_wanted {
                    if let Some(old) = self.wanted_order.pop_front() {
                        self.wanted.remove(&old);
                    }
                }
            }
        }
    }
    pub fn await_header(
        &mut self,
        mut entry: HeaderEntry,
        peer: Option<PeerId>,
        config: &ChildProofConfig,
        pending: &PendingQueue,
        tree: &ChainTree,
        parent: Option<&ChainTree>,
    ) -> bool {
        let Ok(cid) = content_id(&entry.block) else {
            return false;
        };
        if let Some(held) = self.awaiting.get_mut(&cid) {
            if let Some(peer) = &peer
                && !held.announcers.contains(peer)
            {
                held.announcers.push(peer.clone());
            }
            if held.entry.children.is_none() {
                held.entry.children = entry.children.take();
            }
            if held.entry.spec.is_none() {
                held.entry.spec = bound(entry.spec.take(), &entry.block);
            }
            self.offer(entry.proofs, &cid, peer, config, pending, tree);
            return true;
        }
        let wanted = self.wanted.contains(&cid)
            || !pending.children_of(&cid).is_empty()
            || self.awaiting.values().any(|h| {
                h.entry
                    .block
                    .parent
                    .as_ref()
                    .is_some_and(|p| p.0.raw_cid == cid)
            })
            || parent.zip(tree.context()).is_some_and(|(p, c)| {
                c.path()
                    .last()
                    .is_some_and(|d| !p.committers(&cid, d).is_empty())
            });
        if entry.proofs.is_empty() && !wanted {
            return false;
        }
        if self.awaiting.len() >= config.max_awaiting {
            let victim = self
                .awaiting
                .iter()
                .filter(|(cid, _)| !self.holds(cid))
                .min_by_key(|(_, h)| h.sequence)
                .map(|(cid, _)| cid.clone());
            let Some(victim) = victim else { return false };
            self.awaiting.remove(&victim);
        }
        let Some(sequence) = self.sequence.checked_add(1) else {
            return false;
        };
        self.sequence = sequence;
        let proofs = std::mem::take(&mut entry.proofs);
        entry.spec = bound(entry.spec.take(), &entry.block);
        self.awaiting.insert(
            cid.clone(),
            AwaitingChildProof {
                entry,
                announcers: peer.iter().cloned().collect(),
                looked_up: false,
                sequence,
            },
        );
        self.wanted.remove(&cid);
        self.offer(proofs, &cid, peer, config, pending, tree);
        true
    }
    pub fn offer(
        &mut self,
        proofs: Vec<ChildBlockProof>,
        cid: &str,
        source: Option<PeerId>,
        config: &ChildProofConfig,
        pending: &PendingQueue,
        tree: &ChainTree,
    ) {
        let mut bytes = self.queued_bytes();
        let required = self.required.contains(cid);
        for proof in proofs
            .into_iter()
            .filter(|p| required || !credits(tree, pending, &p.root_cid, cid))
            .take(config.max_per_header)
        {
            let Ok(encoded) = proof.serialize() else {
                continue;
            };
            if encoded.len() > config.max_proof_bytes {
                continue;
            }
            let digest = hex::encode(Sha256::digest(&encoded));
            let digits = digest.trim_start_matches('0');
            let key = ChildProofKey {
                child_cid: cid.into(),
                proof_id: format!("0x{}", if digits.is_empty() { "0" } else { digits }),
            };
            if self.verifying.contains_key(&key) || self.queued.iter().any(|p| p.key == key) {
                continue;
            }
            if encoded.len() > config.max_queued_bytes.saturating_sub(bytes) {
                self.skipped(cid, pending, tree);
                continue;
            }
            bytes += encoded.len();
            self.queued.push_back(QueuedProof {
                key,
                proof,
                source: source.clone(),
                bytes: encoded.len(),
            });
        }
    }
    pub fn work(
        &mut self,
        path: &[String],
        config: &ChildProofConfig,
        pending: &PendingQueue,
        tree: &ChainTree,
        new_headers: &[String],
    ) -> ProofWork {
        let mut result = ProofWork::default();
        let mut peer_checks = self.checks(false);
        let mut index_checks = self.checks(true);
        let mut kept = VecDeque::new();
        while let Some(proof) = self.queued.pop_front() {
            let cid = &proof.key.child_cid;
            if !(self.awaiting.contains_key(cid)
                || pending.entries().contains_key(cid)
                || tree.contains(cid))
                || (!self.required.contains(cid)
                    && credits(tree, pending, &proof.proof.root_cid, cid))
            {
                continue;
            }
            let count = if proof.source.is_none() {
                &mut index_checks
            } else {
                &mut peer_checks
            };
            let cap = if proof.source.is_none() {
                config.index_checks
            } else {
                config.max_checks
            };
            if *count >= cap {
                kept.push_back(proof);
                continue;
            }
            *count += 1;
            let block = self
                .awaiting
                .get(cid)
                .map(|h| h.entry.block.clone())
                .or_else(|| pending.entries().get(cid).map(|h| h.block.clone()));
            result.checks.push(ChildProofJob {
                key: proof.key.clone(),
                proof: proof.proof.clone(),
                path: path.to_vec(),
                block,
            });
            self.verifying.insert(proof.key.clone(), proof);
        }
        self.queued = kept;
        let mut wanted: BTreeSet<_> = new_headers.iter().cloned().collect();
        for cid in self.awaiting.keys().cloned().collect::<Vec<_>>() {
            if !self.awaiting[&cid].looked_up && !self.holds(&cid) {
                self.awaiting.get_mut(&cid).unwrap().looked_up = true;
                wanted.insert(cid);
            }
        }
        wanted.append(&mut self.relookup);
        result.lookup = wanted.into_iter().collect();
        result
    }
    pub(crate) fn drop_missing(
        &mut self,
        completion: ChildProofCompletion,
        pending: &PendingQueue,
        tree: &ChainTree,
    ) {
        if completion.result.is_none() && self.verifying.remove(&completion.key).is_some() {
            self.skipped(&completion.key.child_cid, pending, tree);
        }
    }
    pub fn completed(&mut self, completion: ChildProofCompletion) -> Option<CheckedProof> {
        let queued = self.verifying.remove(&completion.key)?;
        let cid = completion.key.child_cid;
        match completion.result? {
            Ok(evidence) if evidence.child_cid() == cid && evidence.contribution().is_some() => {
                Some(CheckedProof::Accepted {
                    child_cid: cid,
                    proof: queued.proof,
                    evidence: Box::new(evidence),
                })
            }
            _ => {
                if !self.holds(&cid)
                    && let Some(waiting) = self.awaiting.get_mut(&cid)
                {
                    waiting.looked_up = false;
                }
                Some(CheckedProof::Rejected {
                    child_cid: cid,
                    source: queued.source,
                })
            }
        }
    }
}

/// A local derivation issued by the core from held parent work. It carries no
/// verified evidence until run() authenticates the generated/composed proof.
#[derive(Debug, Clone)]
pub struct ChildDeriveJob {
    child_cid: String,
    block: Option<Block>,
    carrier: String,
    grinds: BTreeSet<String>,
    path: Vec<String>,
}
impl ChildDeriveJob {
    pub(crate) fn new(
        child_cid: String,
        block: Option<Block>,
        carrier: String,
        grinds: BTreeSet<String>,
        path: Vec<String>,
    ) -> Self {
        Self {
            child_cid,
            block,
            carrier,
            grinds,
            path,
        }
    }
    pub(crate) fn add_grinds(&mut self, grinds: BTreeSet<String>) {
        self.grinds.extend(grinds);
    }
    pub fn child_cid(&self) -> &str {
        &self.child_cid
    }
    pub fn carrier(&self) -> &str {
        &self.carrier
    }
    pub fn path(&self) -> &[String] {
        &self.path
    }
    pub fn grinds(&self) -> &BTreeSet<String> {
        &self.grinds
    }
    pub fn run(
        self,
        held: Option<&Block>,
        upstream: &[ChildBlockProof],
        source: &dyn lattice_cashew::ContentSource,
    ) -> ChildDerivation {
        struct WithBlock<'a> {
            cid: &'a str,
            bytes: Vec<u8>,
            fallback: &'a dyn lattice_cashew::ContentSource,
        }
        impl lattice_cashew::ContentSource for WithBlock<'_> {
            fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
                if cid == self.cid {
                    Ok(self.bytes.clone())
                } else {
                    self.fallback.fetch(cid)
                }
            }
        }
        let mut result = ChildDerivation {
            child_cid: self.child_cid.clone(),
            checks: vec![],
        };
        let Some(block) = self.block.as_ref().or(held) else {
            return result;
        };
        if content_id(block).ok().as_deref() != Some(&self.child_cid) {
            return result;
        }
        let Ok(bytes) = lattice_cashew::cbor::encode(block) else {
            return result;
        };
        let overlay = WithBlock {
            cid: &self.child_cid,
            bytes,
            fallback: source,
        };
        let Ok(hop) = ChildBlockProof::generate(&self.carrier, self.path.last().unwrap(), &overlay)
        else {
            return result;
        };
        let proofs = if self.path.len() == 2 {
            vec![hop]
        } else {
            upstream.iter().map(|p| p.composing(&hop)).collect()
        };
        for proof in proofs
            .into_iter()
            .filter(|p| self.grinds.contains(&p.root_cid))
        {
            let key = ChildProofKey {
                child_cid: self.child_cid.clone(),
                proof_id: String::new(),
            };
            let completion = ChildProofJob {
                key,
                proof: proof.clone(),
                path: self.path.clone(),
                block: Some(block.clone()),
            }
            .run(None);
            let checked = match completion.result {
                Some(Ok(evidence))
                    if evidence.child_cid() == self.child_cid
                        && evidence.contribution().is_some() =>
                {
                    CheckedProof::Accepted {
                        child_cid: self.child_cid.clone(),
                        proof,
                        evidence: Box::new(evidence),
                    }
                }
                _ => CheckedProof::Rejected {
                    child_cid: self.child_cid.clone(),
                    source: None,
                },
            };
            result.checks.push(checked);
        }
        result
    }
}
#[derive(Debug)]
pub struct ChildDerivation {
    child_cid: String,
    checks: Vec<CheckedProof>,
}
impl ChildDerivation {
    pub(crate) fn into_checks(self) -> (String, Vec<CheckedProof>) {
        (self.child_cid, self.checks)
    }
}

#[cfg(test)]
pub(crate) mod missing_tests {
    use super::*;
    use lattice_cashew::{Header, MerkleDictionary, Volume, cbor};
    use lattice_primitives::*;
    fn root() -> Block {
        let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
        Block {
            version: 1,
            parent: None,
            transactions: Header::new(MerkleDictionary::default()).unwrap(),
            target: UInt256([255; 32]),
            next_target: UInt256([255; 32]),
            spec: Volume::new(ChainSpec {
                max_number_of_transactions_per_block: 100,
                max_state_growth: 100_000,
                max_block_size: 1_000_000,
                premine: 0,
                target_block_time: 1000,
                initial_reward: 1024,
                halving_interval: 210_000,
                half_life: 120,
                wasm_policies: vec![],
            })
            .unwrap(),
            parent_state: empty.clone(),
            prev_state: empty.clone(),
            post_state: empty,
            children: Header::new(BTreeMap::new()).unwrap(),
            height: 0,
            timestamp: 1000,
            reward_recipient: None,
            nonce: 0,
        }
    }

    pub(crate) fn fixture(nonce: u64) -> (HeaderEntry, String) {
        let mut child = root();
        child.nonce = nonce;
        let mut carrier = root();
        carrier.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(child.clone()).unwrap(),
        )]))
        .unwrap();
        let cid = content_id(&child).unwrap();
        let source = BTreeMap::from([
            (
                content_id(&carrier).unwrap(),
                cbor::encode(&carrier).unwrap(),
            ),
            (
                carrier.children.raw_cid.clone(),
                cbor::encode(carrier.children.node.as_deref().unwrap()).unwrap(),
            ),
            (cid.clone(), cbor::encode(&child).unwrap()),
        ]);
        let proof =
            ChildBlockProof::generate(&content_id(&carrier).unwrap(), "Alpha", &source).unwrap();
        (
            HeaderEntry {
                children: child.children.node.as_deref().cloned(),
                spec: child.spec.0.node.as_deref().cloned(),
                block: child,
                proofs: vec![proof],
            },
            cid,
        )
    }

    #[test]
    fn missing_local_block_releases_slot_retries_and_never_blames_peer() {
        let path = vec!["Nexus".into(), "Alpha".into()];
        let tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
        let pending = PendingQueue::default();
        let config = ChildProofConfig::default();
        let peer = PeerId {
            key: "peer".into(),
            session: 1,
        };
        let (entry, cid) = fixture(0);
        let mut queue = ChildProofSync::default();
        assert!(queue.await_header(
            entry.clone(),
            Some(peer.clone()),
            &config,
            &pending,
            &tree,
            None
        ));
        let mut job = queue
            .work(&path, &config, &pending, &tree, &[])
            .checks
            .remove(0);
        // A weighed header's job resolves its block from the local journal.
        job.block = None;
        assert_eq!(queue.checks(false), 1);
        // The held header may already have exhausted its earlier index lookup.
        queue.awaiting.get_mut(&cid).unwrap().looked_up = true;
        let dropped = job.clone().run(None);
        assert_eq!(dropped.missing_block(), Some(cid.as_str()));
        queue.drop_missing(dropped, &pending, &tree);
        assert_eq!(queue.checks(false), 0);
        assert_eq!(queue.queued_bytes(), 0);
        assert!(!queue.awaiting[&cid].looked_up);
        let work = queue.work(&path, &config, &pending, &tree, &[]);
        assert_eq!(work.lookup, vec![cid.clone()]);
        assert!(work.checks.is_empty());
        // A late completion after the slot was removed must not repeat retries.
        queue.drop_missing(job.run(None), &pending, &tree);
        assert!(queue.awaiting[&cid].looked_up);
        queue.offer(
            entry.proofs.clone(),
            &cid,
            Some(peer.clone()),
            &config,
            &pending,
            &tree,
        );
        let job = queue
            .work(&path, &config, &pending, &tree, &[])
            .checks
            .remove(0);
        assert!(matches!(
            queue.completed(job.run(None)),
            Some(CheckedProof::Accepted { .. })
        ));
        // Held content + a genuinely bad proof still blames its sender.
        let mut bad = entry.proofs[0].clone();
        bad.entries[0].1[0] ^= 1;
        queue.offer(
            vec![bad],
            &cid,
            Some(peer.clone()),
            &config,
            &pending,
            &tree,
        );
        let job = queue
            .work(&path, &config, &pending, &tree, &[])
            .checks
            .remove(0);
        assert!(
            matches!(queue.completed(job.run(None)), Some(CheckedProof::Rejected { source: Some(p), .. }) if p == peer)
        );
    }
}
