//! Bounded pending headers. Only unweighed leaves can be evicted; a stronger
//! pending descendant raises every pending ancestor's scheduling priority.
use super::sync::PeerId;
use lattice_block_tree::{ChildBlockProof, VerifiedChildEvidence};
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader, ChainSpec, UInt256};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
#[derive(Debug, Clone)]
pub struct PendingHeader {
    pub block_cid: String,
    pub block: Block,
    pub children: Option<FlatDictionary<BlockHeader>>,
    pub spec: Option<ChainSpec>,
    pub hash: UInt256,
    pub bytes: usize,
    pub announcers: Vec<PeerId>,
    pub asked_parent: bool,
    pub not_before: Option<i64>,
    pub evidence: BTreeMap<String, VerifiedChildEvidence>,
    pub proofs: BTreeMap<String, ChildBlockProof>,
}
impl PendingHeader {
    pub fn parent(&self) -> Option<&str> {
        self.block.parent.as_ref().map(|h| h.0.raw_cid.as_str())
    }
    pub fn source(&self) -> Option<&PeerId> {
        self.announcers.first()
    }
}
#[derive(Debug, Clone, Default)]
pub struct PendingQueue {
    entries: BTreeMap<String, PendingHeader>,
    bytes: usize,
    children_of: BTreeMap<String, BTreeSet<String>>,
    by_child_index: BTreeMap<String, BTreeSet<String>>,
    priority: BTreeMap<String, UInt256>,
    leaves: BinaryHeap<(UInt256, String)>,
}
impl PendingQueue {
    pub(crate) fn metadata_mut(&mut self, cid: &str) -> Option<&mut PendingHeader> {
        self.entries.get_mut(cid)
    }
    pub fn entries(&self) -> &BTreeMap<String, PendingHeader> {
        &self.entries
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn priorities(&self) -> &BTreeMap<String, UInt256> {
        &self.priority
    }
    pub fn bookkeeping(&self) -> usize {
        self.leaves.len()
    }
    pub fn children_of(&self, cid: &str) -> Vec<String> {
        self.children_of
            .get(cid)
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }
    pub fn committing(&self, cid: &str) -> Vec<&PendingHeader> {
        self.by_child_index
            .get(cid)
            .into_iter()
            .flatten()
            .filter_map(|hash| self.entries.get(hash))
            .collect()
    }
    fn is_leaf(&self, cid: &str) -> bool {
        self.children_of.get(cid).is_none_or(BTreeSet::is_empty)
    }
    fn lowest_priority(&self, cid: &str) -> UInt256 {
        self.children_of
            .get(cid)
            .into_iter()
            .flatten()
            .filter_map(|c| self.priority.get(c))
            .copied()
            .min()
            .unwrap_or(UInt256([255; 32]))
    }
    pub fn insert(&mut self, header: PendingHeader) -> Result<bool, &'static str> {
        let cid = header.block_cid.clone();
        if self.entries.contains_key(&cid) {
            return Ok(false);
        }
        let bytes = self
            .bytes
            .checked_add(header.bytes)
            .ok_or("pending byte accounting overflow")?;
        self.by_child_index
            .entry(header.block.children.raw_cid.clone())
            .or_default()
            .insert(cid.clone());
        self.priority
            .insert(cid.clone(), header.hash.min(self.lowest_priority(&cid)));
        if self.is_leaf(&cid) {
            self.leaves.push((header.hash, cid.clone()));
        }
        let parent = header.parent().map(String::from);
        self.entries.insert(cid.clone(), header);
        self.bytes = bytes;
        if let Some(parent) = parent {
            self.children_of
                .entry(parent.clone())
                .or_default()
                .insert(cid);
            self.lift(&parent);
        }
        Ok(true)
    }
    pub fn remove(&mut self, cid: &str) -> Option<PendingHeader> {
        let removed = self.entries.remove(cid)?;
        self.bytes -= removed.bytes;
        self.priority.remove(cid);
        let children = &removed.block.children.raw_cid;
        if let Some(index) = self.by_child_index.get_mut(children) {
            index.remove(cid);
            if index.is_empty() {
                self.by_child_index.remove(children);
            }
        }
        if let Some(parent) = removed.parent() {
            if let Some(children) = self.children_of.get_mut(parent) {
                children.remove(cid);
                if children.is_empty() {
                    self.children_of.remove(parent);
                }
            }
            if let Some(held) = self.entries.get(parent) {
                if self.is_leaf(parent) {
                    self.leaves.push((held.hash, parent.into()));
                }
                self.lift(parent);
            }
        }
        Some(removed)
    }
    pub fn set_children(
        &mut self,
        cid: &str,
        children: FlatDictionary<BlockHeader>,
        size: usize,
    ) -> Result<(), &'static str> {
        let Some(held) = self.entries.get_mut(cid).filter(|h| h.children.is_none()) else {
            return Ok(());
        };
        let header_bytes = held
            .bytes
            .checked_add(size)
            .ok_or("pending header size overflow")?;
        let total = self
            .bytes
            .checked_add(size)
            .ok_or("pending byte accounting overflow")?;
        held.children = Some(children);
        held.bytes = header_bytes;
        self.bytes = total;
        Ok(())
    }
    fn lift(&mut self, cid: &str) {
        let mut current = Some(cid.to_string());
        let mut seen = BTreeSet::new();
        while let Some(cid) = current {
            if !seen.insert(cid.clone()) {
                break;
            }
            let Some(header) = self.entries.get(&cid) else {
                break;
            };
            let updated = header.hash.min(self.lowest_priority(&cid));
            if self.priority.get(&cid) == Some(&updated) {
                break;
            }
            current = header.parent().map(String::from);
            self.priority.insert(cid, updated);
        }
    }
    pub fn evict(&mut self, budget: usize) -> Vec<PendingHeader> {
        let mut evicted = vec![];
        while self.bytes > budget {
            let Some((hash, cid)) = self.leaves.pop() else {
                break;
            };
            if self.entries.get(&cid).is_some_and(|h| h.hash == hash) && self.is_leaf(&cid) {
                evicted.push(self.remove(&cid).unwrap());
            }
        }
        evicted
    }
    pub fn compact(&mut self) {
        if self.leaves.len() <= self.entries.len().saturating_mul(2).saturating_add(8) {
            return;
        }
        let mut seen = BTreeSet::new();
        let items = std::mem::take(&mut self.leaves);
        self.leaves = items
            .into_iter()
            .filter(|(hash, cid)| {
                self.entries.get(cid).is_some_and(|h| h.hash == *hash)
                    && self.is_leaf(cid)
                    && seen.insert(cid.clone())
            })
            .collect();
    }
}
