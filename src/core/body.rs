//! Body fetch/connect scheduling, ported from BodyPipeline.swift.
use lattice_block_tree::{
    ChainTree, ConnectJob, CrossChainEvidenceRequirement, fork_choice_prefers_block,
};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parked {
    pub not_before: i64,
    pub attempts: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeMark {
    act_on: String,
    window: Vec<String>,
}
#[derive(Debug, Clone, Default)]
pub struct BodyPipeline {
    pub requested: BTreeSet<String>,
    pub arrived: BTreeSet<String>,
    pub connecting: Option<String>,
    pub parked: BTreeMap<String, Parked>,
    pub awaiting_parent: BTreeMap<String, CrossChainEvidenceRequirement>,
    seen: Option<TreeMark>,
}
#[derive(Debug, Clone)]
pub enum BodyEffect {
    Fetch(String),
    Cancel(String),
    Connect(ConnectJob),
}
fn by_weight(tree: &ChainTree, mut hashes: Vec<String>) -> Vec<String> {
    hashes.sort_by(|a, b| {
        tree.subtree_work(b)
            .cmp(&tree.subtree_work(a))
            .then_with(|| {
                if a == b {
                    std::cmp::Ordering::Equal
                } else if fork_choice_prefers_block(a, b) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            })
    });
    hashes
}
pub fn best_window(tree: &ChainTree, count: usize) -> Vec<String> {
    let start = tree
        .executed_prefix()
        .and_then(|prefix| tree.canonical_path().iter().position(|h| h == prefix))
        .map(|i| i + 1)
        .unwrap_or(0);
    tree.canonical_path()
        .iter()
        .skip(start)
        .take(count)
        .cloned()
        .collect()
}

pub fn fork_children(tree: &ChainTree) -> Vec<String> {
    let mut wanted = vec![];
    let mut candidates = tree
        .executed_prefix()
        .map(|p| tree.children(p))
        .unwrap_or_else(|| tree.genesis_roots());
    loop {
        let children = by_weight(
            tree,
            candidates
                .into_iter()
                .filter(|h| tree.is_executed(h) || !tree.is_excluded_root(h))
                .collect(),
        );
        wanted.extend(
            children
                .iter()
                .take_while(|h| !tree.is_executed(h))
                .cloned(),
        );
        let Some(executed) = children.iter().find(|h| tree.is_executed(h)) else {
            return wanted;
        };
        candidates = tree.children(executed);
    }
}
pub fn body_window(tree: &ChainTree, count: usize) -> Vec<String> {
    let mut result = fork_children(tree);
    for h in best_window(tree, count) {
        if !result.contains(&h) {
            result.push(h)
        }
    }
    result
}
pub fn next_to_connect(tree: &ChainTree, count: usize) -> Vec<String> {
    body_window(tree, count)
        .into_iter()
        .filter(|h| tree.parent(h).is_none_or(|p| tree.is_executed(p)))
        .collect()
}
pub fn connect_distances(tree: &ChainTree, count: usize) -> BTreeMap<String, u64> {
    let window = body_window(tree, count);
    let held: BTreeSet<_> = window.iter().map(String::as_str).collect();
    let mut result = BTreeMap::new();
    for hash in &window {
        let mut at = hash.as_str();
        let mut seen = BTreeSet::new();
        while let Some(parent) = tree.parent(at) {
            if !seen.insert(parent) || !held.contains(parent) || tree.is_executed(parent) {
                break;
            }
            at = parent;
        }
        let distance = tree.height(hash).unwrap_or(0).saturating_sub(
            tree.height(at)
                .unwrap_or_else(|| tree.height(hash).unwrap_or(0)),
        );
        result.insert(hash.clone(), distance);
    }
    result
}
impl BodyPipeline {
    pub fn next_retry(&self, now: i64) -> Option<i64> {
        self.parked
            .values()
            .map(|p| p.not_before)
            .filter(|t| *t > now)
            .min()
    }
    pub fn body_fetched(&mut self, cid: &str) {
        if self.requested.remove(cid) {
            self.arrived.insert(cid.into());
        }
    }
    pub fn parent_facts_present(&mut self, blocks: &[String]) {
        for cid in blocks {
            self.awaiting_parent.remove(cid);
        }
    }
    /// Availability failure only: no invalidity fact and no peer blame.
    pub fn park_unavailable(&mut self, cid: &str, now: i64, base: i64, cap: i64) {
        if self.connecting.as_deref() == Some(cid) {
            self.connecting = None
        }
        self.arrived.remove(cid);
        let attempts = self
            .parked
            .get(cid)
            .map(|p| p.attempts)
            .unwrap_or(0)
            .saturating_add(1);
        let multiplier = 1i64 << attempts.saturating_sub(1).min(20);
        let wait = base.max(0).saturating_mul(multiplier).min(cap.max(0));
        self.parked.insert(
            cid.into(),
            Parked {
                not_before: now.saturating_add(wait),
                attempts,
            },
        );
    }
    pub fn await_parent(&mut self, cid: String, requirement: CrossChainEvidenceRequirement) {
        if self.connecting.as_deref() == Some(&cid) {
            self.connecting = None
        }
        self.awaiting_parent.insert(cid, requirement);
    }
    pub fn completed(&mut self, cid: &str) {
        if self.connecting.as_deref() == Some(cid) {
            self.connecting = None
        }
        self.arrived.remove(cid);
        self.awaiting_parent.remove(cid);
    }
    pub fn schedule(&mut self, tree: &ChainTree, count: usize, now: i64) -> Vec<BodyEffect> {
        let window = body_window(tree, count);
        let wanted: BTreeSet<_> = window.iter().cloned().collect();
        let mark = TreeMark {
            act_on: tree.heaviest_executed_tip(&BTreeSet::new()),
            window: best_window(tree, count),
        };
        if self.seen.as_ref() != Some(&mark) {
            self.seen = Some(mark);
            self.parked.clear();
        }
        let mut effects: Vec<_> = self
            .requested
            .difference(&wanted)
            .cloned()
            .map(BodyEffect::Cancel)
            .collect();
        self.requested.retain(|cid| wanted.contains(cid));
        self.arrived.retain(|cid| wanted.contains(cid));
        self.parked.retain(|cid, _| wanted.contains(cid));
        self.awaiting_parent.retain(|cid, _| wanted.contains(cid));
        for cid in &window {
            if !self.requested.contains(cid)
                && !self.arrived.contains(cid)
                && self.parked.get(cid).is_none_or(|p| p.not_before <= now)
            {
                self.requested.insert(cid.clone());
                effects.push(BodyEffect::Fetch(cid.clone()));
            }
        }
        if self.connecting.is_none()
            && let Some(next) = window.iter().find(|cid| {
                self.arrived.contains(*cid)
                    && !self.awaiting_parent.contains_key(*cid)
                    && tree
                        .parent(cid)
                        .is_none_or(|p| tree.has_executed_ancestry(p))
            })
            && let Some(job) = tree.connect_job(next, None)
        {
            self.connecting = Some(next.clone());
            effects.push(BodyEffect::Connect(job));
        }
        effects
    }
}
