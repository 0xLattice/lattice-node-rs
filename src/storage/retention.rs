//! Retention planning. Plans do not delete bytes: storage must pin entering
//! roots before committing counts, then release leaving roots under its lock.
use super::StoreError;
use lattice_cashew::{ContentSource, Volume, VolumeMerkleDictionary, VolumeRadixNode};
use lattice_primitives::{LatticeState, LatticeStateHeader};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateRetention {
    #[default]
    All,
    Tip,
    None,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BodyRetention {
    #[default]
    All,
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub state: StateRetention,
    pub bodies: BodyRetention,
}

#[derive(Debug, Clone)]
pub struct RetentionScopes {
    pub chain: String,
}
impl RetentionScopes {
    pub fn body_prefix(&self) -> String {
        format!("{}:body:", self.chain)
    }
    pub fn reads_prefix(&self) -> String {
        format!("{}:reads:", self.chain)
    }
    pub fn state(&self, path: &[String]) -> String {
        format!("{}:state:{}", self.chain, path.join("/"))
    }
    pub fn executed(&self, path: &[String]) -> String {
        format!("{}:executed:{}", self.chain, path.join("/"))
    }
    pub fn body(&self, cid: &str, path: &[String]) -> String {
        format!("{}{}:{cid}", self.body_prefix(), path.join("/"))
    }
    pub fn reads(&self, cid: &str, path: &[String]) -> String {
        format!("{}{}:{cid}", self.reads_prefix(), path.join("/"))
    }
    pub(super) fn prepared_prefix(&self) -> String {
        format!("{}:prepared-state:", self.chain)
    }
    pub(super) fn prepared_block_prefix(&self, cid: &str, path: &[String]) -> String {
        format!("{}{}:{cid}:", self.prepared_prefix(), path.join("/"))
    }
    pub(super) fn prepared(&self, state: &str, cid: &str, path: &[String]) -> String {
        format!("{}{state}", self.prepared_block_prefix(cid, path))
    }
    pub(super) fn prepared_block(&self, scope: &str) -> Option<(Vec<String>, String)> {
        let rest = scope.strip_prefix(&self.prepared_prefix())?;
        let (block, _) = rest.rsplit_once(':')?;
        let (path, cid) = block.rsplit_once(':')?;
        Some((path.split('/').map(str::to_owned).collect(), cid.to_owned()))
    }
    pub fn operator_pins(&self) -> String {
        format!("{}:operator", self.chain)
    }
    pub fn block(&self, scope: &str, reads: bool) -> Option<(Vec<String>, String)> {
        let prefix = if reads {
            self.reads_prefix()
        } else {
            self.body_prefix()
        };
        let (path, cid) = scope.strip_prefix(&prefix)?.rsplit_once(':')?;
        Some((
            path.split('/')
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect(),
            cid.to_owned(),
        ))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) enum StateHeader {
    State(Volume<LatticeState>),
    Numbers(Volume<VolumeMerkleDictionary<u64>>),
    Strings(Volume<VolumeMerkleDictionary<String>>),
    NumberNode(Volume<VolumeRadixNode<u64>>),
    StringNode(Volume<VolumeRadixNode<String>>),
}
impl StateHeader {
    pub(super) fn cid(&self) -> &str {
        match self {
            Self::State(h) => &h.raw_cid,
            Self::Numbers(h) => &h.raw_cid,
            Self::Strings(h) => &h.raw_cid,
            Self::NumberNode(h) => &h.raw_cid,
            Self::StringNode(h) => &h.raw_cid,
        }
    }
    pub(super) fn materialize(
        &self,
        source: &impl ContentSource,
    ) -> Result<Vec<lattice_cashew::SerializedVolume>, lattice_cashew::Error> {
        macro_rules! resolve {
            ($h:expr) => {
                lattice_cashew::volume::serialize_recursive(&$h.resolve(source, None)?, &())
            };
        }
        match self {
            Self::State(h) => resolve!(h),
            Self::Numbers(h) => resolve!(h),
            Self::Strings(h) => resolve!(h),
            Self::NumberNode(h) => resolve!(h),
            Self::StringNode(h) => resolve!(h),
        }
    }
    pub(super) fn children(
        &self,
        source: &impl ContentSource,
    ) -> Result<Vec<Self>, lattice_cashew::Error> {
        macro_rules! nodes {
            ($h:expr, $kind:ident) => {{
                let resolved = $h.resolve(source, None)?;
                let node = resolved
                    .node
                    .as_deref()
                    .ok_or_else(|| lattice_cashew::Error::MissingContent($h.raw_cid.clone()))?;
                Ok(node
                    .children
                    .iter()
                    .map(|e| Self::$kind(e.value.clone()))
                    .collect())
            }};
        }
        match self {
            Self::State(h) => {
                let resolved = h.resolve(source, None)?;
                let state = resolved
                    .node
                    .as_deref()
                    .ok_or_else(|| lattice_cashew::Error::MissingContent(h.raw_cid.clone()))?;
                // Sorted Swift Node properties; repeated references remain repeated.
                Ok(vec![
                    Self::Numbers(state.account_state.clone()),
                    Self::Numbers(state.deposit_state.clone()),
                    Self::Strings(state.general_state.clone()),
                    Self::Strings(state.receipt_state.clone()),
                ])
            }
            Self::Numbers(h) => nodes!(h, NumberNode),
            Self::Strings(h) => nodes!(h, StringNode),
            Self::NumberNode(h) => nodes!(h, NumberNode),
            Self::StringNode(h) => nodes!(h, StringNode),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TipStatePlan {
    pub changes: BTreeMap<String, i64>,
    pub entering: Vec<String>,
}
/// Swift TipStateWalk: add the new root, then remove the old root. A node's
/// outgoing edges are traversed only when its total count crosses zero. Missing
/// local nodes are skipped and may be fetched/countable on a subsequent move.
/// The count reader and source must belong to one serialized storage turn.
pub fn plan_tip_state(
    new: &LatticeStateHeader,
    old: Option<&LatticeStateHeader>,
    source: &impl ContentSource,
    mut count: impl FnMut(&str) -> Result<i64, StoreError>,
) -> Result<TipStatePlan, StoreError> {
    let mut counts = BTreeMap::<String, i64>::new();
    let mut changes = BTreeMap::<String, i64>::new();
    for (root, hold) in std::iter::once((new, true)).chain(old.map(|h| (h, false))) {
        let mut stack = vec![StateHeader::State(root.clone())];
        while let Some(header) = stack.pop() {
            let cid = header.cid().to_owned();
            let base = match counts.get(&cid) {
                Some(v) => *v,
                None => {
                    let value = count(&cid)?;
                    if value < 0 {
                        return Err(StoreError::Corrupt("negative state reference count".into()));
                    }
                    counts.insert(cid.clone(), value);
                    value
                }
            };
            let change = changes.get(&cid).copied().unwrap_or(0);
            let current = base
                .checked_add(change)
                .ok_or_else(|| StoreError::Corrupt("state reference count overflow".into()))?;
            if hold {
                let children = if current == 0 {
                    let Ok(children) = header.children(source) else {
                        continue;
                    };
                    children
                } else {
                    vec![]
                };
                current
                    .checked_add(1)
                    .ok_or_else(|| StoreError::Corrupt("state reference count overflow".into()))?;
                changes.insert(
                    cid,
                    change
                        .checked_add(1)
                        .ok_or_else(|| StoreError::Corrupt("state count delta overflow".into()))?,
                );
                stack.extend(children.into_iter().rev());
            } else if current > 0 {
                changes.insert(
                    cid,
                    change
                        .checked_sub(1)
                        .ok_or_else(|| StoreError::Corrupt("state count delta overflow".into()))?,
                );
                if current == 1
                    && let Ok(children) = header.children(source)
                {
                    stack.extend(children.into_iter().rev());
                }
            }
        }
    }
    let entering = changes
        .iter()
        .filter(|(cid, delta)| **delta > 0 && counts.get(*cid) == Some(&0))
        .map(|(cid, _)| cid.clone())
        .collect();
    Ok(TipStatePlan { changes, entering })
}
