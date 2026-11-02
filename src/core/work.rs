//! Bounded externally-issued work; checking a submitted nonce is not admission.
use lattice_primitives::{Block, UInt256};
use std::collections::{BTreeMap, VecDeque};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateChain {
    pub chain_path: Vec<String>,
    pub target: UInt256,
    pub height: u64,
    pub reward_recipient: Option<String>,
}
#[derive(Debug, Clone)]
pub struct WorkTemplate {
    pub work_id: String,
    pub block: Block,
    pub search_target: UInt256,
    pub targets: Vec<UInt256>,
    pub tip_cid: String,
    pub pool_version: u64,
    pub expires_at: i64,
    pub digest: String,
    pub chains: Vec<TemplateChain>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    #[error("unknown work")]
    UnknownWork,
    #[error("expired work")]
    Expired,
    #[error("misses search target")]
    MissesSearchTarget,
    #[error("build failed")]
    BuildFailed,
    #[error("busy")]
    Busy,
    #[error("context changed")]
    ContextChanged,
}
#[derive(Debug, Clone)]
pub struct WorkTemplateBook {
    lifetime: i64,
    capacity: usize,
    templates: BTreeMap<String, WorkTemplate>,
    order: VecDeque<String>,
}
impl Default for WorkTemplateBook {
    fn default() -> Self {
        Self::new(30_000, 16)
    }
}
impl WorkTemplateBook {
    pub fn new(lifetime: i64, capacity: usize) -> Self {
        assert!(lifetime > 0 && capacity > 0);
        Self {
            lifetime,
            capacity,
            templates: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }
    pub fn lifetime(&self) -> i64 {
        self.lifetime
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn count(&self) -> usize {
        self.templates.len()
    }
    pub fn template(&self, id: &str) -> Option<&WorkTemplate> {
        self.templates.get(id)
    }
    pub fn issue(&mut self, template: WorkTemplate, now: i64) -> WorkTemplate {
        self.order.retain(|id| *id != template.work_id);
        self.order.push_back(template.work_id.clone());
        if let Some(existing) = self
            .templates
            .get(&template.work_id)
            .filter(|t| now < t.expires_at)
        {
            return existing.clone();
        }
        self.templates
            .insert(template.work_id.clone(), template.clone());
        while self.order.len() > self.capacity {
            self.templates.remove(&self.order.pop_front().unwrap());
        }
        template
    }
    pub fn discard(&mut self, id: &str) {
        self.templates.remove(id);
        self.order.retain(|key| key != id);
    }
    pub fn submission(&mut self, id: &str, nonce: u64, now: i64) -> Result<Block, TemplateError> {
        let template = self.templates.get(id).ok_or(TemplateError::UnknownWork)?;
        if now >= template.expires_at {
            self.discard(id);
            return Err(TemplateError::Expired);
        }
        let mut candidate = template.block.clone();
        candidate.nonce = nonce;
        if template.search_target == UInt256([0; 32])
            || candidate.pow_hash() > template.search_target.0
        {
            return Err(TemplateError::MissesSearchTarget);
        }
        Ok(candidate)
    }
    pub fn invalidate_all(&mut self) {
        self.templates.clear();
        self.order.clear();
    }
}
