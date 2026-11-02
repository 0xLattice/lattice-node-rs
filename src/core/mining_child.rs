//! Private child preflight completion bound to executed child and parent views.
use super::*;
#[derive(Debug)]
pub struct ChildPreflightCompletion {
    job: PreflightJob,
    path: Vec<String>,
    parent_revision: u64,
    parent_tip: String,
    result: lattice_validation::TransactionPreflightResult,
}
/// Captured on the actor; worker owns both content snapshot and directory lease.
pub struct ChildPreflightTask {
    job: PreflightJob,
    path: Vec<String>,
    parent_revision: u64,
    parent_tip: String,
    parent_state: Option<lattice_primitives::LatticeStateHeader>,
    snapshot: lattice_volume_broker::DiskReadSnapshot,
    _storage: std::sync::Arc<crate::storage::NodeStorage>,
}
impl ChildPreflightTask {
    pub fn run(self, now: i64) -> Result<ChildPreflightCompletion, crate::storage::StoreError> {
        let tip = lattice_cashew::Volume(lattice_cashew::Header::from_cid(&self.job.tip_cid)?);
        let result = lattice_validation::TransactionPreflight {
            transaction: &self.job.transaction,
            tip: &tip,
            chain_path: &self.path,
            parent_state: self.parent_state.as_ref(),
            now_milliseconds: now,
            checks_policies: self.job.checks_policies,
        }
        .classify(&SnapshotSource(&self.snapshot))?;
        Ok(ChildPreflightCompletion {
            job: self.job,
            path: self.path,
            parent_revision: self.parent_revision,
            parent_tip: self.parent_tip,
            result,
        })
    }
}
impl DurableMiningCore {
    pub fn execute_child_preflight(
        &self,
        job: &PreflightJob,
        tree: &lattice_block_tree::ChainTree,
        parent: &lattice_block_tree::ChainTree,
        now: i64,
    ) -> Result<Option<ChildPreflightCompletion>, crate::storage::StoreError> {
        self.prepare_child_preflight(job, tree, parent)?
            .map(|task| task.run(now))
            .transpose()
    }
    pub fn prepare_child_preflight(
        &self,
        job: &PreflightJob,
        tree: &lattice_block_tree::ChainTree,
        parent: &lattice_block_tree::ChainTree,
    ) -> Result<Option<ChildPreflightTask>, crate::storage::StoreError> {
        let Some(context) = tree.context().filter(|c| !c.is_root()) else {
            return Ok(None);
        };
        if parent
            .context()
            .is_none_or(|p| p.path() != &context.path()[..context.path().len() - 1])
            || job.tip_epoch != self.state.tip_epoch
            || job.tip_cid != self.state.tip_cid
            || !tree.has_executed_ancestry(&job.tip_cid)
            || content_id(&job.transaction)? != job.cid
        {
            return Ok(None);
        }
        let parent_tip = parent.heaviest_executed_tip(&BTreeSet::new());
        let parent_state = if parent.has_executed_ancestry(&parent_tip) {
            parent
                .header_snapshot(&parent_tip)
                .map(|s| {
                    lattice_cashew::Header::from_cid(&s.post_state_cid).map(lattice_cashew::Volume)
                })
                .transpose()?
        } else {
            None
        };
        Ok(Some(ChildPreflightTask {
            job: job.clone(),
            path: context.path().to_vec(),
            parent_revision: parent.current_revision(),
            parent_tip,
            parent_state,
            snapshot: self.storage.preflight_snapshot()?,
            _storage: self.storage.clone(),
        }))
    }
    pub fn complete_child_preflight(
        &mut self,
        completion: ChildPreflightCompletion,
        parent: &lattice_block_tree::ChainTree,
        now: i64,
    ) -> Result<Vec<MiningEffect>, crate::storage::StoreError> {
        let ChildPreflightCompletion {
            job,
            path,
            parent_revision,
            parent_tip,
            result,
        } = completion;
        if job.tip_epoch != self.state.tip_epoch
            || job.tip_cid != self.state.tip_cid
            || parent
                .context()
                .is_none_or(|p| p.path() != &path[..path.len() - 1])
            || result.transaction_cid() != job.cid
            || result.tip_cid() != job.tip_cid
            || result.chain_path() != path
            || result.checks_policies() != job.checks_policies
        {
            return Ok(vec![]);
        }
        if parent.current_revision() != parent_revision
            || parent.heaviest_executed_tip(&BTreeSet::new()) != parent_tip
        {
            // The child epoch can stay unchanged when only the parent's graph
            // revision moves. Keep the admission/slot and rerun with a fresh
            // parent basis, rather than losing the sole outstanding result.
            return Ok(vec![MiningEffect::Preflight(job)]);
        }
        let disposition = match result.disposition() {
            lattice_validation::PreflightDisposition::Ready => MempoolDisposition::Ready,
            lattice_validation::PreflightDisposition::Future => MempoolDisposition::Future,
            lattice_validation::PreflightDisposition::Invalid => MempoolDisposition::Invalid,
            lattice_validation::PreflightDisposition::Unavailable => {
                MempoolDisposition::Unavailable
            }
        };
        self.step(
            MiningEvent::Preflighted(job, disposition, result.debit_balances().clone()),
            now,
        )
    }
}

impl DurableMiningCore {
    /// A child can select a different genesis branch with a different spec.
    /// Publish its new limits and tip together only after pool deltas persist.
    pub fn move_tip_with_spec(
        &mut self,
        movement: TipMove,
        spec: ChainSpec,
        now: i64,
    ) -> Result<Vec<MiningEffect>, crate::storage::StoreError> {
        let mut next = self.state.clone();
        next.set_spec(Some(spec));
        let effects = next.step(MiningEvent::TipMoved(movement), now);
        for effect in &effects {
            if let MiningEffect::PoolChanged(delta) = effect {
                self.storage.persist_pool_delta(delta)?;
            }
        }
        self.state = next;
        Ok(effects
            .into_iter()
            .filter(|e| !matches!(e, MiningEffect::PoolChanged(_)))
            .collect())
    }
}
