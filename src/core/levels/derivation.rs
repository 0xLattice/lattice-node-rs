//! Local owed-grind derivation. Queue metadata is independent of the peer/index
//! proof byte budget: a valid later carrier must not disappear behind a prefix.
use super::*;
use crate::core::child_proofs::{ChildDerivation, ChildDeriveJob};

impl ChildChainCore {
    /// Reconsider arrivals even when the parent revision did not change. A parent
    /// revision change also revisits held/pending/waiting children, including
    /// side branches, so new upstream work is attributed at its committed child.
    pub fn schedule_derivations(&mut self, parent: &ChainTree, arrived: &[String]) {
        let path = self.tree.context().unwrap().path();
        if parent
            .context()
            .is_none_or(|p| p.path() != &path[..path.len() - 1])
        {
            return;
        }
        let directory = path.last().unwrap().clone();
        let mut children: BTreeSet<String> = arrived.iter().cloned().collect();
        if self.derivation_parent_revision != Some(parent.current_revision()) {
            self.derivation_parent_revision = Some(parent.current_revision());
            children.extend(self.pending.entries().keys().cloned());
            children.extend(self.proofs.awaiting.keys().cloned());
            let mut frontier = self.tree.genesis_roots();
            while let Some(cid) = frontier.pop() {
                frontier.extend(self.tree.children(&cid));
                children.insert(cid);
            }
        }
        for cid in children {
            if !self.tree.contains(&cid)
                && !self.pending.entries().contains_key(&cid)
                && !self.proofs.awaiting.contains_key(&cid)
            {
                continue;
            }
            for carrier in parent.committers(&cid, &directory) {
                let Some(meta) = parent.consensus_block(&carrier) else {
                    continue;
                };
                let credited = self.tree.consensus_block(&cid);
                let grinds = meta
                    .work_contributions
                    .keys()
                    .filter(|grind| {
                        !meta.attributed_runs.contains(*grind)
                            && !credited
                                .as_ref()
                                .is_some_and(|m| m.work_contributions.contains_key(*grind))
                            && !self
                                .pending
                                .entries()
                                .get(&cid)
                                .is_some_and(|h| h.evidence.contains_key(*grind))
                    })
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if grinds.is_empty() {
                    continue;
                }
                let key = (carrier.clone(), cid.clone());
                if let Some(job) = self.derivations.get_mut(&key) {
                    job.add_grinds(grinds);
                } else {
                    let block = self
                        .proofs
                        .awaiting
                        .get(&cid)
                        .map(|h| h.entry.block.clone())
                        .or_else(|| self.pending.entries().get(&cid).map(|h| h.block.clone()));
                    self.proofs.start_deriving(cid.clone());
                    self.derivations.insert(
                        key,
                        ChildDeriveJob::new(cid.clone(), block, carrier, grinds, path.to_vec()),
                    );
                }
            }
        }
    }
    /// Bounded dispatch, without discarding queued tail pairs. Local jobs never
    /// accept a public validity flag; run() returns private verifier results.
    pub fn derivation_work(&mut self) -> Vec<ChildDeriveJob> {
        let mut jobs = vec![];
        for _ in 0..self.proof_config.index_checks {
            let Some((_, job)) = self.derivations.pop_first() else {
                break;
            };
            jobs.push(job);
        }
        jobs
    }
    pub fn derivation_completed(
        &mut self,
        result: ChildDerivation,
        now: i64,
    ) -> Result<ChainEffects, StoreError> {
        let (cid, checks) = result.into_checks();
        let mut effects = ChainEffects::default();
        for checked in checks {
            let next = self.apply_checked_proof(checked, now)?;
            effects.sync.extend(next.sync);
            effects.bodies.extend(next.bodies);
            effects.headers.extend(next.headers);
        }
        self.proofs.finish_deriving(&cid);
        Ok(effects)
    }
}
