//! Shared bounded mining CPU jobs; actor retains all admission authority.
use super::*;
pub(super) fn retain_epoch(queue: &mut VecDeque<Work>, epoch: u64) {
    queue.retain(|work| match work {
        Work::Mining(effect) => match effect.as_ref() {
            MiningEffect::Preflight(job) => job.tip_epoch == epoch,
            MiningEffect::BuildTemplate(job) => job.tip_epoch == epoch,
            _ => true,
        },
        _ => true,
    });
}
pub(super) enum Completion {
    Root(RootPreflightCompletion),
    Child(Vec<String>, ChildPreflightCompletion),
    Template(Box<templates::TemplateCompletion>),
    Connection(connections::ConnectionCompletion),
    ChildConnection(Box<child_connections::Completion>),
}
const MAX_WORKERS: usize = 4;
pub(super) struct Preflights {
    jobs: tokio::task::JoinSet<Result<Completion, crate::storage::StoreError>>,
    width: usize,
}
impl Default for Preflights {
    fn default() -> Self {
        Self {
            jobs: tokio::task::JoinSet::new(),
            width: std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(MAX_WORKERS),
        }
    }
}
impl Preflights {
    pub(super) fn full(&self) -> bool {
        self.jobs.len() >= self.width
    }
    pub(super) fn active(&self) -> bool {
        !self.jobs.is_empty()
    }
    pub(super) fn start(&mut self, task: RootPreflightTask) {
        self.spawn(move || task.run(now()).map(Completion::Root));
    }
    pub(super) fn start_child(&mut self, path: Vec<String>, task: ChildPreflightTask) {
        self.spawn(move || task.run(now()).map(|done| Completion::Child(path, done)));
    }
    pub(super) fn start_template(&mut self, task: templates::TemplateTask) {
        self.spawn(move || Ok(Completion::Template(Box::new(task.run()))));
    }
    pub(super) fn start_child_connection(&mut self, task: child_connections::Task) {
        self.spawn(move || {
            task.run()
                .map(|done| Completion::ChildConnection(Box::new(done)))
        });
    }
    pub(super) fn start_connection(&mut self, task: connections::ConnectionTask) {
        self.spawn(move || task.run().map(Completion::Connection));
    }
    pub(super) fn spawn(
        &mut self,
        run: impl FnOnce() -> Result<Completion, crate::storage::StoreError> + Send + 'static,
    ) {
        assert!(!self.full());
        // Aborting a JoinSet cannot stop a running blocking task. The task,
        // rather than its join handle, owns the snapshot and directory lease,
        // so cancellation/panic cannot release content under a live reader.
        self.jobs.spawn_blocking(run);
    }
    pub(super) async fn next(&mut self) -> anyhow::Result<Completion> {
        Ok(self.jobs.join_next().await.expect("active preflight")??)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deferred_jobs_are_filtered_after_later_same_drain_tip_moves() {
        let job = |epoch| {
            Work::Mining(Box::new(MiningEffect::Preflight(PreflightJob {
                cid: "transaction".into(),
                transaction: Transaction::new(
                    BTreeMap::new(),
                    lattice_cashew::Header::new(lattice_primitives::TransactionBody {
                        account_actions: vec![],
                        actions: vec![],
                        deposit_actions: vec![],
                        receipt_actions: vec![],
                        withdrawal_actions: vec![],
                        signers: vec![],
                        nonce: 0,
                        chain_path: vec!["Nexus".into()],
                    })
                    .unwrap(),
                ),
                tip_cid: "A".into(),
                tip_epoch: epoch,
                pool_version: 0,
                checks_policies: true,
            })))
        };
        // An earlier local deferred queue is invisible to enqueue's pruning.
        // Even returning to the same CID must not revive its older epoch.
        let mut deferred = VecDeque::from([
            job(1),
            Work::OperatorFetch(7, "content".into()),
            job(2),
            job(3),
        ]);
        retain_epoch(&mut deferred, 3);
        assert_eq!(deferred.len(), 2);
        assert!(matches!(
            deferred.pop_front(),
            Some(Work::OperatorFetch(7, _))
        ));
        assert!(
            matches!(deferred.pop_front(), Some(Work::Mining(effect)) if matches!(*effect, MiningEffect::Preflight(PreflightJob { tip_epoch: 3, .. })))
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn full_workers_leave_actor_runtime_responsive_and_hold_leases_until_exit() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Lease(Arc<AtomicUsize>);
        impl Drop for Lease {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let live = Arc::new(AtomicUsize::new(0));
        let mut workers = Preflights {
            jobs: tokio::task::JoinSet::new(),
            width: 2,
        };
        let mut releases = vec![];
        let (started_tx, mut started) = tokio::sync::mpsc::unbounded_channel();
        for _ in 0..2 {
            let (release, wait) = std::sync::mpsc::channel();
            releases.push(release);
            let live = live.clone();
            let started = started_tx.clone();
            workers.spawn(move || {
                live.fetch_add(1, Ordering::SeqCst);
                let _lease = Lease(live);
                started.send(()).unwrap();
                wait.recv().unwrap();
                Err(crate::storage::StoreError::InvalidConfiguration(
                    "test worker".into(),
                ))
            });
        }
        for _ in 0..2 {
            started.recv().await.unwrap();
        }
        assert!(workers.full());
        assert_eq!(live.load(Ordering::SeqCst), 2);
        // The single runtime/actor thread can process another event even while
        // both blocking workers are deliberately held indefinitely.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        })
        .await
        .unwrap();
        drop(workers);
        assert_eq!(live.load(Ordering::SeqCst), 2);
        for release in releases {
            release.send(()).unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
