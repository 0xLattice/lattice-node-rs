//! Bounded storage jobs. Cancellation retires a running job only after its
//! physical read finishes, preserving Ivy's accounting and storage lifetime.
use crate::storage::NodeStorage;
use ivy_core::ContentEntry;
use ivy_core::core::{ContentJob, ContentJobResult};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::task::JoinSet;

pub(crate) struct ContentJobs {
    storage: Arc<NodeStorage>,
    #[cfg(test)]
    gate: Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>,
    limit: usize,
    queued: VecDeque<(u64, ContentJob)>,
    active: BTreeMap<u64, Arc<AtomicBool>>,
    tasks: JoinSet<(u64, ContentJobResult)>,
    completed: VecDeque<(u64, ContentJobResult)>,
}
fn empty(job: &ContentJob) -> ContentJobResult {
    match job {
        ContentJob::AuthorizeVolume { .. } => ContentJobResult::Authorized(false),
        ContentJob::ReadVolume { .. } => ContentJobResult::Entries(vec![]),
        ContentJob::ReadBundle { .. } => ContentJobResult::Roots(vec![]),
    }
}
fn run(storage: &NodeStorage, job: &ContentJob, cancelled: &AtomicBool) -> ContentJobResult {
    if cancelled.load(Ordering::Acquire) {
        return empty(job);
    }
    let result = match job {
        ContentJob::AuthorizeVolume { .. } => ContentJobResult::Authorized(true),
        ContentJob::ReadVolume {
            root_cid,
            max_data_bytes,
        } => {
            let entries = storage
                .serving_volume(root_cid)
                .ok()
                .flatten()
                .filter(|v| {
                    v.entries
                        .values()
                        .try_fold(0usize, |n, b| n.checked_add(b.len()))
                        .is_some_and(|n| n <= *max_data_bytes)
                })
                .map(|v| {
                    v.entries
                        .into_iter()
                        .map(|(cid, data)| ContentEntry { cid, data })
                        .collect()
                })
                .unwrap_or_default();
            ContentJobResult::Entries(entries)
        }
        ContentJob::ReadBundle { root_cid } => {
            ContentJobResult::Roots(storage.serving_bundle(root_cid).unwrap_or_default())
        }
    };
    if cancelled.load(Ordering::Acquire) {
        empty(job)
    } else {
        result
    }
}
impl ContentJobs {
    pub(crate) fn new(storage: Arc<NodeStorage>, limit: usize) -> Self {
        Self {
            storage,
            #[cfg(test)]
            gate: None,
            limit: limit.max(1),
            queued: VecDeque::new(),
            active: BTreeMap::new(),
            tasks: JoinSet::new(),
            completed: VecDeque::new(),
        }
    }
    pub(crate) fn submit(&mut self, id: u64, job: ContentJob) {
        self.queued.push_back((id, job));
        self.dispatch();
    }
    fn dispatch(&mut self) {
        while self.active.len() < self.limit {
            let Some((id, job)) = self.queued.pop_front() else {
                break;
            };
            let cancelled = Arc::new(AtomicBool::new(false));
            self.active.insert(id, cancelled.clone());
            let storage = self.storage.clone();
            #[cfg(test)]
            let gate = self.gate.clone();
            self.tasks.spawn_blocking(move || {
                #[cfg(test)]
                if let Some(gate) = gate {
                    let (lock, condition) = &*gate;
                    let _guard = condition
                        .wait_while(lock.lock().unwrap(), |released| !*released)
                        .unwrap();
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&storage, &job, &cancelled)
                }))
                .unwrap_or_else(|_| empty(&job));
                (id, result)
            });
        }
    }
    pub(crate) fn cancel(&mut self, id: u64) {
        if let Some(index) = self.queued.iter().position(|(key, _)| *key == id) {
            let (_, job) = self.queued.remove(index).unwrap();
            self.completed.push_back((id, empty(&job)));
        }
        if let Some(flag) = self.active.get(&id) {
            flag.store(true, Ordering::Release);
        }
    }
    /// Idle futures remain pending, so callers may select this unconditionally.
    pub(crate) async fn next(&mut self) -> (u64, ContentJobResult) {
        if let Some(done) = self.completed.pop_front() {
            return done;
        }
        if self.tasks.is_empty() {
            return std::future::pending().await;
        }
        let (id, result) = self
            .tasks
            .join_next()
            .await
            .expect("nonempty jobs")
            .expect("storage panics caught");
        self.active.remove(&id);
        self.dispatch();
        (id, result)
    }
    pub(crate) async fn shutdown(&mut self) {
        self.queued.clear();
        self.completed.clear();
        for flag in self.active.values() {
            flag.store(true, Ordering::Release);
        }
        while self.tasks.join_next().await.is_some() {}
        self.active.clear();
    }
}
impl Drop for ContentJobs {
    fn drop(&mut self) {
        for flag in self.active.values() {
            flag.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn queued_cancellation_and_running_cancellation_preserve_physical_slot() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let mut jobs = ContentJobs::new(storage, 1);
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        jobs.gate = Some(gate.clone());
        let request = || ContentJob::ReadBundle {
            root_cid: "unavailable".into(),
        };
        jobs.submit(1, request());
        jobs.submit(2, request());
        jobs.submit(3, request());
        jobs.cancel(1);
        jobs.cancel(2);
        assert_eq!(jobs.active.len(), 1);
        assert_eq!(jobs.queued.len(), 1);
        let (id, result) = jobs.next().await;
        assert_eq!(id, 2);
        assert!(matches!(result,ContentJobResult::Roots(v) if v.is_empty()));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), jobs.next())
                .await
                .is_err()
        );
        assert!(
            jobs.active.contains_key(&1),
            "cancelled physical read still owns slot"
        );
        assert_eq!(jobs.queued.front().unwrap().0, 3);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let (id, result) = jobs.next().await;
        assert_eq!(id, 1);
        assert!(matches!(result,ContentJobResult::Roots(v) if v.is_empty()));
        let (id, result) = jobs.next().await;
        assert_eq!(id, 3);
        assert!(matches!(result,ContentJobResult::Roots(v) if v == vec!["unavailable"]));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), jobs.next())
                .await
                .is_err()
        );
        jobs.shutdown().await;
    }
    #[tokio::test]
    async fn real_storage_jobs_respect_archive_limit_and_shutdown_drops_queue() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "root", "test").unwrap());
        let bytes = b"checked content".to_vec();
        let cid = lattice_cashew::cid_for_bytes(0x55, &bytes);
        let volume = lattice_cashew::SerializedVolume::new(
            cid.clone(),
            BTreeMap::from([(cid.clone(), bytes.clone())]),
        );
        storage.cache_volumes("test", &[volume]).unwrap();
        let mut jobs = ContentJobs::new(storage, 1);
        jobs.submit(
            1,
            ContentJob::ReadVolume {
                root_cid: cid.clone(),
                max_data_bytes: bytes.len() - 1,
            },
        );
        let (_, result) = jobs.next().await;
        assert!(matches!(result,ContentJobResult::Entries(v) if v.is_empty()));
        jobs.submit(
            2,
            ContentJob::ReadVolume {
                root_cid: cid.clone(),
                max_data_bytes: bytes.len(),
            },
        );
        let (_, result) = jobs.next().await;
        assert!(
            matches!(result,ContentJobResult::Entries(v) if v.len()==1 && v[0].cid==cid && v[0].data==bytes)
        );
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        jobs.gate = Some(gate.clone());
        jobs.submit(
            3,
            ContentJob::ReadBundle {
                root_cid: "active".into(),
            },
        );
        jobs.submit(
            4,
            ContentJob::ReadBundle {
                root_cid: "queued".into(),
            },
        );
        let stop = tokio::spawn(async move {
            jobs.shutdown().await;
            assert!(jobs.active.is_empty());
            assert!(jobs.queued.is_empty());
        });
        tokio::task::yield_now().await;
        assert!(!stop.is_finished());
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(1), stop)
            .await
            .unwrap()
            .unwrap();
    }
}
