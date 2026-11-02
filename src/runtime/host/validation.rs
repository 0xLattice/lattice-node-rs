//! Parallel pure checks, ordered actor completion. This deliberately does not
//! move admission or durable writes onto workers, or claim an async actor turn.
const MAX_WORKERS: usize = 4;

pub(super) fn map<T: Send, R: Send>(items: Vec<T>, check: impl Fn(T) -> R + Sync) -> Vec<R> {
    let workers = std::thread::available_parallelism().map_or(1, usize::from);
    map_with_width(items, workers.min(MAX_WORKERS), check)
}

fn map_with_width<T: Send, R: Send>(
    items: Vec<T>,
    width: usize,
    check: impl Fn(T) -> R + Sync,
) -> Vec<R> {
    let width = width.max(1).min(items.len());
    if width <= 1 {
        return items.into_iter().map(check).collect();
    }
    let mut lanes: Vec<Vec<_>> = (0..width).map(|_| Vec::new()).collect();
    for (index, item) in items.into_iter().enumerate() {
        lanes[index % width].push((index, item));
    }
    let mut results = std::thread::scope(|scope| {
        let handles = lanes
            .into_iter()
            .map(|lane| {
                let check = &check;
                scope.spawn(move || {
                    lane.into_iter()
                        .map(|(i, item)| (i, check(item)))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("validation worker panicked"))
            .collect::<Vec<_>>()
    });
    results.sort_unstable_by_key(|(i, _)| *i);
    results.into_iter().map(|(_, result)| result).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn forty_real_proofs_and_corrupt_twin_complete_without_cross_contamination() {
        use crate::core::{child_proofs::*, pending::PendingQueue};
        use lattice_block_tree::ChainTree;
        use lattice_primitives::ChainRuntimeContext;
        let path = vec!["Nexus".into(), "Alpha".into()];
        let tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
        let pending = PendingQueue::default();
        let config = ChildProofConfig {
            index_checks: 64,
            ..Default::default()
        };
        let mut queue = ChildProofSync::default();
        for nonce in 0..40 {
            let (mut entry, _) = missing_tests::fixture(nonce);
            if nonce == 7 {
                let mut bad = entry.proofs[0].clone();
                bad.entries[0].1[0] ^= 1;
                entry.proofs.push(bad);
            }
            assert!(queue.await_header(entry, None, &config, &pending, &tree, None));
        }
        let jobs = queue.work(&path, &config, &pending, &tree, &[]).checks;
        assert_eq!(jobs.len(), 41);
        let completions = map_with_width(jobs, 4, |job| job.run(None));
        let (mut accepted, mut refused) = (0, 0);
        for result in completions {
            match queue.completed(result).unwrap() {
                CheckedProof::Accepted { .. } => accepted += 1,
                CheckedProof::Rejected { source, .. } => {
                    assert!(source.is_none());
                    refused += 1;
                }
            }
        }
        assert_eq!((accepted, refused), (40, 1));
        assert_eq!(queue.checks(true), 0);
        assert_eq!(queue.queued_bytes(), 0);
    }

    #[test]
    fn bounded_parallel_checks_keep_complete_ordered_tail() {
        let barrier = Barrier::new(4);
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let results = map_with_width((0..41).collect(), 4, |item| {
            let count = live.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(count, Ordering::SeqCst);
            if item < 4 {
                barrier.wait();
            }
            live.fetch_sub(1, Ordering::SeqCst);
            item * item
        });
        assert_eq!(peak.load(Ordering::SeqCst), 4);
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(results, (0..41).map(|i| i * i).collect::<Vec<_>>());
        assert!(map_with_width(Vec::<usize>::new(), 4, |i| i).is_empty());
        assert_eq!(map_with_width(vec![7], 0, |i| i), vec![7]);
    }
}
