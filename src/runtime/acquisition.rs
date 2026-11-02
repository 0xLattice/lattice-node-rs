//! Shared content-session reservations. Waiting never blocks the host actor.
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
pub const SESSION_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Debug)]
pub struct ResourcePolicy {
    pub serving_max_concurrent: usize,
    pub serving_max_in_flight_volume_bytes: usize,
    pub max_acquisition_bytes: usize,
}
impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            serving_max_concurrent: 64,
            serving_max_in_flight_volume_bytes: ivy_core::core::CoreSettings::default()
                .max_in_flight_volume_bytes,
            max_acquisition_bytes: 4 * SESSION_BYTES,
        }
    }
}
impl ResourcePolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.serving_max_concurrent > 0,
            "servingMaxConcurrent must be positive"
        );
        anyhow::ensure!(
            self.serving_max_in_flight_volume_bytes > 0,
            "servingMaxInFlightVolumeBytes must be positive"
        );
        anyhow::ensure!(
            self.max_acquisition_bytes >= SESSION_BYTES,
            "maxAcquisitionBytes must fit one 64 MiB content session"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Priority {
    OwnWork,
    Body { cid: String, depth: usize },
}
#[derive(Debug)]
struct Shared {
    capacity: usize,
    used: Mutex<usize>,
    changed: Arc<Notify>,
}
/// Not cloneable: exactly one owner releases each granted reservation.
#[derive(Debug)]
pub struct Reservation {
    bytes: usize,
    shared: Arc<Shared>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut used = self.shared.used.lock().unwrap();
        *used = used
            .checked_sub(self.bytes)
            .expect("reservation accounting");
        drop(used);
        self.shared.changed.notify_one();
    }
}
struct Waiting<T> {
    bytes: usize,
    priority: Priority,
    value: T,
}
pub(crate) struct Queue<T> {
    shared: Arc<Shared>,
    waiting: VecDeque<Waiting<T>>,
    next: BTreeSet<String>,
}
impl<T> Queue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                capacity,
                used: Mutex::new(0),
                changed: Arc::new(Notify::new()),
            }),
            waiting: VecDeque::new(),
            next: BTreeSet::new(),
        }
    }
    pub fn push(&mut self, bytes: usize, priority: Priority, value: T) {
        assert!(
            bytes <= self.shared.capacity,
            "session must fit validated budget"
        );
        self.waiting.push_back(Waiting {
            bytes,
            priority,
            value,
        });
    }
    pub fn prioritize(&mut self, next: BTreeSet<String>) {
        self.next = next;
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        self.waiting.retain(|row| keep(&row.value));
    }
    pub fn wakeup(&self) -> Arc<Notify> {
        self.shared.changed.clone()
    }
    pub fn pop(&mut self) -> Option<(T, Reservation)> {
        let index = self
            .waiting
            .iter()
            .enumerate()
            .min_by_key(|(index, row)| {
                let (depth, lookahead) = match &row.priority {
                    Priority::OwnWork => (0, 0),
                    Priority::Body { cid, depth } => {
                        (*depth, usize::from(!self.next.contains(cid)))
                    }
                };
                (depth, lookahead, *index)
            })
            .map(|(index, _)| index)?;
        let mut used = self.shared.used.lock().unwrap();
        let row = &self.waiting[index];
        if row.bytes > self.shared.capacity - *used {
            return None;
        }
        let row = self.waiting.remove(index).unwrap();
        *used += row.bytes;
        Some((
            row.value,
            Reservation {
                bytes: row.bytes,
                shared: self.shared.clone(),
            },
        ))
    }
    #[cfg(test)]
    pub fn used(&self) -> usize {
        *self.shared.used.lock().unwrap()
    }
    #[cfg(test)]
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn body(cid: &str, depth: usize) -> Priority {
        Priority::Body {
            cid: cid.into(),
            depth,
        }
    }
    #[test]
    fn depth_frontier_fifo_and_head_of_line_match_swift_budget() {
        let mut queue = Queue::new(10);
        queue.push(6, Priority::OwnWork, "active");
        let (_, active) = queue.pop().unwrap();
        queue.push(5, body("child", 1), "child");
        queue.push(5, body("later", 0), "lookahead");
        queue.push(5, body("next", 0), "next");
        queue.push(1, Priority::OwnWork, "own");
        queue.prioritize(["next".into()].into());
        assert!(
            queue.pop().is_none(),
            "first next request does not fit; later small own request must wait"
        );
        drop(active);
        let (name, first) = queue.pop().unwrap();
        assert_eq!(name, "next");
        let (name, second) = queue.pop().unwrap();
        assert_eq!(name, "own");
        assert!(queue.pop().is_none());
        drop(first);
        drop(second);
        let (name, first) = queue.pop().unwrap();
        assert_eq!(name, "lookahead");
        let (name, second) = queue.pop().unwrap();
        assert_eq!(name, "child");
        assert_eq!(queue.used(), 10);
        drop(first);
        drop(second);
        assert_eq!(queue.used(), 0);
    }
    #[tokio::test]
    async fn cancellation_and_release_wake_waiters_without_partial_reservations() {
        let mut queue = Queue::new(8);
        queue.push(8, Priority::OwnWork, 1);
        let (_, lease) = queue.pop().unwrap();
        queue.push(8, Priority::OwnWork, 2);
        queue.push(8, Priority::OwnWork, 3);
        queue.retain(|id| *id != 2);
        assert_eq!(queue.waiting(), 1);
        assert_eq!(queue.used(), 8);
        let wake = queue.wakeup();
        drop(lease);
        wake.notified().await;
        let (id, lease) = queue.pop().unwrap();
        assert_eq!(id, 3);
        assert_eq!(queue.used(), 8);
        drop(queue);
        drop(lease);
    }
}
