//! Per-level stalled and low-peer search clocks from Swift PeerSearch.swift.
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default)]
pub struct PeerSearch {
    high_water: BTreeMap<Vec<String>, u64>,
    progressed: BTreeMap<Vec<String>, i64>,
    searched: BTreeMap<Vec<String>, i64>,
    topped_up: BTreeMap<Vec<String>, i64>,
}
fn elapsed(now: i64, then: i64, interval: i64) -> bool {
    (now as i128) - (then as i128) >= interval as i128
}
impl PeerSearch {
    pub fn due(
        &mut self,
        heights: &[(Vec<String>, u64)],
        now: i64,
        interval: i64,
        connected: bool,
    ) -> Vec<Vec<String>> {
        let mut due = vec![];
        for (path, height) in heights {
            if self.high_water.get(path).is_some_and(|old| height > old) {
                self.progressed.insert(path.clone(), now);
            }
            self.high_water
                .entry(path.clone())
                .and_modify(|old| *old = (*old).max(*height))
                .or_insert(*height);
            if interval <= 0 || !connected {
                continue;
            }
            let stalled = self
                .progressed
                .get(path)
                .is_none_or(|last| elapsed(now, *last, interval));
            let rested = elapsed(
                now,
                *self.searched.get(path).unwrap_or(&(i64::MIN / 2)),
                interval,
            );
            if stalled && rested {
                self.searched.insert(path.clone(), now);
                due.push(path.clone());
            }
        }
        due
    }
    pub fn top_ups(
        &mut self,
        paths: &[Vec<String>],
        now: i64,
        interval: i64,
        connected: bool,
    ) -> BTreeSet<Vec<String>> {
        if interval <= 0 || !connected {
            return BTreeSet::new();
        }
        paths
            .iter()
            .filter(|path| {
                elapsed(
                    now,
                    *self.topped_up.get(*path).unwrap_or(&(i64::MIN / 2)),
                    interval,
                )
            })
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .inspect(|path| {
                self.topped_up.insert(path.clone(), now);
            })
            .collect()
    }
    /// Runtime configuration bounds the clock maps by the current hosted set.
    pub fn retain(&mut self, paths: &BTreeSet<Vec<String>>) {
        self.high_water.retain(|p, _| paths.contains(p));
        self.progressed.retain(|p, _| paths.contains(p));
        self.searched.retain(|p, _| paths.contains(p));
        self.topped_up.retain(|p, _| paths.contains(p));
    }
}
pub fn rendezvous(nexus: &str, path: &[String]) -> String {
    format!("lattice.chain-peers.v1:{nexus}:{}", path.join("/"))
}
