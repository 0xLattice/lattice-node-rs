//! Verified ephemeral content, charged independently of acquisition sessions.
//! Bodies outside windows go first, then read entries, then furthest bodies.
use lattice_cashew::SerializedVolume;
use std::collections::BTreeMap;
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Body(String),
    Entry(String),
}
struct Held {
    entries: BTreeMap<String, Vec<u8>>,
    bytes: usize,
    read: u64,
}
pub(crate) struct ContentCache {
    held: BTreeMap<Key, Held>,
    distances: BTreeMap<String, usize>,
    bytes: usize,
    limit: usize,
    clock: u64,
}
impl ContentCache {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            held: BTreeMap::new(),
            distances: BTreeMap::new(),
            bytes: 0,
            limit,
            clock: 0,
        }
    }
    pub(crate) fn order(&mut self, distances: BTreeMap<String, usize>) {
        self.distances = distances;
    }
    fn rank(&self, key: &Key, read: u64) -> (u8, usize, u64) {
        match key {
            Key::Entry(_) => (1, 0, u64::MAX - read),
            Key::Body(cid) => match self.distances.get(cid) {
                Some(d) => (0, *d, u64::MAX - read),
                None => (2, 0, u64::MAX - read),
            },
        }
    }
    pub(crate) fn fetch(&mut self, cid: &str) -> Option<Vec<u8>> {
        let key = if self.held.contains_key(&Key::Entry(cid.into())) {
            Key::Entry(cid.into())
        } else {
            self.held
                .iter()
                .find(|(_, v)| v.entries.contains_key(cid))?
                .0
                .clone()
        };
        self.clock = self.clock.saturating_add(1);
        let held = self.held.get_mut(&key).unwrap();
        held.read = self.clock;
        held.entries.get(cid).cloned()
    }
    fn put(&mut self, key: Key, entries: BTreeMap<String, Vec<u8>>, charge: Option<usize>) {
        if let Some(old) = self.held.remove(&key) {
            self.bytes -= old.bytes;
        }
        let Some(bytes) = entries.iter().try_fold(0usize, |n, (cid, b)| {
            n.checked_add(cid.len() + b.len() + 512)
        }) else {
            return;
        };
        let bytes = charge.unwrap_or(bytes);
        if bytes > self.limit {
            return;
        }
        let rank = self.rank(&key, self.clock.saturating_add(1));
        while self.bytes > self.limit - bytes {
            let victim = self
                .held
                .iter()
                .map(|(k, v)| (self.rank(k, v.read), k.clone()))
                .filter(|(r, _)| *r > rank)
                .max();
            let Some((_, victim)) = victim else {
                return;
            };
            self.bytes -= self.held.remove(&victim).unwrap().bytes;
        }
        self.clock = self.clock.saturating_add(1);
        self.bytes += bytes;
        self.held.insert(
            key,
            Held {
                entries,
                bytes,
                read: self.clock,
            },
        );
    }
    pub(crate) fn remember(&mut self, cid: String, bytes: Vec<u8>) {
        self.put(Key::Entry(cid.clone()), [(cid, bytes)].into(), None);
    }
    pub(crate) fn body(&mut self, cid: String, volumes: &[SerializedVolume]) {
        let unique: BTreeMap<_, _> = volumes.iter().map(|v| (&v.root, v)).collect();
        let Some(bytes) = unique
            .values()
            .flat_map(|v| v.entries.iter())
            .try_fold(0usize, |n, (cid, b)| {
                n.checked_add(cid.len() + b.len() + 512)
            })
        else {
            return;
        };
        self.put(
            Key::Body(cid),
            unique.values().flat_map(|v| v.entries.clone()).collect(),
            Some(bytes),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn volume(cid: &str) -> SerializedVolume {
        SerializedVolume::new(cid, BTreeMap::from([(cid.into(), vec![1])]))
    }
    #[test]
    fn window_bodies_outlive_reads_and_outside_bodies_and_insertion_is_whole() {
        let charge = 514;
        let mut cache = ContentCache::new(charge * 2);
        cache.order(BTreeMap::from([("a".into(), 0), ("b".into(), 1)]));
        cache.body("x".into(), &[volume("x")]);
        cache.remember("r".into(), vec![1]);
        cache.body("b".into(), &[volume("b")]);
        assert!(cache.fetch("x").is_none());
        cache.body("a".into(), &[volume("a")]);
        assert!(cache.fetch("r").is_none());
        cache.remember("z".into(), vec![1]);
        assert!(cache.fetch("z").is_none());
        assert!(cache.fetch("a").is_some());
        assert!(cache.fetch("b").is_some());
        cache.body(
            "huge".into(),
            &[SerializedVolume::new(
                "huge",
                BTreeMap::from([("huge".into(), vec![1; 2048])]),
            )],
        );
        assert!(cache.fetch("huge").is_none());
        assert!(cache.bytes <= cache.limit);
    }
}
