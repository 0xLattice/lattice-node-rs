//! Advisory per-chain memory of addresses reached by authenticated direct dials.
use ivy_core::PeerEndpoint;
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
pub type ChainPath = Vec<String>;
pub const CAPACITY: usize = 256;
pub const CAPACITY_PER_GROUP: usize = 8;
pub const HORIZON: i64 = 30 * 24 * 60 * 60 * 1000;
pub const MAX_FAILURES: i64 = 10;
pub const MAX_FILE_BYTES: u64 = 256 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub public_key: String,
    pub host: String,
    pub port: u16,
    pub last_seen: i64,
    pub failures: i64,
}
impl Entry {
    pub fn endpoint(&self) -> PeerEndpoint {
        PeerEndpoint {
            public_key: self.public_key.clone(),
            host: self.host.clone(),
            port: self.port,
        }
    }
    pub fn acceptable(&self) -> bool {
        acceptable_key(&self.public_key)
            && self.port != 0
            && !self.host.is_empty()
            && self.host.len() <= 253
            && self.host != "unknown"
            && self
                .host
                .bytes()
                .all(|b| b.is_ascii() && b > 0x20 && b != 0x7f)
            && (0..MAX_FAILURES).contains(&self.failures)
    }
}
// Swift Crypto's public-key constructor accepts any 32-byte encoding here.
// This advisory input check grants no authentication; actual dials still verify
// identities/signatures through Ivy. Preserve ed01/hex aliases at this boundary.
fn acceptable_key(key: &str) -> bool {
    let raw = if key.len() == 68 && key.get(..4).is_some_and(|p| p.eq_ignore_ascii_case("ed01")) {
        &key[4..]
    } else {
        key
    };
    raw.len() == 64
        && raw.is_ascii()
        && (0..32).all(|i| u8::from_str_radix(&raw[i * 2..i * 2 + 2], 16).is_ok())
}
pub fn group(host: &str) -> String {
    let parts: Vec<_> = host.split('.').collect();
    if parts.len() == 4
        && parts
            .iter()
            .all(|p| p.parse::<i16>().is_ok_and(|n| (0..=255).contains(&n)))
    {
        format!("{}.{}", parts[0], parts[1])
    } else {
        host.to_lowercase()
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerBook {
    pub entries: BTreeMap<String, Entry>,
}
impl PeerBook {
    pub fn from_entries(entries: impl IntoIterator<Item = Entry>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .filter(Entry::acceptable)
                .map(|e| (e.public_key.clone(), e))
                .collect(),
        }
    }
    pub fn reached(&mut self, endpoints: &[PeerEndpoint], excluded: &BTreeSet<String>, now: i64) {
        for e in endpoints
            .iter()
            .filter(|e| !excluded.contains(&e.public_key))
        {
            let entry = Entry {
                public_key: e.public_key.clone(),
                host: e.host.clone(),
                port: e.port,
                last_seen: now,
                failures: 0,
            };
            if entry.acceptable() {
                self.entries.insert(entry.public_key.clone(), entry);
            }
        }
        self.prune(now);
    }
    pub fn still_open(&mut self, keys: &BTreeSet<String>, now: i64) {
        for key in keys {
            if let Some(entry) = self.entries.get_mut(key) {
                entry.last_seen = now;
                entry.failures = 0;
            }
        }
        self.prune(now);
    }
    pub fn failed(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.failures += 1;
            if entry.failures >= MAX_FAILURES {
                self.entries.remove(key);
            }
        }
    }
    pub fn prune(&mut self, now: i64) {
        let mut newest: Vec<_> = self
            .entries
            .values()
            .filter(|e| (now as i128) - (e.last_seen as i128) <= HORIZON as i128)
            .cloned()
            .collect();
        newest.sort_by(|a, b| (b.last_seen, &b.public_key).cmp(&(a.last_seen, &a.public_key)));
        let mut groups = BTreeMap::<String, usize>::new();
        self.entries.clear();
        for entry in newest {
            if self.entries.len() >= CAPACITY {
                break;
            }
            let count = groups.entry(group(&entry.host)).or_default();
            if *count < CAPACITY_PER_GROUP {
                *count += 1;
                self.entries.insert(entry.public_key.clone(), entry);
            }
        }
    }
    pub fn candidates(&self, excluded: &BTreeSet<String>, count: usize) -> Vec<PeerEndpoint> {
        let mut items: Vec<_> = self
            .entries
            .values()
            .filter(|e| !excluded.contains(&e.public_key))
            .map(Entry::endpoint)
            .collect();
        shuffle(&mut items);
        items.truncate(count);
        items
    }
}
pub(crate) fn shuffle<T>(items: &mut [T]) {
    let mut rng = rand_core::OsRng;
    for i in (1..items.len()).rev() {
        let n = (i + 1) as u64;
        let limit = u64::MAX - u64::MAX % n;
        let mut draw = rng.next_u64();
        while draw >= limit {
            draw = rng.next_u64();
        }
        items.swap(i, (draw % n) as usize);
    }
}
#[derive(Deserialize)]
struct FileV1 {
    version: i64,
    peers: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
struct FileV2 {
    version: i64,
    chains: BTreeMap<String, Vec<Entry>>,
}
pub struct PeerMemory {
    file: PathBuf,
    excluded: BTreeSet<String>,
    books: BTreeMap<ChainPath, PeerBook>,
    written: BTreeMap<ChainPath, PeerBook>,
    dialled: BTreeMap<String, PeerEndpoint>,
}
impl PeerMemory {
    pub fn open(
        directory: &Path,
        chains: &[ChainPath],
        excluded: BTreeSet<String>,
        now: i64,
    ) -> Self {
        let file = directory.join("peers.json");
        let mut books = Self::load(&file, chains);
        for book in books.values_mut() {
            book.prune(now);
        }
        Self {
            file,
            excluded,
            written: books.clone(),
            books,
            dialled: BTreeMap::new(),
        }
    }
    pub fn track(&mut self, path: ChainPath, now: i64) {
        if self.books.contains_key(&path) {
            return;
        }
        let mut paths: Vec<_> = self.books.keys().cloned().collect();
        paths.push(path.clone());
        let mut book = Self::load(&self.file, &paths)
            .remove(&path)
            .unwrap_or_default();
        book.prune(now);
        self.written.insert(path.clone(), book.clone());
        self.books.insert(path, book);
    }
    pub fn remembered(&self, path: &[String]) -> Vec<PeerEndpoint> {
        self.books
            .get(path)
            .map(|b| b.entries.values().map(Entry::endpoint).collect())
            .unwrap_or_default()
    }
    pub fn candidates(
        &self,
        path: &[String],
        connected: &BTreeSet<String>,
        count: usize,
    ) -> Vec<PeerEndpoint> {
        self.books
            .get(path)
            .map(|book| book.candidates(&connected.union(&self.excluded).cloned().collect(), count))
            .unwrap_or_default()
    }
    pub fn reached(&mut self, endpoint: PeerEndpoint, paths: &[ChainPath], now: i64) {
        self.dialled
            .insert(endpoint.public_key.clone(), endpoint.clone());
        for path in paths {
            if let Some(book) = self.books.get_mut(path) {
                book.reached(std::slice::from_ref(&endpoint), &self.excluded, now);
            }
        }
    }
    pub fn failed(&mut self, key: &str) {
        for book in self.books.values_mut() {
            book.failed(key);
        }
    }
    pub fn still_open(&mut self, current: &BTreeMap<ChainPath, BTreeSet<String>>, now: i64) {
        let open: BTreeSet<_> = current.values().flatten().cloned().collect();
        for (path, keys) in current {
            if let Some(book) = self.books.get_mut(path) {
                let endpoints: Vec<_> = keys
                    .iter()
                    .filter_map(|key| self.dialled.get(key).cloned())
                    .collect();
                book.reached(&endpoints, &self.excluded, now);
                book.still_open(keys, now);
            }
        }
        self.dialled.retain(|key, _| open.contains(key));
        self.save();
    }
    fn save(&mut self) {
        if self.books == self.written {
            return;
        }
        let data = FileV2 {
            version: 2,
            chains: self
                .books
                .iter()
                .map(|(path, book)| (path.join("/"), book.entries.values().cloned().collect()))
                .collect(),
        };
        let Ok(bytes) = serde_json::to_vec(&data) else {
            return;
        };
        let temporary = self.file.with_extension(format!(
            "json.{}.{}.tmp",
            std::process::id(),
            rand_core::OsRng.next_u64()
        ));
        let result = (|| -> std::io::Result<()> {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut out = options.open(&temporary)?;
            out.write_all(&bytes)?;
            out.sync_all()?;
            fs::rename(&temporary, &self.file)?;
            Ok(())
        })();
        if result.is_ok() {
            self.written = self.books.clone();
        } else {
            let _ = fs::remove_file(temporary);
        }
    }
    fn load(file: &Path, paths: &[ChainPath]) -> BTreeMap<ChainPath, PeerBook> {
        let mut books: BTreeMap<_, _> = paths
            .iter()
            .cloned()
            .map(|p| (p, PeerBook::default()))
            .collect();
        let max = MAX_FILE_BYTES.saturating_mul(paths.len().max(1) as u64);
        let Ok(file) = fs::File::open(file) else {
            return books;
        };
        if !file.metadata().is_ok_and(|m| m.len() <= max) {
            return books;
        }
        let mut bytes = vec![];
        if file
            .take(max.saturating_add(1))
            .read_to_end(&mut bytes)
            .is_err()
            || bytes.len() as u64 > max
        {
            return books;
        }
        if let Ok(data) = serde_json::from_slice::<FileV2>(&bytes) {
            if data.version == 2 {
                for path in paths {
                    if let Some(entries) = data.chains.get(&path.join("/")) {
                        books.insert(
                            path.clone(),
                            PeerBook::from_entries(entries.iter().take(CAPACITY * 4).cloned()),
                        );
                    }
                }
            }
        } else if let Ok(data) = serde_json::from_slice::<FileV1>(&bytes)
            && data.version == 1
            && let Some(path) = paths.first()
        {
            books.insert(
                path.clone(),
                PeerBook::from_entries(data.peers.into_iter().take(CAPACITY * 4)),
            );
        }
        books
    }
}
