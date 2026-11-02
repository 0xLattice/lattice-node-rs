pub use crate::storage::StreamCursor;
use lattice_block_tree::ChildBlockProof;
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader, ChainSpec};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId {
    pub key: String,
    pub session: u64,
}
impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.key, self.session)
    }
}
#[derive(Debug, Clone)]
pub struct HeaderEntry {
    pub block: Block,
    pub children: Option<FlatDictionary<BlockHeader>>,
    pub proofs: Vec<ChildBlockProof>,
    pub spec: Option<ChainSpec>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogKind {
    Header,
    Proof,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogEntry {
    pub kind: LogKind,
    pub cid: String,
    pub block: String,
}
impl LogEntry {
    pub fn header(cid: impl Into<String>) -> Self {
        let cid = cid.into();
        Self {
            kind: LogKind::Header,
            block: cid.clone(),
            cid,
        }
    }
    pub fn proof(cid: impl Into<String>, block: impl Into<String>) -> Self {
        Self {
            kind: LogKind::Proof,
            cid: cid.into(),
            block: block.into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub position: u64,
    pub entry: LogEntry,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPage {
    pub request_id: u64,
    pub log_id: String,
    pub entries: Vec<StreamEntry>,
    pub has_more: bool,
}
#[derive(Debug, Clone)]
pub struct HeadersResponse {
    pub request_id: u64,
    pub entries: Vec<HeaderEntry>,
    pub has_more: bool,
}
/// Decoded protocol; wire encoding remains the shell's responsibility.
#[derive(Debug, Clone)]
pub enum SyncMessage {
    GetStream {
        request_id: u64,
        log_id: Option<String>,
        after: u64,
        own: String,
    },
    Stream(StreamPage),
    GetData {
        request_id: u64,
        cids: Vec<String>,
    },
    GetAncestors {
        request_id: u64,
        cid: String,
        max: usize,
    },
    Headers(HeadersResponse),
}
/// Append-only verifiable-object log. Unlike the Swift bitset implementation,
/// this first Rust port retains deduplication IDs by value (same semantics).
#[derive(Debug, Clone)]
pub struct WeighLog {
    pub id: String,
    entries: Vec<LogEntry>,
    members: BTreeSet<LogEntry>,
}
impl WeighLog {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            entries: vec![],
            members: BTreeSet::new(),
        }
    }
    pub fn restoring(
        id: impl Into<String>,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> Result<Self, &'static str> {
        let mut log = Self::new(id);
        for entry in entries {
            log.append(entry)?;
        }
        Ok(log)
    }
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }
    pub fn count(&self) -> u64 {
        self.entries.len() as u64
    }
    pub fn contains(&self, entry: &LogEntry) -> bool {
        self.members.contains(entry)
    }
    pub fn append(&mut self, entry: LogEntry) -> Result<bool, &'static str> {
        if entry.kind == LogKind::Header && entry.cid != entry.block {
            return Err("header entry must name its own block");
        }
        if !self.members.insert(entry.clone()) {
            return Ok(false);
        }
        self.entries.push(entry);
        Ok(true)
    }
    /// The first entry always fits, even if it exceeds the byte budget.
    pub fn page(&self, after: u64, limit: usize, bytes: usize) -> (Vec<StreamEntry>, bool) {
        if after >= self.count() {
            return (vec![], false);
        }
        let mut page = vec![];
        let mut total = 0usize;
        let mut next = after as usize;
        while next < self.entries.len() && page.len() < limit {
            let entry = &self.entries[next];
            total = total
                .saturating_add(entry.cid.len())
                .saturating_add(entry.block.len())
                .saturating_add(16);
            if !page.is_empty() && total > bytes {
                break;
            }
            page.push(StreamEntry {
                position: next as u64 + 1,
                entry: entry.clone(),
            });
            next += 1;
        }
        (page, next < self.entries.len())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightStream {
    pub request_id: u64,
    pub deadline: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightData {
    pub request_id: u64,
    pub cids: Vec<String>,
    pub deadline: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightHeader {
    pub request_id: u64,
    pub cid: String,
    pub deadline: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightFetch {
    pub cid: String,
    pub deadline: i64,
}
#[derive(Debug, Clone)]
pub struct PeerSync {
    pub stream: Option<InFlightStream>,
    pub more: bool,
    pub taken: u64,
    pub holes: Vec<StreamEntry>,
    pub requested: u64,
    pub data: Option<InFlightData>,
    pub asked_at: i64,
    pub subscribed: bool,
    pub parent_request: Option<InFlightHeader>,
    pub child_index: Option<InFlightFetch>,
    pub serving: Option<u64>,
    pub queued: VecDeque<SyncMessage>,
}
impl Default for PeerSync {
    fn default() -> Self {
        Self {
            stream: None,
            more: false,
            taken: 0,
            holes: vec![],
            requested: 0,
            data: None,
            asked_at: i64::MIN,
            subscribed: false,
            parent_request: None,
            child_index: None,
            serving: None,
            queued: VecDeque::new(),
        }
    }
}
impl PeerSync {
    pub fn deadlines(&self) -> Vec<i64> {
        [
            self.stream.as_ref().map(|v| v.deadline),
            self.data.as_ref().map(|v| v.deadline),
            self.parent_request.as_ref().map(|v| v.deadline),
            self.child_index.as_ref().map(|v| v.deadline),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}
#[derive(Debug, Clone)]
pub struct DeclinedSet {
    capacity: usize,
    members: BTreeSet<String>,
    order: VecDeque<String>,
}
impl Default for DeclinedSet {
    fn default() -> Self {
        Self::new(100_000)
    }
}
impl DeclinedSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            members: BTreeSet::new(),
            order: VecDeque::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
    pub fn contains(&self, cid: &str) -> bool {
        self.members.contains(cid)
    }
    pub fn insert(&mut self, cid: String) {
        if !self.members.insert(cid.clone()) {
            return;
        }
        self.order.push_back(cid);
        if self.members.len() > self.capacity {
            self.members.remove(&self.order.pop_front().unwrap());
        }
    }
}
pub fn next_deadline(
    peers: &BTreeMap<PeerId, PeerSync>,
    held: Option<i64>,
    now: i64,
) -> Option<i64> {
    peers
        .values()
        .flat_map(PeerSync::deadlines)
        .chain(held)
        .filter(|t| *t > now && *t != i64::MAX)
        .min()
}
