//! Session-fenced stream replication. Cursor updates are returned with each
//! turn so the host persists them with admitted facts before publishing effects.
use super::sync::*;
use lattice_block_tree::{BlockImportBatch, ChainFact, ChainTree};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub max_headers: usize,
    pub max_page_bytes: usize,
    pub timeout: i64,
}
impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_headers: 2000,
            max_page_bytes: 1 << 20,
            timeout: 30_000,
        }
    }
}
#[derive(Debug, Clone)]
pub enum SyncEffect {
    Send(PeerId, SyncMessage),
    ServeHeaders {
        peer: PeerId,
        token: u64,
        request_id: u64,
        cids: Vec<String>,
        has_more: bool,
    },
    DisconnectStalled(PeerId),
}
#[derive(Debug, Clone, Default)]
pub struct SyncTurn {
    pub effects: Vec<SyncEffect>,
    pub cursors: BTreeMap<String, StreamCursor>,
}
#[derive(Debug, Clone)]
pub struct HeaderSync {
    pub peers: BTreeMap<PeerId, PeerSync>,
    pub log: WeighLog,
    pub cursors: BTreeMap<String, StreamCursor>,
    known_cursors: BTreeSet<String>,
    next_request: u64,
    next_token: u64,
    pub config: StreamConfig,
}
#[derive(Debug)]
pub struct HeaderResponseWork {
    pub entries: Vec<HeaderEntry>,
    pub asked_parent: Option<String>,
    data: Option<InFlightData>,
}
impl HeaderSync {
    pub fn new(
        log: WeighLog,
        cursors: BTreeMap<String, StreamCursor>,
        config: StreamConfig,
    ) -> Self {
        Self {
            peers: BTreeMap::new(),
            log,
            known_cursors: cursors.keys().cloned().collect(),
            cursors,
            next_request: 1,
            next_token: 1,
            config,
        }
    }
    fn request_id(&mut self) -> Option<u64> {
        let id = self.next_request;
        self.next_request = self.next_request.checked_add(1)?;
        Some(id)
    }
    pub fn peer_ready(&mut self, peer: PeerId, now: i64) -> SyncTurn {
        let mut turn = SyncTurn::default();
        if self.peers.contains_key(&peer) {
            return turn;
        }
        self.peers.insert(
            peer.clone(),
            PeerSync {
                taken: self.cursors.get(&peer.key).map(|v| v.position).unwrap_or(0),
                ..Default::default()
            },
        );
        self.request_stream(&peer, now, &mut turn);
        turn
    }
    pub fn drop_peer(&mut self, peer: &PeerId) {
        if self.peers.remove(peer).is_none() {
            return;
        }
        if !self.known_cursors.contains(&peer.key) && !self.peers.keys().any(|p| p.key == peer.key)
        {
            self.cursors.remove(&peer.key);
        }
    }
    fn request_stream(&mut self, peer: &PeerId, now: i64, turn: &mut SyncTurn) {
        let Some(state) = self.peers.get(peer).filter(|p| p.stream.is_none()) else {
            return;
        };
        let taken = state.taken;
        let Some(id) = self.request_id() else { return };
        let state = self.peers.get_mut(peer).unwrap();
        state.stream = Some(InFlightStream {
            request_id: id,
            deadline: now.saturating_add(self.config.timeout),
        });
        state.more = false;
        turn.effects.push(SyncEffect::Send(
            peer.clone(),
            SyncMessage::GetStream {
                request_id: id,
                log_id: self.cursors.get(&peer.key).map(|v| v.log_id.clone()),
                after: taken,
                own: self.log.id.clone(),
            },
        ));
    }
    pub fn receive_page(
        &mut self,
        peer: &PeerId,
        page: StreamPage,
        now: i64,
        applied: impl Fn(&LogEntry) -> bool,
        lacks: impl Fn(&LogEntry) -> bool,
    ) -> SyncTurn {
        let mut turn = SyncTurn::default();
        let Some(state) = self.peers.get_mut(peer) else {
            return turn;
        };
        let reset = self
            .cursors
            .get(&peer.key)
            .is_none_or(|c| c.log_id != page.log_id);
        if page.request_id != 0 {
            if state.stream.as_ref().map(|r| r.request_id) != Some(page.request_id) {
                return turn;
            }
            state.stream = None;
            state.more = page.has_more;
        } else if !reset && state.stream.is_some() {
            return turn;
        } else if page.has_more {
            state.more = true;
        }
        if reset {
            let cursor = StreamCursor {
                log_id: page.log_id,
                position: 0,
            };
            self.cursors.insert(peer.key.clone(), cursor.clone());
            turn.cursors.insert(peer.key.clone(), cursor);
            state.taken = 0;
            state.holes.clear();
            state.requested = 0;
            state.data = None;
        }
        for item in page.entries {
            if item.position <= state.taken {
                continue;
            }
            if state.taken.checked_add(1) != Some(item.position)
                || state.holes.len() >= self.config.max_headers
            {
                state.more = true;
                break;
            }
            state.taken = item.position;
            if !applied(&item.entry) {
                state.holes.push(item);
            }
        }
        self.advance_cursor(peer, applied, &mut turn);
        self.pump(peer, now, false, lacks, &mut turn);
        turn
    }
    pub fn advance_cursor(
        &mut self,
        peer: &PeerId,
        applied: impl Fn(&LogEntry) -> bool,
        turn: &mut SyncTurn,
    ) {
        let Some(state) = self.peers.get_mut(peer) else {
            return;
        };
        let Some(cursor) = self.cursors.get(&peer.key) else {
            return;
        };
        let prefix = state.holes.iter().take_while(|h| applied(&h.entry)).count();
        state.holes.drain(..prefix);
        let position = state
            .holes
            .first()
            .map(|h| h.position - 1)
            .unwrap_or(state.taken);
        if position != cursor.position {
            let next = StreamCursor {
                log_id: cursor.log_id.clone(),
                position,
            };
            self.cursors.insert(peer.key.clone(), next.clone());
            turn.cursors.insert(peer.key.clone(), next);
        }
    }
    pub fn pump(
        &mut self,
        peer: &PeerId,
        now: i64,
        again: bool,
        lacks: impl Fn(&LogEntry) -> bool,
        turn: &mut SyncTurn,
    ) {
        let Some(state) = self
            .peers
            .get(peer)
            .filter(|s| s.data.is_none() && self.cursors.contains_key(&peer.key))
        else {
            return;
        };
        let inflight: BTreeSet<_> = self
            .peers
            .values()
            .filter_map(|s| s.data.as_ref())
            .flat_map(|d| d.cids.iter().cloned())
            .collect();
        let requested = state
            .holes
            .last()
            .map(|h| state.requested.max(h.position))
            .unwrap_or(state.requested);
        let mut seen = BTreeSet::new();
        let cids: Vec<_> = state
            .holes
            .iter()
            .filter(|h| {
                (again || h.position > state.requested)
                    && !inflight.contains(&h.entry.block)
                    && lacks(&h.entry)
                    && seen.insert(h.entry.block.clone())
            })
            .map(|h| h.entry.block.clone())
            .collect();
        let more =
            state.more && state.stream.is_none() && state.holes.len() < self.config.max_headers;
        self.peers.get_mut(peer).unwrap().requested = requested;
        if !cids.is_empty() {
            self.request_data(peer, cids, now, turn);
        } else if more {
            self.request_stream(peer, now, turn);
        }
    }
    fn request_data(&mut self, peer: &PeerId, cids: Vec<String>, now: i64, turn: &mut SyncTurn) {
        let Some(id) = self.request_id() else { return };
        let Some(state) = self.peers.get_mut(peer) else {
            return;
        };
        state.data = Some(InFlightData {
            request_id: id,
            cids: cids.clone(),
            deadline: now.saturating_add(self.config.timeout),
        });
        state.asked_at = now;
        turn.effects.push(SyncEffect::Send(
            peer.clone(),
            SyncMessage::GetData {
                request_id: id,
                cids,
            },
        ));
    }
    /// Request a missing parent from its announcer. A busy slot leaves the
    /// pending-header owner responsible for retaining and retrying the want.
    pub fn request_ancestors(&mut self, peer: &PeerId, cid: String, now: i64) -> SyncTurn {
        let mut turn = SyncTurn::default();
        if self
            .peers
            .get(peer)
            .is_none_or(|s| s.parent_request.is_some())
        {
            return turn;
        }
        let Some(request_id) = self.request_id() else {
            return turn;
        };
        self.peers.get_mut(peer).unwrap().parent_request = Some(InFlightHeader {
            request_id,
            cid: cid.clone(),
            deadline: now.saturating_add(self.config.timeout),
        });
        turn.effects.push(SyncEffect::Send(
            peer.clone(),
            SyncMessage::GetAncestors {
                request_id,
                cid,
                max: self.config.max_headers,
            },
        ));
        turn
    }
    pub fn receive_request(
        &mut self,
        peer: &PeerId,
        message: SyncMessage,
        tree: &ChainTree,
        now: i64,
    ) -> SyncTurn {
        let mut turn = SyncTurn::default();
        if !self.peers.contains_key(peer) {
            return turn;
        }
        if let SyncMessage::GetStream { own, .. } = &message
            && self.cursors.get(&peer.key).is_none_or(|c| c.log_id != *own)
        {
            self.request_stream(peer, now, &mut turn);
        }
        if !matches!(
            message,
            SyncMessage::GetStream { .. }
                | SyncMessage::GetData { .. }
                | SyncMessage::GetAncestors { .. }
        ) {
            return turn;
        }
        let state = self.peers.get_mut(peer).unwrap();
        if state.serving.is_none() {
            self.serve(peer, message, tree, &mut turn);
        } else if state.queued.len() < 2 {
            state.queued.push_back(message);
        }
        turn
    }
    fn serve(
        &mut self,
        peer: &PeerId,
        message: SyncMessage,
        tree: &ChainTree,
        turn: &mut SyncTurn,
    ) {
        let root_genesis = |cid: &str| {
            tree.context().is_some_and(|c| c.is_root())
                && tree.contains(cid)
                && tree.parent(cid).is_none()
        };
        let (request_id, cids) = match message {
            SyncMessage::GetStream {
                request_id,
                log_id,
                after,
                ..
            } => {
                let from = if log_id.as_deref() == Some(&self.log.id) && after <= self.log.count() {
                    after
                } else {
                    0
                };
                let (entries, has_more) =
                    self.log
                        .page(from, self.config.max_headers, self.config.max_page_bytes);
                self.peers.get_mut(peer).unwrap().subscribed = !has_more;
                turn.effects.push(SyncEffect::Send(
                    peer.clone(),
                    SyncMessage::Stream(StreamPage {
                        request_id,
                        log_id: self.log.id.clone(),
                        entries,
                        has_more,
                    }),
                ));
                if let Some(next) = self.peers.get_mut(peer).unwrap().queued.pop_front() {
                    self.serve(peer, next, tree, turn)
                }
                return;
            }
            SyncMessage::GetData { request_id, cids } => {
                let mut seen = BTreeSet::new();
                (
                    request_id,
                    cids.into_iter()
                        .take(self.config.max_headers)
                        .filter(|cid| {
                            !root_genesis(cid) && tree.contains(cid) && seen.insert(cid.clone())
                        })
                        .collect(),
                )
            }
            SyncMessage::GetAncestors {
                request_id,
                cid,
                max,
            } => {
                let mut result = vec![];
                if !root_genesis(&cid) && tree.contains(&cid) {
                    result.push(cid);
                    while result.len() <= max.min(self.config.max_headers) {
                        let Some(parent) = tree
                            .parent(result.last().unwrap())
                            .filter(|p| !root_genesis(p))
                        else {
                            break;
                        };
                        result.push(parent.into());
                    }
                }
                (request_id, result)
            }
            _ => return,
        };
        let token = self.next_token;
        let Some(next) = token.checked_add(1) else {
            return;
        };
        self.next_token = next;
        self.peers.get_mut(peer).unwrap().serving = Some(token);
        turn.effects.push(SyncEffect::ServeHeaders {
            peer: peer.clone(),
            token,
            request_id,
            cids,
            has_more: false,
        });
    }
    pub fn headers_served(&mut self, peer: &PeerId, token: u64, tree: &ChainTree) -> SyncTurn {
        let mut turn = SyncTurn::default();
        let Some(state) = self
            .peers
            .get_mut(peer)
            .filter(|s| s.serving == Some(token))
        else {
            return turn;
        };
        state.serving = None;
        if let Some(next) = state.queued.pop_front() {
            self.serve(peer, next, tree, &mut turn)
        }
        turn
    }
    pub fn begin_headers_response(
        &mut self,
        peer: &PeerId,
        response: HeadersResponse,
    ) -> Option<HeaderResponseWork> {
        let state = self.peers.get_mut(peer)?;
        let (mut data, mut asked) = (None, None);
        if state
            .data
            .as_ref()
            .is_some_and(|d| d.request_id == response.request_id)
        {
            data = state.data.take();
        } else if state
            .parent_request
            .as_ref()
            .is_some_and(|d| d.request_id == response.request_id)
        {
            asked = state.parent_request.take().map(|d| d.cid);
        } else {
            return None;
        }
        let entries = if let Some(data) = &data {
            let mut wanted: BTreeSet<_> = data.cids.iter().cloned().collect();
            response
                .entries
                .into_iter()
                .filter(|entry| {
                    lattice_primitives::content_id(&entry.block)
                        .ok()
                        .is_some_and(|cid| wanted.remove(&cid))
                })
                .collect()
        } else {
            let mut next = asked.clone();
            let mut entries = vec![];
            for entry in response
                .entries
                .into_iter()
                .take(self.config.max_headers.saturating_add(1))
            {
                if lattice_primitives::content_id(&entry.block).ok() != next {
                    break;
                }
                next = entry.block.parent.as_ref().map(|p| p.0.raw_cid.clone());
                entries.push(entry);
            }
            entries.reverse();
            entries
        };
        Some(HeaderResponseWork {
            entries,
            asked_parent: asked,
            data,
        })
    }
    pub fn finish_headers_response(
        &mut self,
        peer: &PeerId,
        response: HeaderResponseWork,
        received: &BTreeSet<String>,
        now: i64,
        lacks: impl Fn(&LogEntry) -> bool,
    ) -> SyncTurn {
        let mut turn = SyncTurn::default();
        if !self.peers.contains_key(peer) {
            return turn;
        }
        if let Some(data) = response.data {
            let rest: Vec<_> = data
                .cids
                .into_iter()
                .filter(|cid| !received.contains(cid))
                .collect();
            if rest.is_empty() {
                self.pump(peer, now, false, lacks, &mut turn)
            } else if received.is_empty() {
                self.drop_peer(peer);
                turn.effects
                    .push(SyncEffect::DisconnectStalled(peer.clone()));
            } else {
                self.request_data(peer, rest, now, &mut turn);
            }
        }
        turn
    }
    /// Publish only batches already committed by the host's persistence barrier.
    /// A truncated push removes the subscription until the peer pulls to the tail.
    pub fn publish_durable(
        &mut self,
        batches: &[BlockImportBatch],
        is_root: bool,
    ) -> Result<SyncTurn, &'static str> {
        let mut next = self.log.clone();
        let start = next.count();
        for entry in log_entries(batches, is_root) {
            next.append(entry)?;
        }
        let (entries, has_more) =
            next.page(start, self.config.max_headers, self.config.max_page_bytes);
        self.log = next;
        let mut turn = SyncTurn::default();
        if entries.is_empty() {
            return Ok(turn);
        }
        for (peer, state) in &mut self.peers {
            if state.subscribed {
                turn.effects.push(SyncEffect::Send(
                    peer.clone(),
                    SyncMessage::Stream(StreamPage {
                        request_id: 0,
                        log_id: self.log.id.clone(),
                        entries: entries.clone(),
                        has_more,
                    }),
                ));
                if has_more {
                    state.subscribed = false;
                }
            }
        }
        Ok(turn)
    }
    pub fn tick(&mut self, now: i64, lacks: impl Fn(&LogEntry) -> bool) -> SyncTurn {
        let mut turn = SyncTurn::default();
        let stalled: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, s)| s.deadlines().iter().any(|t| *t <= now))
            .map(|(p, _)| p.clone())
            .collect();
        for peer in stalled {
            self.drop_peer(&peer);
            turn.effects.push(SyncEffect::DisconnectStalled(peer));
        }
        for peer in self.peers.keys().cloned().collect::<Vec<_>>() {
            let again = self.peers[&peer]
                .asked_at
                .checked_add(self.config.timeout)
                .is_none_or(|due| now >= due);
            self.pump(&peer, now, again, &lacks, &mut turn);
        }
        turn
    }
}
pub fn log_entries(batches: &[BlockImportBatch], is_root: bool) -> Vec<LogEntry> {
    let roots: BTreeSet<_> = batches
        .iter()
        .flat_map(BlockImportBatch::facts)
        .filter_map(|fact| match fact {
            ChainFact::Block(block) if is_root && block.parent_block_hash.is_none() => {
                Some(block.block_hash.clone())
            }
            _ => None,
        })
        .collect();
    batches
        .iter()
        .flat_map(BlockImportBatch::facts)
        .filter_map(|fact| match fact {
            ChainFact::Block(block) if !roots.contains(&block.block_hash) => {
                Some(LogEntry::header(block.block_hash.clone()))
            }
            ChainFact::Work(work) if !is_root => {
                Some(LogEntry::proof(work.contribution.id(), &work.block_hash))
            }
            _ => None,
        })
        .collect()
}
