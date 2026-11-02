use lattice_node::core::sync::*;
#[test]
fn weigh_log_deduplicates_and_pages_position_and_bytes() {
    let mut log = WeighLog::new("session");
    assert!(log.append(LogEntry::header("a")).unwrap());
    assert!(!log.append(LogEntry::header("a")).unwrap());
    log.append(LogEntry::proof("proof", "a")).unwrap();
    log.append(LogEntry::header("b")).unwrap();
    let (first, more) = log.page(0, 20, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].position, 1);
    assert!(more);
    let (rest, more) = log.page(1, 20, usize::MAX);
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[1].position, 3);
    assert!(!more);
    assert!(log.page(u64::MAX, 20, 20).0.is_empty());
    assert!(
        log.append(LogEntry {
            kind: LogKind::Header,
            cid: "wrong".into(),
            block: "other".into()
        })
        .is_err()
    );
}
#[test]
fn declined_queue_remembers_recent_without_refreshing_duplicates() {
    let mut declined = DeclinedSet::new(2);
    declined.insert("a".into());
    declined.insert("b".into());
    declined.insert("a".into());
    declined.insert("c".into());
    assert!(!declined.contains("a"));
    assert!(declined.contains("b"));
    assert_eq!(declined.len(), 2);
    let mut empty = DeclinedSet::new(0);
    empty.insert("a".into());
    assert!(empty.is_empty());
}

use lattice_node::core::stream::*;
use std::collections::BTreeMap;
fn peer(key: &str, session: u64) -> PeerId {
    PeerId {
        key: key.into(),
        session,
    }
}
fn engine() -> HeaderSync {
    HeaderSync::new(
        WeighLog::new("local"),
        BTreeMap::new(),
        StreamConfig {
            timeout: 10,
            ..Default::default()
        },
    )
}
fn page(id: u64, log: &str, entries: &[(u64, &str)]) -> StreamPage {
    StreamPage {
        request_id: id,
        log_id: log.into(),
        has_more: false,
        entries: entries
            .iter()
            .map(|(position, cid)| StreamEntry {
                position: *position,
                entry: LogEntry::header(*cid),
            })
            .collect(),
    }
}
fn request(turn: &SyncTurn) -> u64 {
    match &turn.effects[0] {
        SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => *request_id,
        _ => panic!("expected stream request"),
    }
}
#[test]
fn cursor_never_crosses_unapplied_hole_and_reset_rewinds() {
    let mut sync = engine();
    let p = peer("p", 1);
    let id = request(&sync.peer_ready(p.clone(), 0));
    let turn = sync.receive_page(
        &p,
        page(id, "old", &[(1, "a"), (2, "b"), (3, "c")]),
        1,
        |e| e.cid != "b",
        |_| true,
    );
    assert_eq!(turn.cursors["p"].position, 1);
    assert_eq!(sync.peers[&p].holes.len(), 1);
    assert!(
        matches!(&turn.effects[0],SyncEffect::Send(_,SyncMessage::GetData{cids,..}) if cids==&vec!["b".to_string()])
    );
    let mut turn = SyncTurn::default();
    sync.advance_cursor(&p, |_| true, &mut turn);
    assert_eq!(turn.cursors["p"].position, 3);
    let reset = sync.receive_page(&p, page(0, "new", &[(1, "z")]), 2, |_| false, |_| true);
    assert_eq!(reset.cursors["p"].position, 0);
    assert_eq!(sync.peers[&p].taken, 1);
    assert_eq!(sync.peers[&p].data.as_ref().unwrap().cids, vec!["z"]);
}
#[test]
fn concurrent_peers_deduplicate_fetch_and_old_session_is_inert() {
    let mut sync = engine();
    let a = peer("p", 1);
    let b = peer("q", 1);
    let aid = request(&sync.peer_ready(a.clone(), 0));
    let bid = request(&sync.peer_ready(b.clone(), 0));
    sync.receive_page(&a, page(aid, "a", &[(1, "same")]), 1, |_| false, |_| true);
    let second = sync.receive_page(&b, page(bid, "b", &[(1, "same")]), 1, |_| false, |_| true);
    assert!(second.effects.is_empty());
    assert!(sync.peers[&b].data.is_none());
    sync.drop_peer(&a);
    assert!(
        sync.receive_page(&a, page(0, "a", &[(2, "injected")]), 2, |_| false, |_| true)
            .effects
            .is_empty()
    );
    let turn = sync.tick(11, |_| true);
    assert!(
        matches!(&turn.effects[0],SyncEffect::Send(p,SyncMessage::GetData{cids,..}) if p==&b && cids==&vec!["same".to_string()])
    );
}
#[test]
fn stream_gap_stops_take_and_timeout_disconnects_without_ban() {
    let mut sync = engine();
    let p = peer("p", 7);
    let id = request(&sync.peer_ready(p.clone(), 0));
    let turn = sync.receive_page(
        &p,
        page(id, "remote", &[(1, "a"), (3, "c")]),
        1,
        |_| true,
        |_| false,
    );
    assert_eq!(sync.peers[&p].taken, 1);
    assert_eq!(turn.cursors["p"].position, 1);
    assert!(matches!(
        &turn.effects[0],
        SyncEffect::Send(_, SyncMessage::GetStream { after: 1, .. })
    ));
    assert!(sync.tick(10, |_| true).effects.is_empty());
    assert!(
        matches!(&sync.tick(11,|_|true).effects[0],SyncEffect::DisconnectStalled(who) if who==&p)
    );
    assert!(sync.peers.is_empty());
}

#[test]
fn serving_queue_is_bounded_and_completion_token_is_session_fenced() {
    let root = lattice_cashew::cid_for_bytes(0x71, b"root");
    let context =
        lattice_primitives::ChainRuntimeContext::new(vec!["Nexus".into()], Some(&root)).unwrap();
    let tree = lattice_block_tree::ChainTree::empty(context);
    let mut sync = engine();
    let p = peer("p", 1);
    sync.peer_ready(p.clone(), 0);
    let ask = |request_id| SyncMessage::GetData {
        request_id,
        cids: vec![root.clone()],
    };
    let first = sync.receive_request(&p, ask(1), &tree, 0);
    let token = match first.effects[0] {
        SyncEffect::ServeHeaders { token, .. } => token,
        _ => panic!(),
    };
    for i in 2..9 {
        assert!(
            sync.receive_request(&p, ask(i), &tree, 0)
                .effects
                .is_empty()
        );
    }
    assert_eq!(sync.peers[&p].queued.len(), 2);
    assert!(sync.headers_served(&p, token + 1, &tree).effects.is_empty());
    let second = sync.headers_served(&p, token, &tree);
    assert!(matches!(
        second.effects[0],
        SyncEffect::ServeHeaders { request_id: 2, .. }
    ));
    sync.drop_peer(&p);
    let newer = peer("p", 2);
    sync.peer_ready(newer.clone(), 1);
    assert!(sync.headers_served(&p, token, &tree).effects.is_empty());
    assert!(sync.peers[&newer].serving.is_none());
}
#[test]
fn unmatched_headers_are_ignored_but_empty_requested_data_disconnects() {
    let mut sync = engine();
    let p = peer("p", 1);
    let id = request(&sync.peer_ready(p.clone(), 0));
    sync.receive_page(
        &p,
        page(id, "remote", &[(1, "missing")]),
        1,
        |_| false,
        |_| true,
    );
    let data_id = sync.peers[&p].data.as_ref().unwrap().request_id;
    assert!(
        sync.begin_headers_response(
            &p,
            HeadersResponse {
                request_id: data_id + 1,
                entries: vec![],
                has_more: false
            }
        )
        .is_none()
    );
    assert!(sync.peers[&p].data.is_some());
    let work = sync
        .begin_headers_response(
            &p,
            HeadersResponse {
                request_id: data_id,
                entries: vec![],
                has_more: false,
            },
        )
        .unwrap();
    let turn = sync.finish_headers_response(&p, work, &Default::default(), 2, |_| true);
    assert!(matches!(&turn.effects[0],SyncEffect::DisconnectStalled(who) if who==&p));
}
