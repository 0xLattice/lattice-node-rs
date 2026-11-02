//! Authenticated core ingestion: configured finality must not stall cursors.
use super::*;
use crate::core::{chain::RootChainCore, levels::ChildChainCore, stream::*, sync::*};
fn ready_root(core: &mut RootChainCore, peer: &PeerId, entries: Vec<LogEntry>) -> Option<u64> {
    let effects = core.peer_ready(peer.clone(), 10_000);
    let request_id = effects
        .sync
        .iter()
        .find_map(|e| match e {
            SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => Some(*request_id),
            _ => None,
        })
        .unwrap();
    let effects = core
        .stream_page(
            peer,
            StreamPage {
                request_id,
                log_id: "remote".into(),
                has_more: false,
                entries: entries
                    .into_iter()
                    .enumerate()
                    .map(|(n, entry)| StreamEntry {
                        position: n as u64 + 1,
                        entry,
                    })
                    .collect(),
            },
            10_000,
        )
        .unwrap();
    effects.sync.iter().find_map(|e| match e {
        SyncEffect::Send(_, SyncMessage::GetData { request_id, .. }) => Some(*request_id),
        _ => None,
    })
}
#[test]
fn configured_finality_declines_old_root_header_but_advances_durable_stream() {
    let directory = tempfile::tempdir().unwrap();
    let (storage, _, _, genesis, _, _) = fixture(directory.path());
    let verified = lattice_validation::validate_root_genesis(
        &Volume::new(genesis.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    let mut core = RootChainCore::open(
        storage.clone(),
        &verified,
        "finality",
        StreamConfig::default(),
        8,
    )
    .unwrap();
    core.set_finality_window(Some(2));
    let mut blocks = vec![genesis];
    for height in 1..=5 {
        let mut block = blocks.last().unwrap().clone();
        block.parent = Some(Volume::new(blocks.last().unwrap().clone()).unwrap());
        block.height = height;
        block.timestamp = 1000 + height as i64 * 1000;
        block.prev_state = blocks.last().unwrap().post_state.clone();
        core.admit_header(&block, block.children.node.as_deref().unwrap(), 10_000)
            .unwrap();
        blocks.push(block);
    }
    let mut old = blocks[1].clone();
    old.nonce += 99;
    let mut boundary = blocks[3].clone();
    boundary.nonce += 99;
    let old_cid = content_id(&old).unwrap();
    let boundary_cid = content_id(&boundary).unwrap();
    let peer = PeerId {
        key: "finality-peer".into(),
        session: 1,
    };
    let request_id = ready_root(
        &mut core,
        &peer,
        vec![LogEntry::header(&old_cid), LogEntry::header(&boundary_cid)],
    )
    .unwrap();
    core.received_headers(
        &peer,
        HeadersResponse {
            request_id,
            has_more: false,
            entries: vec![old, boundary]
                .into_iter()
                .map(|block| HeaderEntry {
                    children: block.children.node.as_deref().cloned(),
                    spec: None,
                    proofs: vec![],
                    block,
                })
                .collect(),
        },
        10_000,
    )
    .unwrap();
    assert!(!core.tree().contains(&old_cid));
    assert!(core.tree().contains(&boundary_cid));
    assert_eq!(
        storage
            .journal()
            .recovery_snapshot(&["Nexus".into()])
            .unwrap()
            .cursors[&peer.key]
            .position,
        2
    );
    let peer2 = PeerId {
        key: "finality-peer-2".into(),
        session: 2,
    };
    assert!(ready_root(&mut core, &peer2, vec![LogEntry::header(&old_cid)]).is_none());
    assert_eq!(
        storage
            .journal()
            .recovery_snapshot(&["Nexus".into()])
            .unwrap()
            .cursors[&peer2.key]
            .position,
        1
    );
}

#[test]
fn configured_child_finality_skips_old_header_and_proof_without_stalling() {
    let directory = tempfile::tempdir().unwrap();
    let (storage, parent, _, _, carrier, child) = fixture(directory.path());
    let path = vec!["Nexus".into(), "Alpha".into()];
    let mut core = ChildChainCore::open(
        storage.clone(),
        ChainRuntimeContext::new(path.clone(), None).unwrap(),
        &parent,
        vec![],
        "finality",
        StreamConfig::default(),
        8,
    )
    .unwrap();
    core.set_finality_window(Some(2));
    let mut children = vec![];
    let mut child = child;
    let mut first_proof = None;
    for height in 0..=5 {
        if height > 0 {
            let previous = child.clone();
            child.parent = Some(Volume::new(previous.clone()).unwrap());
            child.height = height;
            child.timestamp = 1000 + height as i64 * 1000;
            child.prev_state = previous.post_state;
        }
        let mut carrier = carrier.clone();
        carrier.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(child.clone()).unwrap(),
        )]))
        .unwrap();
        storage
            .cache_volumes(
                "finality-content",
                &lattice_cashew::volume::serialize_recursive(
                    &Volume::new(carrier.clone()).unwrap(),
                    &(),
                )
                .unwrap(),
            )
            .unwrap();
        let proof =
            ChildBlockProof::generate(&content_id(&carrier).unwrap(), "Alpha", storage.as_ref())
                .unwrap();
        core.admit_header(
            &child,
            child.children.node.as_deref().unwrap(),
            Some(child.spec.0.node.as_deref().unwrap()),
            &proof,
            10_000,
        )
        .unwrap();
        if height == 0 {
            first_proof = Some(proof);
        }
        children.push(child.clone());
    }
    let mut old = children[1].clone();
    old.nonce += 99;
    let cid = content_id(&old).unwrap();
    let peer = PeerId {
        key: "finality-child-peer".into(),
        session: 1,
    };
    let ready = core.peer_ready(peer.clone(), 10_000);
    let request_id = ready
        .sync
        .iter()
        .find_map(|e| match e {
            SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => Some(*request_id),
            _ => None,
        })
        .unwrap();
    let _proof = first_proof.unwrap();
    let proof_entry = LogEntry::proof(
        content_id(&"unseen-finality-proof").unwrap(),
        content_id(&children[0]).unwrap(),
    );
    let effects = core
        .stream_page(
            &peer,
            StreamPage {
                request_id,
                log_id: "remote".into(),
                has_more: false,
                entries: vec![
                    StreamEntry {
                        position: 1,
                        entry: LogEntry::header(&cid),
                    },
                    StreamEntry {
                        position: 2,
                        entry: proof_entry,
                    },
                ],
            },
            10_000,
        )
        .unwrap();
    let request_id = effects
        .sync
        .iter()
        .find_map(|e| match e {
            SyncEffect::Send(_, SyncMessage::GetData { request_id, .. }) => Some(*request_id),
            _ => None,
        })
        .unwrap();
    core.received_headers(
        &peer,
        HeadersResponse {
            request_id,
            has_more: false,
            entries: vec![HeaderEntry {
                children: old.children.node.as_deref().cloned(),
                spec: None,
                proofs: vec![],
                block: old,
            }],
        },
        Some(&parent),
        10_000,
    )
    .unwrap();
    assert!(!core.tree().contains(&cid));
    assert_eq!(
        storage.journal().recovery_snapshot(&path).unwrap().cursors[&peer.key].position,
        2
    );
    assert!(core.proof_work().checks.is_empty());
}
