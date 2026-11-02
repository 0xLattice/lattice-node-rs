use lattice_block_tree::{ChainTree, ChildBlockProof};
use lattice_cashew::{Header, MerkleDictionary, Volume, cbor};
use lattice_node::core::{
    child_proofs::*,
    pending::PendingQueue,
    sync::{HeaderEntry, PeerId},
};
use lattice_primitives::*;
use std::collections::BTreeMap;
fn root() -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: None,
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume::new(ChainSpec {
            max_number_of_transactions_per_block: 100,
            max_state_growth: 100_000,
            max_block_size: 1_000_000,
            premine: 0,
            target_block_time: 1000,
            initial_reward: 1024,
            halving_interval: 210_000,
            half_life: 120,
            wasm_policies: vec![],
        })
        .unwrap(),
        parent_state: empty.clone(),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: 0,
        timestamp: 1000,
        reward_recipient: None,
        nonce: 0,
    }
}

fn fixture(nonce: u64) -> (HeaderEntry, String) {
    let mut child = root();
    child.nonce = nonce;
    let mut carrier = root();
    carrier.children = Header::new(BTreeMap::from([(
        "Alpha".into(),
        Volume::new(child.clone()).unwrap(),
    )]))
    .unwrap();
    let cid = content_id(&child).unwrap();
    let source = BTreeMap::from([
        (
            content_id(&carrier).unwrap(),
            cbor::encode(&carrier).unwrap(),
        ),
        (
            carrier.children.raw_cid.clone(),
            cbor::encode(carrier.children.node.as_deref().unwrap()).unwrap(),
        ),
        (cid.clone(), cbor::encode(&child).unwrap()),
    ]);
    let proof =
        ChildBlockProof::generate(&content_id(&carrier).unwrap(), "Alpha", &source).unwrap();
    (
        HeaderEntry {
            children: child.children.node.as_deref().cloned(),
            spec: child.spec.0.node.as_deref().cloned(),
            block: child,
            proofs: vec![proof],
        },
        cid,
    )
}
#[test]
fn separate_check_slots_content_dedup_and_real_verification() {
    let path = vec!["Nexus".into(), "Alpha".into()];
    let tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
    let pending = PendingQueue::default();
    let mut queue = ChildProofSync::default();
    let config = ChildProofConfig {
        max_checks: 1,
        index_checks: 1,
        ..Default::default()
    };
    let peer = PeerId {
        key: "peer".into(),
        session: 1,
    };
    let (entry, cid) = fixture(0);
    assert!(queue.await_header(
        entry.clone(),
        Some(peer.clone()),
        &config,
        &pending,
        &tree,
        None
    ));
    let bytes = queue.queued_bytes();
    queue.offer(
        entry.proofs.clone(),
        &cid,
        Some(peer.clone()),
        &config,
        &pending,
        &tree,
    );
    assert_eq!(queue.queued_bytes(), bytes);
    // Same root, different content is a distinct check. A forged twin must
    // neither suppress the valid proof nor share its successful verdict.
    let mut bad = entry.proofs[0].clone();
    bad.entries[0].1[0] ^= 1;
    queue.offer(vec![bad], &cid, None, &config, &pending, &tree);
    let work = queue.work(&path, &config, &pending, &tree, &[]);
    assert_eq!(work.checks.len(), 2);
    assert_eq!(queue.checks(false), 1);
    assert_eq!(queue.checks(true), 1);
    assert_ne!(work.checks[0].key().proof_id, work.checks[1].key().proof_id);
    let mut accepted = 0;
    let mut refused = 0;
    for job in work.checks {
        match queue.completed(job.run(None)).unwrap() {
            CheckedProof::Accepted {
                child_cid,
                evidence,
                ..
            } => {
                assert_eq!(child_cid, cid);
                assert_eq!(evidence.child_cid(), cid);
                accepted += 1;
            }
            CheckedProof::Rejected { source, .. } => {
                assert!(source.is_none());
                refused += 1;
            }
        }
    }
    assert_eq!((accepted, refused), (1, 1));
    assert_eq!(queue.queued_bytes(), 0);
}
#[test]
fn awaiting_budget_no_proof_gate_and_session_purge() {
    let path = vec!["Nexus".into(), "Alpha".into()];
    let tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
    let pending = PendingQueue::default();
    let config = ChildProofConfig {
        max_awaiting: 1,
        max_wanted: 1,
        max_queued_bytes: 0,
        ..Default::default()
    };
    let mut queue = ChildProofSync::default();
    let peer = PeerId {
        key: "p".into(),
        session: 1,
    };
    let (mut first, cid) = fixture(1);
    let proofs = std::mem::take(&mut first.proofs);
    assert!(!queue.await_header(
        first.clone(),
        Some(peer.clone()),
        &config,
        &pending,
        &tree,
        None
    ));
    queue.evidence_changed(std::slice::from_ref(&cid), &config, &pending, &tree);
    assert!(queue.await_header(
        first.clone(),
        Some(peer.clone()),
        &config,
        &pending,
        &tree,
        None
    ));
    queue.offer(
        proofs.clone(),
        &cid,
        Some(peer.clone()),
        &config,
        &pending,
        &tree,
    );
    assert_eq!(queue.queued_bytes(), 0);
    assert_eq!(
        queue.work(&path, &config, &pending, &tree, &[]).lookup,
        vec![cid.clone()]
    );
    let enabled = ChildProofConfig {
        max_queued_bytes: 1 << 20,
        ..config
    };
    queue.offer(proofs, &cid, Some(peer.clone()), &enabled, &pending, &tree);
    let job = queue
        .work(&path, &enabled, &pending, &tree, &[])
        .checks
        .pop()
        .unwrap();
    let (second, second_cid) = fixture(2);
    assert!(!queue.await_header(
        second.clone(),
        Some(peer.clone()),
        &enabled,
        &pending,
        &tree,
        None
    ));
    queue.drop_peer(&peer);
    assert!(queue.completed(job.run(Some(&first.block))).is_none());
    assert!(queue.await_header(
        second,
        Some(PeerId {
            key: "p".into(),
            session: 2
        }),
        &enabled,
        &pending,
        &tree,
        None
    ));
    assert!(!queue.awaiting.contains_key(&cid));
    assert!(queue.awaiting.contains_key(&second_cid));
}
