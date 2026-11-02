use lattice_block_tree::{ChainTree, ChildBlockProof};
use lattice_cashew::{Header, MerkleDictionary, Volume, VolumeMerkleDictionary, cbor};
use lattice_node::{
    core::{body::BodyEffect, chain::RootChainCore, levels::HostedLevels, stream::StreamConfig},
    storage::*,
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
#[test]
fn child_parent_wake_durability_and_rehosting() {
    let source = BTreeMap::<String, Vec<u8>>::new();
    let mut parent = root();
    let mut spec = parent.spec.0.node.as_deref().unwrap().clone();
    spec.premine = 1;
    parent.spec = Volume::new(spec).unwrap();
    let body = TransactionBody {
        account_actions: vec![AccountAction {
            owner: "alice".into(),
            delta: 100,
        }],
        actions: vec![],
        deposit_actions: vec![],
        receipt_actions: vec![],
        withdrawal_actions: vec![],
        signers: vec![],
        nonce: 0,
        chain_path: vec!["Nexus".into()],
    };
    parent.transactions = Header::new(
        MerkleDictionary::default()
            .inserting(
                "tx",
                Volume::new(Transaction {
                    body: Header::new(body).unwrap(),
                    signatures: BTreeMap::new(),
                })
                .unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let mut state = LatticeState::empty().unwrap();
    state.account_state = Volume::new(
        VolumeMerkleDictionary::default()
            .inserting("alice", 100u64)
            .unwrap(),
    )
    .unwrap();
    parent.post_state = Volume::new(state).unwrap();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(parent.clone()).unwrap(),
        &source,
        i64::MAX,
    )
    .unwrap();
    let hash = content_id(&parent).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let storage = std::sync::Arc::new(storage);
    let mut root_core =
        RootChainCore::open(storage.clone(), &genesis, "log", StreamConfig::default(), 8).unwrap();
    let path = vec!["Nexus".into(), "Alpha".into()];
    let mut child = root();
    child.parent_state = parent.post_state.clone();
    let child_genesis = lattice_validation::validate_child_genesis(
        &Volume::new(child.clone()).unwrap(),
        &source,
        i64::MAX,
        &path,
    )
    .unwrap();
    let proof_for = |block: &Block| {
        let mut carrier = root();
        carrier.prev_state = parent.post_state.clone();
        carrier.nonce = block.height + 1;
        carrier.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(block.clone()).unwrap(),
        )]))
        .unwrap();
        let bytes = BTreeMap::from([
            (
                content_id(&carrier).unwrap(),
                cbor::encode(&carrier).unwrap(),
            ),
            (
                carrier.children.raw_cid.clone(),
                cbor::encode(carrier.children.node.as_deref().unwrap()).unwrap(),
            ),
            (content_id(block).unwrap(), cbor::encode(block).unwrap()),
        ]);
        ChildBlockProof::generate(&content_id(&carrier).unwrap(), "Alpha", &bytes).unwrap()
    };
    let proof = proof_for(&child);

    let mut levels = HostedLevels::default();
    assert!(
        levels
            .host(
                &mut root_core,
                vec!["Nexus".into(), "Missing".into(), "Nested".into()],
                vec![],
                StreamConfig::default(),
                8
            )
            .is_err()
    );
    assert!(
        levels
            .host(
                &mut root_core,
                path.clone(),
                vec![],
                StreamConfig::default(),
                8
            )
            .unwrap()
    );
    assert!(
        !levels
            .host(
                &mut root_core,
                path.clone(),
                vec![],
                StreamConfig::default(),
                8
            )
            .unwrap()
    );
    let child_core = levels.get_mut(&path).unwrap();
    let source_peer = lattice_node::core::sync::PeerId {
        key: "source".into(),
        session: 1,
    };
    child_core.peer_ready(source_peer.clone(), 0);
    assert!(
        child_core
            .queue_header(
                lattice_node::core::sync::HeaderEntry {
                    block: child.clone(),
                    children: None,
                    spec: child.spec.0.node.as_deref().cloned(),
                    proofs: vec![proof.clone()],
                },
                Some(source_peer.clone()),
                Some(root_core.tree()),
                i64::MAX
            )
            .unwrap()
    );
    let job = child_core.proof_work().checks.pop().unwrap();
    let turn = child_core.proof_completed(job.run(None), i64::MAX).unwrap();
    assert!(turn.bodies.is_empty());
    assert!(turn.headers.iter().any(|e|matches!(e,lattice_node::core::headers::HeaderEffect::FetchChildIndex{cid,..} if cid==&child.children.raw_cid)));
    let index = cbor::encode(child.children.node.as_deref().unwrap()).unwrap();
    let stale = lattice_node::core::sync::PeerId {
        key: source_peer.key.clone(),
        session: 2,
    };
    assert!(
        child_core
            .child_index_fetched(&stale, &child.children.raw_cid, Some(&index), i64::MAX)
            .unwrap()
            .bodies
            .is_empty()
    );
    let turn = child_core
        .child_index_fetched(
            &source_peer,
            &child.children.raw_cid,
            Some(&index),
            i64::MAX,
        )
        .unwrap();
    let cid = child_genesis.block_cid();
    assert!(
        turn.bodies
            .iter()
            .any(|e| matches!(e,BodyEffect::Fetch(c) if c==cid))
    );
    storage
        .cache_volumes(
            "child-body",
            &lattice_cashew::volume::serialize_recursive(child_genesis.header(), &()).unwrap(),
        )
        .unwrap();
    let turn = child_core.body_arrived(cid, i64::MAX);
    assert!(
        turn.bodies
            .iter()
            .any(|e| matches!(e,BodyEffect::Connect(j) if j.block_hash()==cid))
    );
    let job = turn
        .bodies
        .iter()
        .find_map(|e| {
            if let BodyEffect::Connect(j) = e {
                Some(j.clone())
            } else {
                None
            }
        })
        .unwrap();
    // Losing proof bytes must wait and repair them, not turn already credited
    // work into an unauthenticated execution capability.
    let proof_db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    proof_db
        .execute("DELETE FROM child_proofs WHERE child=?1", [cid])
        .unwrap();
    assert!(
        child_core
            .run_connection(&job, root_core.tree(), i64::MAX)
            .unwrap()
            .bodies
            .is_empty()
    );
    assert!(child_core.proof_work().lookup.contains(&cid.to_owned()));
    child_core.proofs_found(cid, vec![proof.clone()]);
    let repair = child_core.proof_work().checks.pop().unwrap();
    let resumed = child_core
        .proof_completed(repair.run(Some(&child)), i64::MAX)
        .unwrap();
    let job = resumed
        .bodies
        .iter()
        .find_map(|e| {
            if let BodyEffect::Connect(j) = e {
                Some(j.clone())
            } else {
                None
            }
        })
        .unwrap();
    let absent_parent = ChainTree::empty(root_core.tree().context().unwrap().clone());
    let turn = child_core
        .run_connection(&job, &absent_parent, i64::MAX)
        .unwrap();
    assert!(turn.bodies.is_empty());
    assert_eq!(child_core.awaiting_parent().len(), 1);
    assert!(!child_core.tree().is_executed(cid));
    assert!(
        child_core
            .parent_advanced(&absent_parent, i64::MAX)
            .unwrap()
            .bodies
            .is_empty()
    );
    let turns = levels.parent_advanced(&root_core, i64::MAX).unwrap();
    assert_eq!(turns.len(), 1);
    assert!(
        turns[0]
            .1
            .bodies
            .iter()
            .any(|e| matches!(e,BodyEffect::Connect(j) if j.block_hash()==cid))
    );
    let job = turns[0]
        .1
        .bodies
        .iter()
        .find_map(|e| {
            if let BodyEffect::Connect(j) = e {
                Some(j.clone())
            } else {
                None
            }
        })
        .unwrap();
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_child BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'durability boundary'); END;").unwrap();
    let child_core = levels.get_mut(&path).unwrap();
    assert!(
        child_core
            .run_connection(&job, root_core.tree(), i64::MAX)
            .is_err()
    );
    assert!(!child_core.tree().is_executed(cid));
    db.execute_batch("DROP TRIGGER fail_child").unwrap();
    child_core
        .run_connection(&job, root_core.tree(), i64::MAX)
        .unwrap();
    assert!(child_core.tree().is_executed(cid));
    let entries = child_core.sync().log.entries().to_vec();
    assert!(!entries.is_empty());
    let nested = vec!["Nexus".into(), "Alpha".into(), "Beta".into()];
    levels
        .host(
            &mut root_core,
            nested.clone(),
            vec![],
            StreamConfig::default(),
            8,
        )
        .unwrap();
    assert_eq!(levels.ordered(), vec![path.clone(), nested.clone()]);
    assert!(levels.unhost(&["Nexus".into()]).is_empty());
    assert_eq!(levels.unhost(&path), vec![path.clone(), nested]);
    levels
        .host(
            &mut root_core,
            path.clone(),
            vec![],
            StreamConfig::default(),
            8,
        )
        .unwrap();
    let restored = levels.get(&path).unwrap();
    assert!(restored.tree().is_executed(cid));
    let served = restored
        .served_entries(&[cid.into(), "not-held".into()])
        .unwrap();
    assert_eq!(served.len(), 1);
    assert!(served[0].spec.is_some());
    assert!(served[0].children.is_some());
    assert!(!served[0].proofs.is_empty());
    assert_eq!(
        lattice_primitives::content_id(&served[0].block).unwrap(),
        cid
    );
    assert_eq!(restored.sync().log.entries(), entries);
    assert!(
        !storage
            .journal()
            .saved_proofs(&path, &[cid.into()])
            .unwrap()[cid]
            .is_empty()
    );
    use lattice_node::core::{
        stream::SyncEffect,
        sync::{LogEntry, PeerId, StreamEntry, StreamPage, SyncMessage},
    };
    let restored = levels.get_mut(&path).unwrap();
    let peer = PeerId {
        key: "child-peer".into(),
        session: 9,
    };
    let ready = restored.peer_ready(peer.clone(), 0);
    let request_id = match &ready.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => *request_id,
        _ => panic!(),
    };
    let page = StreamPage {
        request_id,
        log_id: "remote-child".into(),
        has_more: false,
        entries: vec![
            StreamEntry {
                position: 1,
                entry: LogEntry::header(cid),
            },
            StreamEntry {
                position: 2,
                entry: LogEntry::proof(lattice_cashew::cid_for_bytes(0x55, b"unseen-carrier"), cid),
            },
        ],
    };
    db.execute_batch("CREATE TRIGGER fail_cursor BEFORE INSERT ON stream_cursors BEGIN SELECT RAISE(ABORT,'cursor barrier'); END;").unwrap();
    assert!(restored.stream_page(&peer, page.clone(), 1).is_err());
    assert!(!restored.sync().cursors.contains_key(&peer.key));
    db.execute_batch("DROP TRIGGER fail_cursor").unwrap();
    restored.stream_page(&peer, page, 1).unwrap();
    assert_eq!(restored.sync().cursors[&peer.key].position, 1);
    assert_eq!(
        storage.journal().recovery_snapshot(&path).unwrap().cursors[&peer.key].position,
        1
    );
    let mut invalid = child.clone();
    invalid.parent = Some(Volume::new(child.clone()).unwrap());
    invalid.height = 1;
    invalid.timestamp += 1000;
    invalid.post_state = parent.post_state.clone(); // unsupported state creation
    let invalid_proof = proof_for(&invalid);
    let invalid_header = Volume::new(invalid.clone()).unwrap();
    storage
        .cache_volumes(
            "invalid-child",
            &lattice_cashew::volume::serialize_recursive(&invalid_header, &()).unwrap(),
        )
        .unwrap();
    restored
        .admit_header(
            &invalid,
            invalid.children.node.as_deref().unwrap(),
            None,
            &invalid_proof,
            i64::MAX,
        )
        .unwrap();
    let effects = restored.body_arrived(&invalid_header.0.raw_cid, i64::MAX);
    let job = effects
        .bodies
        .iter()
        .find_map(|e| {
            if let BodyEffect::Connect(j) = e {
                Some(j.clone())
            } else {
                None
            }
        })
        .unwrap();
    assert!(matches!(
        restored
            .tree()
            .execute_child_connection(
                &job,
                root_core.tree(),
                &invalid_proof,
                &BTreeMap::<String, Vec<u8>>::new(),
                i64::MAX
            )
            .unwrap(),
        lattice_block_tree::ChildConnectionExecution::Unavailable
    ));
    assert!(matches!(
        restored
            .tree()
            .execute_child_connection(
                &job,
                &absent_parent,
                &invalid_proof,
                storage.as_ref(),
                i64::MAX
            )
            .unwrap(),
        lattice_block_tree::ChildConnectionExecution::AwaitingParent(_)
    ));
    let mut malformed = invalid_proof.clone();
    malformed.entries[0].1[0] ^= 1;
    assert!(matches!(
        restored.tree().execute_child_connection(
            &job,
            root_core.tree(),
            &malformed,
            storage.as_ref(),
            i64::MAX
        ),
        Err(lattice_block_tree::AdmissionError::ProofOfWorkInvalid)
    ));
    db.execute_batch("CREATE TRIGGER fail_rejection BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'rejection barrier'); END;").unwrap();
    assert!(
        restored
            .run_connection(&job, root_core.tree(), i64::MAX)
            .is_err()
    );
    assert!(!restored.tree().is_excluded_root(job.block_hash()));
    db.execute_batch("DROP TRIGGER fail_rejection").unwrap();
    restored
        .run_connection(&job, root_core.tree(), i64::MAX)
        .unwrap();
    assert!(restored.tree().is_excluded_root(job.block_hash()));
    assert!(!restored.tree().is_executed(job.block_hash()));
    let invalid_cid = job.block_hash().to_owned();
    levels.unhost(&path);
    levels
        .host(
            &mut root_core,
            path.clone(),
            vec![],
            StreamConfig::default(),
            8,
        )
        .unwrap();
    assert!(
        levels
            .get(&path)
            .unwrap()
            .tree()
            .is_excluded_root(&invalid_cid)
    );
}
