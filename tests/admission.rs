use lattice_block_tree::{BlockImportBatch, ChainTree};
use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_node::storage::*;
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
// Trusted genesis fixture; production genesis execution is intentionally not
// bypassed by a public verified constructor.
fn bootstrap(store: &NodeStorage, root: &Block) -> ChainTree {
    let hash = content_id(root).unwrap();
    let fact = serde_json::json!({"facts":[{"block":{"_0":{"blockHash":hash,"blockHeight":0,"postStateCID":root.post_state.0.raw_cid,"prevStateCID":root.prev_state.0.raw_cid,"parentStateCID":root.parent_state.0.raw_cid,"specCID":root.spec.0.raw_cid,"target":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","nextTarget":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","timestamp":root.timestamp,"stateDiff":{"replaced":{},"created":{}}}}},{"work":{"_0":{"blockHash":hash,"contribution":{"id":hash,"work":"0x1"}}}},{"validation":{"_0":{"blockHash":hash}}}]});
    let batch: BlockImportBatch = serde_json::from_value(fact).unwrap();
    store
        .journal()
        .stage_node_facts(
            &[NodeFactBatch {
                proofs: vec![],
                path: vec!["Nexus".into()],
                facts: vec![batch],
                headers: vec![
                    StoredHeader::from_nodes(root, root.children.node.as_deref().unwrap()).unwrap(),
                ],
                volume_roots: vec![],
                cursors: BTreeMap::new(),
            }],
            "log",
        )
        .unwrap();
    let (tree, _) = store
        .journal()
        .restore_chain(
            ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap(),
            None,
            vec![root.spec.0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    tree
}
#[test]
fn authenticated_header_sql_failure_never_mutates_graph() {
    let tmp = tempfile::tempdir().unwrap();
    let root = root();
    let storage = NodeStorage::open(tmp.path(), content_id(&root).unwrap(), "held").unwrap();
    let mut tree = bootstrap(&storage, &root);
    let mut child = root.clone();
    child.parent = Some(Volume::new(root.clone()).unwrap());
    child.height = 1;
    child.timestamp += 1000;
    let cid = content_id(&child).unwrap();
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_admission BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'injected IO boundary failure'); END;").unwrap();
    assert!(
        storage
            .import_root_header(
                &mut tree,
                &child,
                child.children.node.as_deref().unwrap(),
                child.timestamp,
                "log"
            )
            .is_err()
    );
    assert_eq!(tree.block_count(), 1);
    assert_eq!(tree.current_revision(), 1);
    assert_eq!(
        storage.journal().revision_floor(&["Nexus".into()]).unwrap(),
        1
    );
    assert!(storage.journal().header(&cid).unwrap().is_none());
    db.execute_batch("DROP TRIGGER fail_admission").unwrap();
    assert!(matches!(
        storage
            .import_root_header(
                &mut tree,
                &child,
                child.children.node.as_deref().unwrap(),
                child.timestamp,
                "log"
            )
            .unwrap(),
        HeaderImport::Applied(_)
    ));
    assert_eq!(tree.canonical_tip(), cid);
    let stored = storage.journal().header(&cid).unwrap().unwrap();
    assert_eq!(content_id(&stored.0).unwrap(), cid);
    let revision = tree.current_revision();
    assert!(matches!(
        storage
            .import_root_header(
                &mut tree,
                &child,
                child.children.node.as_deref().unwrap(),
                child.timestamp,
                "log"
            )
            .unwrap(),
        HeaderImport::Duplicate
    ));
    assert_eq!(tree.current_revision(), revision);
}
#[test]
fn typed_header_read_authenticates_disk_content() {
    let tmp = tempfile::tempdir().unwrap();
    let root = root();
    let hash = content_id(&root).unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let _ = bootstrap(&storage, &root);
    assert!(storage.journal().header(&hash).unwrap().is_some());
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute(
        "UPDATE headers SET block=?1 WHERE cid=?2",
        rusqlite::params![vec![0u8], hash],
    )
    .unwrap();
    assert!(storage.journal().header(&hash).unwrap().is_none());
}
#[test]
fn pending_queue_evicts_only_leaves_and_lifts_ancestors() {
    use lattice_node::core::pending::*;
    fn pending(block: Block, hash: u8) -> PendingHeader {
        PendingHeader {
            block_cid: content_id(&block).unwrap(),
            block,
            children: None,
            spec: None,
            hash: UInt256([hash; 32]),
            bytes: 10,
            announcers: vec![],
            asked_parent: false,
            not_before: None,
            evidence: BTreeMap::new(),
            proofs: BTreeMap::new(),
        }
    }
    let root = root();
    let root_cid = content_id(&root).unwrap();
    let mut child = root.clone();
    child.parent = Some(Volume::new(root.clone()).unwrap());
    child.height = 1;
    child.nonce = 1;
    let child_cid = content_id(&child).unwrap();
    let mut sibling = child.clone();
    sibling.nonce = 2;
    let sibling_cid = content_id(&sibling).unwrap();
    let mut queue = PendingQueue::default();
    queue.insert(pending(root, 200)).unwrap();
    queue.insert(pending(child, 1)).unwrap();
    queue.insert(pending(sibling.clone(), 100)).unwrap();
    assert_eq!(queue.priorities()[&root_cid], UInt256([1; 32]));
    let gone = queue.evict(20);
    assert_eq!(gone[0].block_cid, sibling_cid);
    assert!(queue.entries().contains_key(&root_cid));
    queue.remove(&child_cid);
    assert_eq!(queue.priorities()[&root_cid], UInt256([200; 32]));
    for _ in 0..50 {
        queue.insert(pending(sibling.clone(), 100)).unwrap();
        queue.remove(&sibling_cid);
    }
    queue.compact();
    assert!(queue.bookkeeping() <= 2 * queue.entries().len() + 8);
    assert_eq!(queue.bytes(), 10);
}

#[test]
fn authentic_genesis_preparation_requires_pin_and_is_not_visible_before_apply() {
    let root = root();
    let hash = content_id(&root).unwrap();
    let verified = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    let mut tree =
        ChainTree::empty(ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap());
    let lattice_block_tree::HeaderPreparation::Prepared(prepared) = tree
        .prepare_root_genesis(&verified, root.children.node.as_deref().unwrap())
        .unwrap()
    else {
        panic!()
    };
    assert!(!tree.contains(&hash));
    assert_eq!(prepared.batches().len(), 1);
    tree.apply_prepared(*prepared).unwrap();
    assert!(tree.is_executed(&hash));
    assert_eq!(tree.canonical_tip(), hash);
    assert!(matches!(
        tree.prepare_root_genesis(&verified, root.children.node.as_deref().unwrap())
            .unwrap(),
        lattice_block_tree::HeaderPreparation::Duplicate
    ));
    let other = lattice_cashew::cid_for_bytes(0x71, b"wrong pin");
    let wrong =
        ChainTree::empty(ChainRuntimeContext::new(vec!["Nexus".into()], Some(&other)).unwrap());
    assert!(
        wrong
            .prepare_root_genesis(&verified, root.children.node.as_deref().unwrap())
            .is_err()
    );
}

#[test]
fn ancestor_responses_only_admit_linked_prefix_in_parent_order() {
    use lattice_node::core::{stream::*, sync::*};
    let parent = root();
    let parent_id = content_id(&parent).unwrap();
    let mut child = parent.clone();
    child.height = 1;
    child.parent = Some(Volume::new(parent.clone()).unwrap());
    let child_id = content_id(&child).unwrap();
    let peer = PeerId {
        key: "peer".into(),
        session: 1,
    };
    let mut sync = HeaderSync::new(
        WeighLog::new("own"),
        BTreeMap::new(),
        StreamConfig::default(),
    );
    sync.peer_ready(peer.clone(), 0);
    let turn = sync.request_ancestors(&peer, child_id.clone(), 1);
    let request_id = match turn.effects[0] {
        SyncEffect::Send(_, SyncMessage::GetAncestors { request_id, .. }) => request_id,
        _ => panic!(),
    };
    assert!(
        sync.request_ancestors(&peer, parent_id.clone(), 1)
            .effects
            .is_empty()
    );
    let entry = |block| HeaderEntry {
        block,
        children: None,
        proofs: vec![],
        spec: None,
    };
    let work = sync
        .begin_headers_response(
            &peer,
            HeadersResponse {
                request_id,
                entries: vec![entry(child), entry(parent.clone()), entry(parent)],
                has_more: false,
            },
        )
        .unwrap();
    assert_eq!(work.entries.len(), 2);
    assert_eq!(content_id(&work.entries[0].block).unwrap(), parent_id);
    assert_eq!(content_id(&work.entries[1].block).unwrap(), child_id);
    assert_eq!(work.asked_parent.as_deref(), Some(child_id.as_str()));
}

#[test]
fn byte_sourced_genesis_persists_materialized_volumes_before_execution_and_recovers() {
    let root = root();
    let full = Volume::new(root.clone()).unwrap();
    let hash = full.0.raw_cid.clone();
    let volumes = lattice_cashew::volume::serialize_recursive(&full, &()).unwrap();
    let source: BTreeMap<String, Vec<u8>> = volumes.into_iter().flat_map(|v| v.entries).collect();
    let lazy = Volume(lattice_cashew::Header::<Block>::from_cid(&hash).unwrap());
    let verified = lattice_validation::validate_root_genesis(&lazy, &source, i64::MAX).unwrap();
    let context = ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let mut tree = ChainTree::empty(context.clone());
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_genesis BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(
        storage
            .import_root_genesis(&mut tree, &verified, "log")
            .is_err()
    );
    assert_eq!(tree.block_count(), 0);
    assert_eq!(tree.current_revision(), 0);
    assert!(
        storage
            .journal()
            .recovery_snapshot(&["Nexus".into()])
            .unwrap()
            .batches
            .is_empty()
    );
    assert!(storage.fetch_volume(&hash).is_some());
    db.execute_batch("DROP TRIGGER fail_genesis").unwrap();
    drop(db);
    assert!(matches!(
        storage
            .import_root_genesis(&mut tree, &verified, "log")
            .unwrap(),
        HeaderImport::Applied(_)
    ));
    assert!(tree.is_executed(&hash));
    let expected = lattice_cashew::volume::serialize_recursive(verified.header(), &()).unwrap();
    for volume in &expected {
        assert_eq!(
            storage.fetch_volume(&volume.root).unwrap().entries,
            volume.entries
        );
        assert!(storage.retained_roots().unwrap().contains(&volume.root));
    }
    drop(storage);
    let reopened = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let from_disk = lattice_validation::validate_root_genesis(&lazy, &reopened, i64::MAX).unwrap();
    assert_eq!(from_disk.block_cid(), hash);

    let (restored, snapshot) = reopened
        .journal()
        .restore_chain(
            context,
            None,
            vec![root.spec.0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    assert_eq!(snapshot.batches.len(), 1);
    assert!(restored.is_executed(&hash));
    assert_eq!(restored.canonical_tip(), hash);
}

#[test]
fn root_core_does_not_release_or_remember_cursor_turn_before_sql_commit() {
    use lattice_node::core::{chain::*, stream::*, sync::*};
    let root = root();
    let hash = content_id(&root).unwrap();
    let verified = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = std::sync::Arc::new(NodeStorage::open(tmp.path(), &hash, "held").unwrap());
    let mut core = RootChainCore::open(
        storage.clone(),
        &verified,
        "log",
        StreamConfig::default(),
        64,
    )
    .unwrap();
    let peer = PeerId {
        key: "peer".into(),
        session: 1,
    };
    let effects = core.peer_ready(peer.clone(), 1000);
    let id = match effects.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => request_id,
        _ => panic!(),
    };
    let page = StreamPage {
        request_id: id,
        log_id: "remote".into(),
        entries: vec![StreamEntry {
            position: 1,
            entry: LogEntry::header(&hash),
        }],
        has_more: false,
    };
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_cursor BEFORE INSERT ON stream_cursors BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    let (topic, payload) =
        lattice_node::core::wire::encode(&SyncMessage::Stream(page.clone()), &["Nexus".into()])
            .unwrap();
    assert!(core.received_wire(&peer, topic, &payload, 1001).is_err());
    assert_eq!(core.sync().peers[&peer].taken, 0);
    assert!(core.sync().cursors.is_empty());
    assert!(core.sync().peers[&peer].stream.is_some());
    db.execute_batch("DROP TRIGGER fail_cursor").unwrap();
    assert!(matches!(
        core.received_wire(&peer, topic, &payload, 1002).unwrap(),
        WireDispatch::Applied(_)
    ));
    let (other_topic, other_payload) = lattice_node::core::wire::encode(
        &SyncMessage::Stream(page),
        &["Nexus".into(), "child".into()],
    )
    .unwrap();
    assert!(matches!(
        core.received_wire(&peer, other_topic, &other_payload, 1002)
            .unwrap(),
        WireDispatch::OtherLevel { .. }
    ));
    assert!(matches!(
        core.received_wire(&peer, "unrelated", b"bad", 1002)
            .unwrap(),
        WireDispatch::UnknownTopic
    ));
    assert!(core.received_wire(&peer, topic, b"bad", 1002).is_err());
    assert_eq!(core.sync().cursors["peer"].position, 1);
    assert_eq!(
        storage
            .journal()
            .recovery_snapshot(&["Nexus".into()])
            .unwrap()
            .cursors["peer"]
            .position,
        1
    );
    let mut child = root.clone();
    child.height = 1;
    child.timestamp += 1000;
    child.parent = Some(Volume::new(root).unwrap());
    let cid = content_id(&child).unwrap();
    let effects = core
        .admit_header(&child, child.children.node.as_deref().unwrap(), 2000)
        .unwrap();
    assert!(
        effects
            .bodies
            .iter()
            .any(|e| matches!(e,lattice_node::core::body::BodyEffect::Fetch(h) if h==&cid))
    );
    let capability = lattice_validation::validate_root_transition(
        &Volume::new(child).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
        None,
    )
    .unwrap();
    assert!(core.connected_root(&capability, 2001).is_err());
    let effects = core.body_arrived(&cid, 2001);
    assert!(effects.bodies.iter().any(
        |e| matches!(e,lattice_node::core::body::BodyEffect::Connect(job) if job.block_hash()==cid)
    ));
    core.connected_root(&capability, 2002).unwrap();
    assert!(core.tree().is_executed(&cid));
    assert!(core.connected_root(&capability, 2003).is_err());
}

#[test]
fn root_transition_requires_held_header_then_stages_execution_atomically() {
    let root = root();
    let hash = content_id(&root).unwrap();
    let source = BTreeMap::<String, Vec<u8>>::new();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &source,
        i64::MAX,
    )
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let context = ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap();
    let mut tree = ChainTree::empty(context.clone());
    store
        .import_root_genesis(&mut tree, &genesis, "log")
        .unwrap();
    tree.hold_spec(
        root.spec.0.node.as_deref().unwrap().clone(),
        &root.spec.0.raw_cid,
    );
    let mut child = root.clone();
    child.parent = Some(Volume::new(root.clone()).unwrap());
    child.height = 1;
    child.timestamp += 1000;
    let cid = content_id(&child).unwrap();
    let verified = lattice_validation::validate_root_transition(
        &Volume::new(child.clone()).unwrap(),
        &source,
        i64::MAX,
        None,
    )
    .unwrap();
    assert!(tree.prepare_root_transition(&verified).is_err());
    store
        .import_root_header(
            &mut tree,
            &child,
            child.children.node.as_deref().unwrap(),
            i64::MAX,
            "log",
        )
        .unwrap();
    assert!(!tree.is_executed(&cid));
    let mismatched = lattice_validation::validate_root_transition(
        &Volume::new(child.clone()).unwrap(),
        &source,
        i64::MAX,
        Some(lattice_validation::DifficultyAnchor {
            block_height: 999,
            timestamp: 0,
            target: UInt256([1; 32]),
        }),
    )
    .unwrap();
    assert!(tree.prepare_root_transition(&mismatched).is_err());
    let revision = tree.current_revision();
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_execution BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(
        store
            .import_root_transition(&mut tree, &verified, "log")
            .is_err()
    );
    assert!(!tree.is_executed(&cid));
    assert_eq!(tree.current_revision(), revision);
    db.execute_batch("DROP TRIGGER fail_execution").unwrap();
    drop(db);
    store
        .import_root_transition(&mut tree, &verified, "log")
        .unwrap();
    assert!(tree.is_executed(&cid));
    assert_eq!(tree.heaviest_executed_tip(&Default::default()), cid);
    drop(store);
    let store = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let (restored, _) = store
        .journal()
        .restore_chain(
            context,
            None,
            vec![root.spec.0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    assert!(restored.is_executed(&cid));
}

#[test]
fn child_execution_requires_real_parent_facts_in_addition_to_carrier_proof() {
    use lattice_block_tree::{ChildBlockProof, ParentAnchorFailure};
    use lattice_cashew::{VolumeMerkleDictionary, cbor};
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
    let context = ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "held").unwrap();
    let mut parent_tree = ChainTree::empty(context.clone());
    storage
        .import_root_genesis(&mut parent_tree, &genesis, "log")
        .unwrap();
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
    let mut tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
    let absent_parent = ChainTree::empty(context);
    assert!(matches!(
        storage.import_child_genesis(&mut tree, &absent_parent, &child_genesis, &proof, "log"),
        Err(StoreError::ParentAnchor(ParentAnchorFailure::Required(_)))
    ));
    assert_eq!(tree.block_count(), 0);
    storage
        .import_child_genesis_header(
            &mut tree,
            &child,
            child.children.node.as_deref().unwrap(),
            child.spec.0.node.as_deref().unwrap(),
            &proof,
            "log",
        )
        .unwrap();
    assert!(tree.contains(child_genesis.block_cid()));
    assert!(!tree.is_executed(child_genesis.block_cid()));
    assert!(storage.fetch_volume(&child.spec.0.raw_cid).is_some());
    storage
        .import_child_genesis(&mut tree, &parent_tree, &child_genesis, &proof, "log")
        .unwrap();
    assert!(tree.is_executed(child_genesis.block_cid()));
    let mut next = child.clone();
    next.parent = Some(Volume::new(child).unwrap());
    next.height = 1;
    next.timestamp += 1000;
    let proof = proof_for(&next);
    storage
        .import_child_header(
            &mut tree,
            &next,
            next.children.node.as_deref().unwrap(),
            &proof,
            i64::MAX,
            "log",
        )
        .unwrap();
    let execution = lattice_validation::validate_child_transition(
        &Volume::new(next).unwrap(),
        &source,
        i64::MAX,
        None,
        &path,
    )
    .unwrap();
    storage
        .import_child_transition(&mut tree, &parent_tree, &execution, &proof, "log")
        .unwrap();
    assert!(tree.is_executed(execution.block_cid()));
    let (restored, _) = storage
        .journal()
        .restore_chain(
            ChainRuntimeContext::new(path, None).unwrap(),
            Some(&parent_tree),
            vec![execution.spec().0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    assert!(restored.is_executed(execution.block_cid()));
}

#[test]
fn received_orphan_ancestors_commit_as_one_turn_and_unblock_cursor() {
    use lattice_node::core::{chain::*, stream::*, sync::*};
    let root = root();
    let hash = content_id(&root).unwrap();
    let source = BTreeMap::<String, Vec<u8>>::new();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &source,
        i64::MAX,
    )
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = std::sync::Arc::new(NodeStorage::open(tmp.path(), &hash, "held").unwrap());
    let mut core = RootChainCore::open(
        storage.clone(),
        &genesis,
        "log",
        StreamConfig::default(),
        64,
    )
    .unwrap();
    let mut first = root.clone();
    first.height = 1;
    first.timestamp += 1000;
    first.parent = Some(Volume::new(root).unwrap());
    let mut second = first.clone();
    second.height = 2;
    second.timestamp += 1000;
    second.parent = Some(Volume::new(first.clone()).unwrap());
    let first_cid = content_id(&first).unwrap();
    let second_cid = content_id(&second).unwrap();
    let peer = PeerId {
        key: "peer".into(),
        session: 1,
    };
    let ready = core.peer_ready(peer.clone(), 4000);
    let id = match ready.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => request_id,
        _ => panic!(),
    };
    let turn = core
        .stream_page(
            &peer,
            StreamPage {
                request_id: id,
                log_id: "remote".into(),
                entries: vec![StreamEntry {
                    position: 1,
                    entry: LogEntry::header(&second_cid),
                }],
                has_more: false,
            },
            4001,
        )
        .unwrap();
    let data_id = match turn.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetData { request_id, .. }) => request_id,
        _ => panic!(),
    };
    let entry = |block: Block| HeaderEntry {
        children: block.children.node.as_deref().cloned(),
        block,
        proofs: vec![],
        spec: None,
    };
    let turn = core
        .received_headers(
            &peer,
            HeadersResponse {
                request_id: data_id,
                entries: vec![entry(second.clone())],
                has_more: false,
            },
            4002,
        )
        .unwrap();
    assert!(!core.tree().contains(&second_cid));
    assert_eq!(core.sync().cursors["peer"].position, 0);
    let ancestor_id = turn
        .sync
        .iter()
        .find_map(|e| match e {
            SyncEffect::Send(
                _,
                SyncMessage::GetAncestors {
                    request_id, cid, ..
                },
            ) if cid == &first_cid => Some(*request_id),
            _ => None,
        })
        .unwrap();
    let response = HeadersResponse {
        request_id: ancestor_id,
        entries: vec![entry(first)],
        has_more: false,
    };
    let db = rusqlite::Connection::open(tmp.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_batch BEFORE INSERT ON admission_batches WHEN (SELECT COUNT(*) FROM admission_batches)>1 BEGIN SELECT RAISE(ABORT,'second header failed'); END;").unwrap();
    assert!(
        core.received_headers(&peer, response.clone(), 4003)
            .is_err()
    );
    assert!(!core.tree().contains(&first_cid));
    assert!(!core.tree().contains(&second_cid));
    assert_eq!(
        storage
            .journal()
            .recovery_snapshot(&["Nexus".into()])
            .unwrap()
            .batches
            .len(),
        1
    );
    db.execute_batch("DROP TRIGGER fail_batch").unwrap();
    core.received_headers(&peer, response, 4004).unwrap();
    assert!(core.tree().contains(&first_cid));
    assert!(core.tree().contains(&second_cid));
    assert_eq!(core.sync().cursors["peer"].position, 1);
    let snapshot = storage
        .journal()
        .recovery_snapshot(&["Nexus".into()])
        .unwrap();
    assert_eq!(snapshot.batches.len(), 3);
    assert_eq!(snapshot.cursors["peer"].position, 1);
}

#[test]
fn future_header_waits_before_requesting_missing_index_and_old_session_cannot_complete_it() {
    use lattice_node::core::{chain::*, headers::*, stream::*, sync::*};
    let root = root();
    let hash = content_id(&root).unwrap();
    let source = BTreeMap::<String, Vec<u8>>::new();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &source,
        i64::MAX,
    )
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = std::sync::Arc::new(NodeStorage::open(tmp.path(), &hash, "held").unwrap());
    let mut core =
        RootChainCore::open(storage, &genesis, "log", StreamConfig::default(), 64).unwrap();
    let mut child = root.clone();
    child.height = 1;
    child.timestamp += 1000;
    child.parent = Some(Volume::new(root).unwrap());
    let cid = content_id(&child).unwrap();
    let index_cid = child.children.raw_cid.clone();
    let index = lattice_cashew::cbor::encode(child.children.node.as_deref().unwrap()).unwrap();
    let peer = PeerId {
        key: "peer".into(),
        session: 2,
    };
    let ready = core.peer_ready(peer.clone(), 1000);
    let id = match ready.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => request_id,
        _ => panic!(),
    };
    let turn = core
        .stream_page(
            &peer,
            StreamPage {
                request_id: id,
                log_id: "remote".into(),
                entries: vec![StreamEntry {
                    position: 1,
                    entry: LogEntry::header(&cid),
                }],
                has_more: false,
            },
            1001,
        )
        .unwrap();
    let id = match turn.sync[0] {
        SyncEffect::Send(_, SyncMessage::GetData { request_id, .. }) => request_id,
        _ => panic!(),
    };
    let turn = core
        .received_headers(
            &peer,
            HeadersResponse {
                request_id: id,
                entries: vec![HeaderEntry {
                    block: child,
                    children: None,
                    proofs: vec![],
                    spec: None,
                }],
                has_more: false,
            },
            1002,
        )
        .unwrap();
    assert!(turn.headers.is_empty());
    assert!(!core.tree().contains(&cid));
    assert!(core.tick(1999).unwrap().headers.is_empty());
    let turn = core.tick(2000).unwrap();
    assert!(
        matches!(&turn.headers[0],HeaderEffect::FetchChildIndex{peer:p,cid:c} if p==&peer && c==&index_cid)
    );
    core.child_index_fetched(
        &PeerId {
            key: "peer".into(),
            session: 1,
        },
        &index_cid,
        Some(&index),
        2001,
    )
    .unwrap();
    assert!(!core.tree().contains(&cid));
    core.child_index_fetched(&peer, &index_cid, Some(&index), 2002)
        .unwrap();
    assert!(core.tree().contains(&cid));
    assert_eq!(core.sync().cursors["peer"].position, 1);
}

#[test]
fn root_body_rejection_requires_real_validation_and_is_durable_before_exclusion() {
    use lattice_block_tree::RootConnectionExecution;
    let root = root();
    let hash = content_id(&root).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(dir.path(), &hash, "held").unwrap();
    let verified = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &BTreeMap::new(),
        i64::MAX,
    )
    .unwrap();
    let mut tree =
        ChainTree::empty(ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap());
    storage
        .import_root_genesis(&mut tree, &verified, "log")
        .unwrap();
    let mut child = root.clone();
    child.parent = Some(Volume::new(root).unwrap());
    child.height = 1;
    child.timestamp = 2000;
    child.post_state = Volume::new(LatticeState {
        account_state: Volume::new(AccountState::default().inserting("invented", 100).unwrap())
            .unwrap(),
        ..LatticeState::empty().unwrap()
    })
    .unwrap();
    let cid = content_id(&child).unwrap();
    storage
        .import_root_header(
            &mut tree,
            &child,
            child.children.node.as_deref().unwrap(),
            2000,
            "log",
        )
        .unwrap();
    let job = tree.connect_job(&cid, None).unwrap();
    assert!(matches!(
        tree.execute_root_connection(&job, &BTreeMap::new(), 2000)
            .unwrap(),
        RootConnectionExecution::Unavailable
    ));
    let volumes =
        lattice_cashew::volume::serialize_recursive(&Volume::new(child).unwrap(), &()).unwrap();
    storage.cache_volumes(&cid, &volumes).unwrap();
    let RootConnectionExecution::Rejected(rejected) =
        tree.execute_root_connection(&job, &storage, 2000).unwrap()
    else {
        panic!("invented post-state passed validation")
    };
    let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER refuse_exclusion BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(
        storage
            .import_root_rejection(&mut tree, rejected, "log")
            .is_err()
    );
    assert!(!tree.is_excluded_root(&cid));
    db.execute_batch("DROP TRIGGER refuse_exclusion").unwrap();
    let RootConnectionExecution::Rejected(rejected) =
        tree.execute_root_connection(&job, &storage, 2000).unwrap()
    else {
        panic!()
    };
    storage
        .import_root_rejection(&mut tree, rejected, "log")
        .unwrap();
    assert!(tree.is_excluded_root(&cid));
    assert_eq!(tree.executed_prefix(), Some(hash.as_str()));
    let (recovered, _) = storage
        .journal()
        .restore_chain(
            ChainRuntimeContext::new(vec!["Nexus".into()], Some(&hash)).unwrap(),
            None,
            vec![verified.spec().0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    assert!(recovered.is_excluded_root(&cid));
}
