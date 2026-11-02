use lattice_block_tree::BlockImportBatch;
use lattice_node::storage::*;
use std::collections::BTreeMap;
fn fixtures() -> Vec<BlockImportBatch> {
    include_str!("fixtures/swift-facts.jsonl")
        .lines()
        .filter(|l| l.starts_with("{\"facts\""))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn level(path: &[&str], facts: Vec<BlockImportBatch>) -> NodeFactBatch {
    NodeFactBatch {
        proofs: vec![],
        path: path.iter().map(|s| s.to_string()).collect(),
        facts,
        headers: vec![],
        volume_roots: vec!["root-b".into(), "root-a".into(), "root-b".into()],
        cursors: BTreeMap::new(),
    }
}
fn path() -> Vec<String> {
    vec!["Nexus".into()]
}
#[test]
fn actual_swift_fact_bytes_and_ids() {
    let mut lines = include_str!("fixtures/swift-facts.jsonl").lines();
    let mut count = 0;
    while let Some(line) = lines.next() {
        let batch: BlockImportBatch = serde_json::from_str(line).unwrap();
        assert_eq!(
            serde_json::to_string(&serde_json::to_value(&batch).unwrap()).unwrap(),
            line
        );
        for fact in batch.facts() {
            assert_eq!(
                serde_json::to_string(&serde_json::to_value(fact.id()).unwrap()).unwrap(),
                lines.next().unwrap()
            );
        }
        count += 1;
    }
    assert_eq!(count, 22);
}
#[test]
fn stage_reopen_idempotency_and_revisions() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("node.sqlite");
    let store = NodeStore::open(&file, "genesis").unwrap();
    let facts = fixtures();
    let mut l = level(&["Nexus"], facts.clone());
    l.cursors.insert(
        "peer".into(),
        StreamCursor {
            log_id: "remote".into(),
            position: 42,
        },
    );
    store.stage_node_facts(&[l.clone()], "first").unwrap();
    store.stage_node_facts(&[l], "ignored").unwrap();
    assert_eq!(store.revision_floor(&path()).unwrap(), 22);
    drop(store);
    let store = NodeStore::open(&file, "genesis").unwrap();
    assert_eq!(store.chain_log_id().unwrap().as_deref(), Some("first"));
    assert_eq!(store.import_batches(&path()).unwrap(), facts);
    assert_eq!(store.chain_cursors(&path()).unwrap()["peer"].position, 42);
    assert_eq!(
        store.staged_imports(None).unwrap()[0].volume_roots,
        vec!["root-a", "root-b"]
    );
}
#[test]
fn conflict_rolls_back_every_level_and_evidence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let fact = fixtures()[0].clone();
    store
        .stage_node_facts(&[level(&["Nexus"], vec![fact.clone()])], "initial")
        .unwrap();
    let mut changed = serde_json::to_value(&fact).unwrap();
    changed["facts"][0]["block"]["_0"]["timestamp"] = 999.into();
    let changed = serde_json::from_value(changed).unwrap();
    let mut child = level(&["Nexus", "Alpha"], vec![fact]);
    child.headers.push(StoredHeader {
        block_cid: "header".into(),
        block: vec![1],
        children_cid: "children".into(),
        children: vec![2],
    });
    let root = level(&["Nexus"], vec![changed]);
    assert!(matches!(
        store.stage_node_facts(&[child, root], "other"),
        Err(StoreError::ConflictingImportFact)
    ));
    assert_eq!(store.staged_imports(None).unwrap().len(), 1);
    assert_eq!(store.header_block_bytes("header").unwrap(), None);
}
#[test]
fn immutable_block_diff_may_fill_once() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let mut empty = serde_json::to_value(&fixtures()[0]).unwrap();
    empty["facts"][0]["block"]["_0"]["stateDiff"] = serde_json::json!({"replaced":{},"created":{}});
    let empty = serde_json::from_value(empty).unwrap();
    store
        .stage_node_facts(&[level(&["Nexus"], vec![empty])], "log")
        .unwrap();
    store
        .stage_node_facts(&[level(&["Nexus"], vec![fixtures()[0].clone()])], "log")
        .unwrap();
    let mut conflicting = serde_json::to_value(&fixtures()[0]).unwrap();
    conflicting["facts"][0]["block"]["_0"]["stateDiff"]["created"]["new"] = 2.into();
    assert!(
        store
            .stage_node_facts(
                &[level(
                    &["Nexus"],
                    vec![serde_json::from_value(conflicting).unwrap()]
                )],
                "log"
            )
            .is_err()
    );
    assert_eq!(store.revision_floor(&path()).unwrap(), 2);
}
#[test]
fn schema_and_genesis_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("db");
    drop(NodeStore::open(&file, "one").unwrap());
    assert!(matches!(
        NodeStore::open(&file, "two"),
        Err(StoreError::WipeRequired(_))
    ));
    let db = rusqlite::Connection::open(&file).unwrap();
    db.execute("UPDATE node_metadata SET schema_epoch=46", [])
        .unwrap();
    assert!(matches!(
        NodeStore::open(&file, "one"),
        Err(StoreError::WipeRequired(_))
    ));
}
#[test]
fn paged_recovery_keeps_every_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let facts: Vec<_> = (0..1100)
        .map(|i| BlockImportBatch::validation(format!("block/{i}")))
        .collect();
    store
        .stage_node_facts(&[level(&["Nexus"], facts.clone())], "log")
        .unwrap();
    assert_eq!(store.import_batches(&path()).unwrap(), facts);
}
#[test]
fn directory_lock_lifetime() {
    let tmp = tempfile::tempdir().unwrap();
    let lock = StorageDirectoryLock::acquire(tmp.path()).unwrap();
    assert!(matches!(
        StorageDirectoryLock::acquire(tmp.path()),
        Err(StoreError::DirectoryInUse)
    ));
    drop(lock);
    StorageDirectoryLock::acquire(tmp.path()).unwrap();
}
#[test]
fn mempool_journal_validates_preserves_first_time_and_orders() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let a = lattice_cashew::cid_for_bytes(0x71, b"a");
    let b = lattice_cashew::cid_for_bytes(0x71, b"b");
    store.persist_local_mempool_transaction(&a, 20).unwrap();
    store.persist_local_mempool_transaction(&a, 1).unwrap();
    store.persist_local_mempool_transaction(&b, 10).unwrap();
    assert_eq!(
        store
            .local_mempool_transactions()
            .unwrap()
            .iter()
            .map(|v| v.added_at)
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    assert!(store.persist_local_mempool_transaction("bad", 1).is_err());
    assert!(store.persist_local_mempool_transaction(&a, -1).is_err());
    store.remove_local_mempool_transaction(&a).unwrap();
    assert_eq!(store.local_mempool_transactions().unwrap().len(), 1);
}
#[test]
fn abrupt_uncommitted_writer_rolls_back() {
    const KEY: &str = "LATTICE_NODE_CRASH_DB";
    if let Ok(path) = std::env::var(KEY) {
        let db = rusqlite::Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE; INSERT INTO core_meta VALUES('partial','visible-before-crash'); INSERT INTO stream_cursors VALUES('Nexus','half','log',9);").unwrap();
        std::process::exit(73);
    }
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("db");
    let store = NodeStore::open(&file, "genesis").unwrap();
    store
        .stage_node_facts(
            &[level(&["Nexus"], vec![fixtures()[0].clone()])],
            "committed",
        )
        .unwrap();
    drop(store);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "abrupt_uncommitted_writer_rolls_back",
            "--nocapture",
        ])
        .env(KEY, &file)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let store = NodeStore::open(&file, "genesis").unwrap();
    assert_eq!(store.chain_log_id().unwrap().as_deref(), Some("committed"));
    assert!(store.chain_cursors(&path()).unwrap().is_empty());
    assert_eq!(store.revision_floor(&path()).unwrap(), 1);
    let db = rusqlite::Connection::open(file).unwrap();
    let n: i64 = db
        .query_row(
            "SELECT count(*) FROM core_meta WHERE key='partial'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);
}
#[test]
fn state_counts_atomic_overflow_and_reference_release() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let initial = BTreeMap::from([("a".into(), 2), ("z".into(), i64::MAX)]);
    assert_eq!(
        store
            .move_state_counts(&path(), "tip1", &initial)
            .unwrap()
            .entered,
        vec!["a", "z"]
    );
    let bad = BTreeMap::from([("a".into(), -2), ("z".into(), 1)]);
    assert!(store.move_state_counts(&path(), "tip2", &bad).is_err());
    assert_eq!(store.state_count("a", &path()).unwrap(), 2);
    assert_eq!(store.state_tip(&path()).unwrap().as_deref(), Some("tip1"));
    assert_eq!(
        store
            .move_state_counts(&path(), "tip2", &BTreeMap::from([("a".into(), -3)]))
            .unwrap()
            .left,
        vec!["a"]
    );
    assert_eq!(store.state_count("a", &path()).unwrap(), 0);
    store.forget_state_counts().unwrap();
    assert_eq!(store.state_tip(&path()).unwrap(), None);
}
#[test]
fn durable_facts_restore_shared_graph_and_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let store = NodeStore::open(tmp.path().join("db"), "genesis").unwrap();
    let c = |s: &str| lattice_cashew::cid_for_bytes(0x71, s.as_bytes());
    let root = c("genesis");
    let fact = serde_json::json!({"facts":[{"block":{"_0":{"blockHash":root,"blockHeight":0,"postStateCID":c("post"),"prevStateCID":c("prev"),"parentStateCID":c("parent"),"specCID":c("spec"),"target":"ff","nextTarget":"ff","timestamp":0,"stateDiff":{"replaced":{},"created":{}}}}},{"work":{"_0":{"blockHash":root,"contribution":{"id":c("grind"),"work":"0x1"}}}},{"validation":{"_0":{"blockHash":root}}}]});
    let fact: BlockImportBatch = serde_json::from_value(fact).unwrap();
    store
        .stage_node_facts(
            &[level(
                &["Nexus"],
                vec![fact, BlockImportBatch::validation(root.clone())],
            )],
            "log",
        )
        .unwrap();
    let context = lattice_primitives::ChainRuntimeContext::new(path(), Some(&root)).unwrap();
    let (tree, snapshot) = store.restore_chain(context, None, vec![]).unwrap();
    assert_eq!(tree.canonical_tip(), root);
    assert_eq!(tree.current_revision(), 2);
    assert!(tree.is_executed(&root));
    assert_eq!(snapshot.revision_floor, 2);
}
#[test]
fn content_first_persistence_retains_before_journal_and_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), "genesis", "node-held").unwrap();
    let bytes = b"materialized state".to_vec();
    let root = lattice_cashew::cid_for_bytes(0x55, &bytes);
    let volume = lattice_cashew::SerializedVolume::new(
        root.clone(),
        BTreeMap::from([(root.clone(), bytes)]),
    );
    let mut batch = level(&["Nexus"], vec![fixtures()[0].clone()]);
    batch.volume_roots = vec![root.clone()];
    storage
        .persist_materialized_batch(&[batch], std::slice::from_ref(&volume), "log")
        .unwrap();
    assert_eq!(storage.sweep().unwrap(), 0);
    assert_eq!(storage.fetch_volume(&root), Some(volume.clone()));
    assert!(storage.retained_roots().unwrap().contains(&root));
    drop(storage);
    let storage = NodeStorage::open(tmp.path(), "genesis", "node-held").unwrap();
    assert_eq!(storage.fetch_volume(&root), Some(volume));
    assert_eq!(
        storage.journal().staged_imports(None).unwrap()[0].volume_roots,
        vec![root]
    );
}
#[test]
fn invalid_content_cannot_stage_facts() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), "genesis", "node-held").unwrap();
    let root = lattice_cashew::cid_for_bytes(0x55, b"expected");
    let invalid = lattice_cashew::SerializedVolume::new(
        root.clone(),
        BTreeMap::from([(root, b"wrong".to_vec())]),
    );
    assert!(
        storage
            .persist_materialized_batch(
                &[level(&["Nexus"], vec![fixtures()[0].clone()])],
                &[invalid],
                "log"
            )
            .is_err()
    );
    assert!(!storage.journal().has_staged_imports().unwrap());
    assert!(storage.retained_roots().unwrap().is_empty());
}
#[test]
fn saved_proofs_are_path_bound_and_bad_rows_are_removed() {
    use lattice_block_tree::ChildBlockProof;
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("db");
    let store = NodeStore::open(&file, "genesis").unwrap();
    let root = lattice_cashew::cid_for_bytes(0x71, b"root");
    let child = lattice_cashew::cid_for_bytes(0x71, b"child");
    let proof = ChildBlockProof::new(root.clone(), vec!["Alpha".into()], vec![]);
    let mut batch = level(&["Nexus", "Alpha"], vec![]);
    batch.proofs = vec![StoredProof {
        child_cid: child.clone(),
        proof: proof.clone(),
    }];
    store.stage_node_facts(&[batch.clone()], "log").unwrap();
    let path = vec!["Nexus".into(), "Alpha".into()];
    assert_eq!(
        store
            .saved_proofs(&path, std::slice::from_ref(&child))
            .unwrap()[&child],
        vec![proof]
    );
    let db = rusqlite::Connection::open(file).unwrap();
    db.execute("UPDATE child_proofs SET bytes=?1", [vec![0u8]])
        .unwrap();
    assert!(
        store
            .saved_proofs(&path, std::slice::from_ref(&child))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM child_proofs", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    batch.path = vec!["Nexus".into(), "Beta".into()];
    assert!(store.stage_node_facts(&[batch], "log").is_err());
}
