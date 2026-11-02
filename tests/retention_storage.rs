use lattice_cashew::{SerializedVolume, Volume};
use lattice_node::storage::*;
use lattice_primitives::LatticeState;
use std::collections::{BTreeMap, BTreeSet};
fn volume(bytes: &[u8]) -> SerializedVolume {
    let cid = lattice_cashew::cid_for_bytes(0x55, bytes);
    SerializedVolume::new(cid.clone(), BTreeMap::from([(cid, bytes.to_vec())]))
}
fn roots(volumes: &[SerializedVolume]) -> BTreeSet<String> {
    volumes.iter().map(|v| v.root.clone()).collect()
}
#[test]
fn config_lease_replacement_validates_before_releasing_previous_content() {
    let dir = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(dir.path(), "genesis", "held").unwrap();
    let a = volume(b"a");
    let b = volume(b"b");
    storage
        .replace_cached_volumes("config", std::slice::from_ref(&a))
        .unwrap();
    let mut corrupt = b.clone();
    corrupt.entries.insert(b.root.clone(), b"wrong".to_vec());
    assert!(
        storage
            .replace_cached_volumes("config", &[corrupt])
            .is_err()
    );
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&a.root).is_some());
    storage
        .replace_cached_volumes("config", std::slice::from_ref(&b))
        .unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&a.root).is_none());
    assert!(storage.fetch_volume(&b.root).is_some());
}
#[test]
fn none_state_releases_previous_outputs_but_keeps_constants_and_other_owners() {
    let dir = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(dir.path(), "genesis", "held").unwrap();
    let values = [volume(b"old"), volume(b"new"), volume(b"constant")];
    storage.cache_volumes("stage", &values).unwrap();
    let path = vec!["Nexus".into()];
    storage
        .retain_execution_content(
            StateRetention::None,
            &path,
            &roots(&values[..1]),
            &roots(&values[2..]),
        )
        .unwrap();
    storage
        .retain_execution_content(
            StateRetention::None,
            &path,
            &roots(&values[1..2]),
            &BTreeSet::new(),
        )
        .unwrap();
    storage.release_cached_volumes("stage").unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&values[0].root).is_none());
    assert!(storage.fetch_volume(&values[1].root).is_some());
    assert!(storage.fetch_volume(&values[2].root).is_some());
    drop(storage);
    let storage = NodeStorage::open(dir.path(), "genesis", "held").unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&values[1].root).is_some());
    assert!(storage.fetch_volume(&values[2].root).is_some());
}

// These are trusted local journal records solely for retention tests, not block
// admission evidence. The state bytes/counts themselves are real typed volumes.
fn state_fact(storage: &NodeStorage, tip: &str, state: &str) {
    let batch = serde_json::from_value(serde_json::json!({"facts":[{"block":{"_0":{
        "blockHash":tip,"blockHeight":0,"postStateCID":state,"prevStateCID":state,
        "parentStateCID":state,"specCID":state,"target":"ff","nextTarget":"ff",
        "timestamp":0,"stateDiff":{"replaced":{},"created":{}}
    }}}]}))
    .unwrap();
    storage
        .journal()
        .stage_node_facts(
            &[NodeFactBatch {
                path: vec!["Nexus".into()],
                facts: vec![batch],
                headers: vec![],
                proofs: vec![],
                volume_roots: vec![],
                cursors: BTreeMap::new(),
            }],
            "log",
        )
        .unwrap();
}
#[test]
fn tip_counts_release_old_roots_and_keep_shared_subtrees_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open_with_retention(
        dir.path(),
        "genesis",
        "held",
        RetentionPolicy {
            state: StateRetention::Tip,
            bodies: BodyRetention::All,
        },
    )
    .unwrap();
    let a = Volume::new(LatticeState::empty().unwrap()).unwrap();
    let mut state = LatticeState::empty().unwrap();
    state.account_state = Volume::new(
        state
            .account_state
            .node
            .as_ref()
            .unwrap()
            .inserting("account", 7)
            .unwrap(),
    )
    .unwrap();
    let b = Volume::new(state).unwrap();
    let va = lattice_cashew::volume::serialize_recursive(&a, &()).unwrap();
    let vb = lattice_cashew::volume::serialize_recursive(&b, &()).unwrap();
    let all: Vec<_> = va.iter().chain(vb.iter()).cloned().collect();
    storage.cache_volumes("stage", &all).unwrap();
    let path = vec!["Nexus".into()];
    let ta = lattice_cashew::cid_for_bytes(0x71, b"tip-a");
    let tb = lattice_cashew::cid_for_bytes(0x71, b"tip-b");
    state_fact(&storage, &ta, &a.raw_cid);
    state_fact(&storage, &tb, &b.raw_cid);
    storage
        .retain_execution_content(StateRetention::Tip, &path, &roots(&va), &BTreeSet::new())
        .unwrap();
    storage.keep_tip_state(&ta, &path).unwrap();
    let shared = a.node.as_ref().unwrap().account_state.raw_cid.clone();
    assert_eq!(storage.journal().state_count(&shared, &path).unwrap(), 4);
    storage
        .retain_execution_content(StateRetention::Tip, &path, &roots(&vb), &BTreeSet::new())
        .unwrap();
    // Counts and tip move in one SQL transaction. Failure after new content
    // is pinned leaves the old durable counts and both roots safely retained.
    let sql = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_tip BEFORE INSERT ON core_meta WHEN NEW.key='state_tip:Nexus' BEGIN SELECT RAISE(ABORT, 'injected count failure'); END;").unwrap();
    assert!(storage.keep_tip_state(&tb, &path).is_err());
    assert_eq!(
        storage.journal().state_tip(&path).unwrap().as_deref(),
        Some(ta.as_str())
    );
    assert_eq!(storage.journal().state_count(&shared, &path).unwrap(), 4);
    storage.release_cached_volumes("stage").unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&a.raw_cid).is_some());
    assert!(storage.fetch_volume(&b.raw_cid).is_some());
    sql.execute_batch("DROP TRIGGER fail_tip;").unwrap();
    storage.keep_tip_state(&tb, &path).unwrap();
    assert_eq!(storage.journal().state_count(&shared, &path).unwrap(), 3);
    storage.release_cached_volumes("stage").unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&a.raw_cid).is_none());
    assert!(storage.fetch_volume(&b.raw_cid).is_some());
    assert!(storage.fetch_volume(&shared).is_some());
    drop(storage);
    let storage = NodeStorage::open_with_retention(
        dir.path(),
        "genesis",
        "held",
        RetentionPolicy {
            state: StateRetention::Tip,
            bodies: BodyRetention::All,
        },
    )
    .unwrap();
    assert_eq!(
        storage.journal().state_tip(&path).unwrap().as_deref(),
        Some(tb.as_str())
    );
    storage.keep_tip_state(&tb, &path).unwrap();
    storage.sweep().unwrap();
    assert!(storage.fetch_volume(&b.raw_cid).is_some());
}
