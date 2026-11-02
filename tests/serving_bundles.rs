use lattice_cashew::SerializedVolume;
use lattice_node::storage::NodeStorage;
use serde_json::Value;
fn rows() -> Vec<Value> {
    serde_json::from_str(include_str!("serving-bundle-swift.json")).unwrap()
}
#[test]
fn selected_bundle_roots_match_actual_swift_without_admission_history() {
    for row in rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
        let volumes: Vec<SerializedVolume> =
            serde_json::from_value(row["volumes"].clone()).unwrap();
        store.cache_volumes("test", &volumes).unwrap();
        let root = row["root"].as_str().unwrap();
        let expected: Vec<String> = serde_json::from_value(row["swift"].clone()).unwrap();
        assert_eq!(
            store.serving_bundle(root).unwrap(),
            expected,
            "{}",
            row["name"]
        );
        assert_eq!(
            store.serving_bundle(root).unwrap(),
            expected,
            "cached {}",
            row["name"]
        );
        drop(store);
        let reopened = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
        assert_eq!(
            reopened.serving_bundle(root).unwrap(),
            expected,
            "reopened {}",
            row["name"]
        );
    }
}
#[test]
fn disposable_corrupt_or_missing_cache_recomputes_and_incomplete_walk_is_not_cached() {
    let rows = rows();
    let row = rows.iter().find(|r| r["name"] == "combined").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let root = row["root"].as_str().unwrap();
    let expected: Vec<String> = serde_json::from_value(row["swift"].clone()).unwrap();
    let volumes: Vec<SerializedVolume> = serde_json::from_value(row["volumes"].clone()).unwrap();
    let store = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
    // An unavailable source must not poison the cache before content arrives.
    assert_eq!(store.serving_bundle(root).unwrap(), vec![root]);
    store.cache_volumes("test", &volumes).unwrap();
    assert_eq!(store.serving_bundle(root).unwrap(), expected);
    let db = rusqlite::Connection::open(dir.path().join("volume-bundles.db")).unwrap();
    db.execute(
        "UPDATE volume_bundles SET roots=?1 WHERE root=?2",
        rusqlite::params![b"invalid-json".as_slice(), root],
    )
    .unwrap();
    assert_eq!(store.serving_bundle(root).unwrap(), expected);
    drop(db);
    drop(store);
    std::fs::remove_file(dir.path().join("volume-bundles.db")).unwrap();
    let store = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
    assert_eq!(store.serving_bundle(root).unwrap(), expected);
    drop(store);
    // Unopenable sidecar is an optimization failure only.
    std::fs::remove_file(dir.path().join("volume-bundles.db")).unwrap();
    std::fs::create_dir(dir.path().join("volume-bundles.db")).unwrap();
    let store = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
    assert_eq!(store.serving_bundle(root).unwrap(), expected);
}
