use lattice_node::runtime::peer_memory::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
fn root() -> Vec<String> {
    vec!["Nexus".into()]
}
fn key(n: u8) -> String {
    hex::encode(
        ed25519_dalek::SigningKey::from_bytes(&[n; 32])
            .verifying_key()
            .to_bytes(),
    )
}
fn endpoint(n: u8) -> ivy_core::PeerEndpoint {
    ivy_core::PeerEndpoint {
        public_key: key(n),
        host: format!("127.{n}.0.1"),
        port: 7447,
    }
}
#[test]
fn unchanged_swift_peer_book_observations() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/swift-peer-memory.json")).unwrap();
    let mut book = PeerBook::default();
    for (i, row) in fixture["rows"].as_array().unwrap().iter().enumerate() {
        let now = row["now"].as_i64().unwrap();
        let keys: BTreeSet<String> = serde_json::from_value(row["keys"].clone()).unwrap();
        let entries: Vec<Entry> = serde_json::from_value(row["entries"].clone()).unwrap();
        match row["op"].as_str().unwrap() {
            "load" => book = PeerBook::from_entries(entries),
            "reached" => book.reached(
                &entries.iter().map(Entry::endpoint).collect::<Vec<_>>(),
                &keys,
                now,
            ),
            "open" => book.still_open(&keys, now),
            "fail" => {
                for key in keys {
                    book.failed(&key);
                }
            }
            _ => book.prune(now),
        }
        let groups: Vec<_> = row["hosts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| group(v.as_str().unwrap()))
            .collect();
        assert!(
            json!({"entries":book.entries.values().collect::<Vec<_>>(),"groups":groups})
                == fixture["expected"][i],
            "observation {i}; entries={}, expected={}",
            book.entries.len(),
            fixture["expected"][i]["entries"].as_array().unwrap().len()
        );
    }
}
#[test]
fn reached_endpoints_survive_atomic_restart_with_chain_books_and_exclusions() {
    let directory = tempfile::tempdir().unwrap();
    let child = vec!["Nexus".into(), "Alpha".into()];
    let paths = vec![root(), child.clone()];
    let excluded = BTreeSet::from([key(2)]);
    let mut memory = PeerMemory::open(directory.path(), &paths, excluded.clone(), 0);
    memory.reached(endpoint(1), &[root()], 1);
    memory.reached(endpoint(2), &paths, 1);
    memory.still_open(
        &BTreeMap::from([
            (root(), BTreeSet::from([key(1), key(2), key(3)])),
            (child.clone(), BTreeSet::from([key(1), key(3)])),
        ]),
        2,
    );
    assert_eq!(memory.remembered(&root()), vec![endpoint(1)]);
    assert_eq!(memory.remembered(&child), vec![endpoint(1)]);
    let file = directory.path().join("peers.json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    drop(memory);
    let mut restored = PeerMemory::open(directory.path(), &[root()], excluded, 3);
    restored.track(child.clone(), 3);
    assert_eq!(restored.remembered(&root()), vec![endpoint(1)]);
    assert_eq!(restored.remembered(&child), vec![endpoint(1)]);
    assert!(
        restored
            .candidates(&root(), &BTreeSet::from([key(1)]), 4)
            .is_empty()
    );
    for _ in 0..10 {
        restored.failed(&key(1));
    }
    restored.still_open(&BTreeMap::new(), 4);
    assert!(
        PeerMemory::open(directory.path(), &paths, BTreeSet::new(), 5)
            .remembered(&root())
            .is_empty()
    );
}
#[test]
fn malformed_oversized_and_unwritable_memory_is_advisory_and_v1_migrates() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("peers.json");
    for bytes in [
        b"not json".to_vec(),
        vec![b' '; MAX_FILE_BYTES as usize + 1],
    ] {
        std::fs::write(&file, bytes).unwrap();
        assert!(
            PeerMemory::open(dir.path(), &[root()], BTreeSet::new(), 0)
                .remembered(&root())
                .is_empty()
        );
    }
    std::fs::write(&file,serde_json::to_vec(&json!({"version":1,"peers":[{"publicKey":key(1),"host":"127.1.0.1","port":7447,"lastSeen":0,"failures":0}]})).unwrap()).unwrap();
    let mut memory = PeerMemory::open(dir.path(), &[root()], BTreeSet::new(), 1);
    assert_eq!(memory.remembered(&root()), vec![endpoint(1)]);
    memory.still_open(&BTreeMap::from([(root(), BTreeSet::from([key(1)]))]), 2);
    let saved: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!(saved["version"], 2);
    let mut unavailable =
        PeerMemory::open(&dir.path().join("absent"), &[root()], BTreeSet::new(), 0);
    unavailable.reached(endpoint(1), &[root()], 1);
    unavailable.still_open(&BTreeMap::new(), 2);
    assert_eq!(unavailable.remembered(&root()), vec![endpoint(1)]);
}
