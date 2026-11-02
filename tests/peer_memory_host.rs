//! Real localhost hosts: remember successful direct dials, rejoin before asking
//! seeds, fall back after failed memory, and respect an isolated restart.
use lattice_cashew::{Header, Volume};
use lattice_node::runtime::host::*;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::{Duration, timeout};
fn fixture() -> (String, Vec<lattice_cashew::SerializedVolume>) {
    let rows: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let entries: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[0]["entries"].clone()).unwrap();
    let cid = rows[0]["cid"].as_str().unwrap().to_owned();
    let verified = lattice_validation::validate_root_genesis(
        &Volume(Header::from_cid(&cid).unwrap()),
        &entries,
        i64::MAX,
    )
    .unwrap();
    (
        cid,
        lattice_cashew::volume::serialize_recursive(verified.header(), &()).unwrap(),
    )
}
fn endpoint(host: &RunningHost) -> ivy_core::PeerEndpoint {
    ivy_core::PeerEndpoint {
        public_key: host.public_key.clone(),
        host: "127.0.0.1".into(),
        port: host.overlay_addr.port(),
    }
}
async fn peers(host: &RunningHost) -> BTreeSet<String> {
    let (status, value) = host
        .handle
        .request("/api/peers", Value::Null)
        .await
        .unwrap();
    assert_eq!(status, 200);
    value["peers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"].as_str().unwrap().into())
        .collect()
}
async fn wait_peers(host: &RunningHost, expected: &BTreeSet<String>) {
    timeout(Duration::from_secs(15), async {
        loop {
            if peers(host).await == *expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn stop(host: RunningHost) {
    host.handle.shutdown().await;
    host.task.await.unwrap().unwrap();
}
#[tokio::test]
async fn actual_hosts_rejoin_from_memory_then_fall_back_and_isolated_start_ignores_book() {
    let (cid, volumes) = fixture();
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = |index: usize, bootstrap: bool| HostConfig {
        directory: dirs[index].path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: if bootstrap { volumes.clone() } else { vec![] },
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[81 + index as u8; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    };
    let one = start(config(0, true)).await.unwrap();
    let two = start(config(1, true)).await.unwrap();
    let unused = start(config(2, true)).await.unwrap();
    let policy = |seeds: Vec<ivy_core::PeerEndpoint>| DiscoveryConfig {
        seed_peers: seeds,
        peer_search_interval: 1.0,
        ..Default::default()
    };
    let node = start_with_policies(
        config(3, true),
        Default::default(),
        policy(vec![endpoint(&one), endpoint(&two)]),
    )
    .await
    .unwrap();
    wait_peers(
        &node,
        &BTreeSet::from([one.public_key.clone(), two.public_key.clone()]),
    )
    .await;
    let file = dirs[3].path().join("peers.json");
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(data) = std::fs::read(&file)
                && let Ok(value) = serde_json::from_slice::<Value>(&data)
                && value["chains"]["Nexus"]
                    .as_array()
                    .is_some_and(|v| v.len() == 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop(node).await;
    let node = start_with_policies(
        config(3, false),
        Default::default(),
        policy(vec![endpoint(&unused)]),
    )
    .await
    .unwrap();
    wait_peers(
        &node,
        &BTreeSet::from([one.public_key.clone(), two.public_key.clone()]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        peers(&unused).await.is_empty(),
        "enough remembered peers must suppress seed contact"
    );
    stop(node).await;
    stop(one).await;
    stop(two).await;
    let node = start_with_policies(
        config(3, false),
        Default::default(),
        policy(vec![endpoint(&unused)]),
    )
    .await
    .unwrap();
    wait_peers(&node, &BTreeSet::from([unused.public_key.clone()])).await;
    stop(node).await;
    let node = start_with_policies(config(3, false), Default::default(), policy(vec![]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        peers(&node).await.is_empty(),
        "an isolated node must not use the existing peer book"
    );
    stop(node).await;
    stop(unused).await;
}
#[tokio::test]
async fn failed_named_seed_falls_back_to_fixed_loopback_address() {
    let (cid, volumes) = fixture();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let config = |directory, key| HostConfig {
        directory,
        genesis_cid: cid.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[key; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    };
    let standing = start(config(a.path().into(), 91)).await.unwrap();
    let fixed = endpoint(&standing);
    let mut failed = fixed.clone();
    // A reserved, then closed localhost listener gives a deterministic refused
    // dial without DNS/public-network activity or an arbitrary port guess.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    failed.port = listener.local_addr().unwrap().port();
    drop(listener);
    let node = start_with_policies(
        config(b.path().into(), 92),
        Default::default(),
        DiscoveryConfig {
            seed_peers: vec![failed],
            fixed_seed_peers: vec![fixed],
            peer_search_interval: 1.0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    wait_peers(&node, &BTreeSet::from([standing.public_key.clone()])).await;
    stop(node).await;
    stop(standing).await;
}

fn child_hosting(seeds: &[ivy_core::PeerEndpoint]) -> Value {
    serde_json::json!({
        "hostChain": ["Nexus/Alpha", "Nexus/Alpha/Leaf"],
        "chainPeer": seeds.iter().map(|peer| format!("Nexus/Alpha/Leaf={}@{}:{}",peer.public_key,peer.host,peer.port)).collect::<Vec<_>>()
    })
}
#[tokio::test]
async fn child_only_seeds_enable_all_initial_books_and_remembered_peers_precede_new_child_seeds() {
    let (cid, volumes) = fixture();
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let config = |index: usize, bootstrap: bool| HostConfig {
        directory: dirs[index].path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: if bootstrap { volumes.clone() } else { vec![] },
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[101 + index as u8; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    };
    let policy = || DiscoveryConfig {
        peer_search_interval: 1.0,
        ..Default::default()
    };
    let mut standing = vec![];
    for index in 0..3 {
        standing.push(
            start_with_hosted_configuration(
                config(index, true),
                Default::default(),
                policy(),
                Default::default(),
                child_hosting(&[]),
            )
            .await
            .unwrap(),
        );
    }
    let initial = vec![endpoint(&standing[0]), endpoint(&standing[1])];
    let next_seed = endpoint(&standing[2]);
    let expected = initial.iter().map(|e| e.public_key.clone()).collect();
    let node = start_with_hosted_configuration(
        config(3, true),
        Default::default(),
        policy(),
        Default::default(),
        child_hosting(&initial),
    )
    .await
    .unwrap();
    wait_peers(&node, &expected).await;
    let file = dirs[3].path().join("peers.json");
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(bytes) = std::fs::read(&file)
                && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
                && ["Nexus", "Nexus/Alpha", "Nexus/Alpha/Leaf"]
                    .iter()
                    .all(|path| {
                        value["chains"][*path]
                            .as_array()
                            .is_some_and(|entries| entries.len() == 2)
                    })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Updating a seed on unchanged hosted paths is a maintenance wakeup, not an
    // unconditional dial. The existing two child hosts satisfy the threshold.
    let (code, response) = node
        .handle
        .request(
            "POST /hosted-chains",
            child_hosting(std::slice::from_ref(&next_seed)),
        )
        .await
        .unwrap();
    assert_eq!(code, 200, "{response}");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(peers(&standing[2]).await.is_empty());
    stop(node).await;
    // Root and both child books must load before any root-only maintenance save.
    let node = start_with_hosted_configuration(
        config(3, false),
        Default::default(),
        policy(),
        Default::default(),
        child_hosting(std::slice::from_ref(&next_seed)),
    )
    .await
    .unwrap();
    wait_peers(&node, &expected).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        peers(&standing[2]).await.is_empty(),
        "remembered child hosts must suppress the new seed"
    );
    let saved: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!(saved["chains"].as_object().unwrap().len(), 3);
    stop(node).await;
    let unused = standing.pop().unwrap();
    for host in standing {
        stop(host).await;
    }
    let node = start_with_hosted_configuration(
        config(3, false),
        Default::default(),
        policy(),
        Default::default(),
        child_hosting(std::slice::from_ref(&next_seed)),
    )
    .await
    .unwrap();
    wait_peers(&node, &BTreeSet::from([unused.public_key.clone()])).await;
    stop(node).await;
    // With no initial outbound choices, existing books remain dormant even
    // though the node still hosts both child paths.
    let node = start_with_hosted_configuration(
        config(3, false),
        Default::default(),
        policy(),
        Default::default(),
        child_hosting(&[]),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(peers(&node).await.is_empty());
    stop(node).await;
    stop(unused).await;
}
