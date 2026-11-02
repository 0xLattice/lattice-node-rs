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

#[tokio::test]
async fn explicit_peer_is_kept_after_initial_refusal_and_restart_without_author_restart() {
    use serde_json::json;
    let (cid, volumes) = fixture();
    let parent_dir = tempfile::tempdir().unwrap();
    let author_dir = tempfile::tempdir().unwrap();
    let lease = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = lease.local_addr().unwrap();
    let parent_key = ed25519_dalek::SigningKey::from_bytes(&[141; 32]);
    let parent_peer = ivy_core::PeerEndpoint {
        public_key: hex::encode(parent_key.verifying_key().to_bytes()),
        host: "127.0.0.1".into(),
        port: address.port(),
    };
    let parent_config = |bootstrap| HostConfig {
        directory: parent_dir.path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: if bootstrap { volumes.clone() } else { vec![] },
        signing_key: parent_key.clone(),
        listen: address,
        min_relay_fee: 1,
        seeds: vec![],
    };
    drop(lease);
    // Default 600-second discovery cadence and no named seed layer. The parent
    // is initially absent: only configured Ivy topology can retry this dial.
    let author = start(HostConfig {
        directory: author_dir.path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[142; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![parent_peer.clone()],
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(peers(&author).await.is_empty());
    let parent = start(parent_config(true)).await.unwrap();
    wait_peers(&author, &BTreeSet::from([parent_peer.public_key.clone()])).await;
    async fn mine(parent: &RunningHost) -> String {
        let (code, template) = parent
            .handle
            .request("/mining/templates", json!({}))
            .await
            .unwrap();
        assert_eq!(code, 200, "{template}");
        let mut block: lattice_primitives::Block =
            serde_json::from_value(template["block"].clone()).unwrap();
        let nonce = (0..100_000)
            .find(|nonce| {
                block.nonce = *nonce;
                block.has_valid_pow()
            })
            .unwrap();
        let (code, result) = parent
            .handle
            .request(
                "/mining/work",
                json!({"workID":template["workID"],"nonce":nonce}),
            )
            .await
            .unwrap();
        assert_eq!(code, 200, "{result}");
        assert_eq!(result["accepted"], true, "{result}");
        result["tipCID"].as_str().unwrap().into()
    }
    async fn synced(author: &RunningHost, tip: &str) {
        timeout(Duration::from_secs(15), async {
            loop {
                let (code, status) = author.handle.request("/status", Value::Null).await.unwrap();
                assert_eq!(code, 200);
                if status["tipCID"] == tip {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    synced(&author, &mine(&parent).await).await;
    stop(parent).await;
    wait_peers(&author, &BTreeSet::new()).await;
    // Reuse the parent's identity, durable journal and address. Mine while the
    // author remains alive; the new authenticated session must resume sync.
    let parent = start(parent_config(false)).await.unwrap();
    let tip = mine(&parent).await;
    synced(&author, &tip).await;
    let (_, status) = author.handle.request("/status", Value::Null).await.unwrap();
    assert_eq!(status["height"], "2");
    stop(author).await;
    stop(parent).await;
}
