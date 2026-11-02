use lattice_block_tree::{ChainTree, ChildBlockProof};
use lattice_cashew::{Header, MerkleDictionary, Volume, VolumeMerkleDictionary, cbor};
use lattice_node::{
    core::{chain::RootChainCore, stream::StreamConfig},
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
#[tokio::test]
async fn hosted_actor_syncs_proof_bound_child_and_recovers() {
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
    let other = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "node").unwrap();
    let storage = std::sync::Arc::new(storage);
    let root_core =
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

    let mut tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
    storage
        .import_child_genesis(&mut tree, root_core.tree(), &child_genesis, &proof, "log")
        .unwrap();
    let child_cid = content_id(&child).unwrap();
    let volumes = lattice_cashew::volume::serialize_recursive(genesis.header(), &()).unwrap();
    drop(root_core);
    drop(storage);
    use lattice_node::runtime::host::{HostConfig, start};
    use serde_json::json;
    let config = |directory, key, seeds| HostConfig {
        directory,
        genesis_cid: hash.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[key; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds,
    };
    let a = start(config(tmp.path().into(), 81, vec![])).await.unwrap();
    let initial = a.handle.health("/health");
    assert_eq!(initial.0, 200);
    assert_eq!(initial.1["tipCID"], hash);
    assert_eq!(initial.1["height"], "0");
    assert!(initial.1.get("revision").is_none());
    assert!(initial.1.get("templateDigest").is_none());
    assert_eq!(a.handle.health("/health?chainPath=Nexus/Alpha").0, 404);
    let b = start(config(
        other.path().into(),
        82,
        vec![ivy_core::PeerEndpoint {
            public_key: a.public_key.clone(),
            host: "127.0.0.1".into(),
            port: a.overlay_addr.port(),
        }],
    ))
    .await
    .unwrap();
    for host in [&a, &b] {
        assert_eq!(
            host.handle
                .request("POST /hosted-chains", json!({"hostChain":["Nexus/Alpha"]}))
                .await
                .unwrap()
                .0,
            200
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if b.handle.volume(&child_cid).await.unwrap().is_some()
                && b.handle
                    .request("/api/chain/info?chainPath=Nexus%2FAlpha", json!({}))
                    .await
                    .unwrap()
                    .1["acceptsSubmit"]
                    == true
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (status, info) = b
        .handle
        .request("/api/chain/info?chainPath=Nexus%2FAlpha", json!({}))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        info["acceptsSubmit"], true,
        "execution must initialize the pool even when the selected header revision is unchanged"
    );
    for route in [
        "/api/deposit-state?chainPath=Nexus%2FAlpha&key=alice%2F1%2F0",
        "/api/receipt-state?chainPath=Nexus%2FAlpha%2FBeta&demander=alice&amount=1&nonce=0",
    ] {
        let (status, response) = b.handle.request(route, json!({})).await.unwrap();
        assert_eq!(status, 200, "{route}: {response}");
        let proof: lattice_light_client::StateDictionaryProof =
            serde_json::from_value(response["proof"].clone()).unwrap();
        assert_eq!(proof.block_hash, child_cid);
        assert!(lattice_light_client::verify_dictionary(&proof));
    }
    assert_eq!(
        b.handle
            .request(
                "POST /hosted-chains",
                json!({"hostChain":["Nexus/Alpha/Beta"]})
            )
            .await
            .unwrap()
            .0,
        400
    );
    assert_eq!(
        b.handle
            .request("/hosted-chains", json!({}))
            .await
            .unwrap()
            .1,
        json!({"chains":["Nexus/Alpha"]})
    );
    assert_eq!(
        b.handle
            .request(
                "POST /hosted-chains",
                json!({"hostChain":["Nexus/Alpha","Nexus/Beta"]})
            )
            .await
            .unwrap()
            .0,
        200
    );
    let (status, waiting) = b
        .handle
        .request("/status?chainPath=Nexus%2FBeta", json!({}))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert_eq!(waiting["phase"], "awaitingGenesis");
    let health = b.handle.health("/health?chainPath=Nexus/Beta");
    assert_eq!(health.0, 200);
    assert_eq!(health.1["phase"], "awaitingGenesis");
    assert!(health.1.get("tipCID").is_none());
    assert_eq!(b.handle.health("/health?chainPath=Nexus//Beta").0, 404);
    assert!(waiting.get("tipCID").is_none());
    assert_eq!(
        b.handle
            .request("POST /hosted-chains", json!({"hostChain":[]}))
            .await
            .unwrap()
            .0,
        200
    );
    assert_eq!(
        b.handle
            .request("/hosted-chains", json!({}))
            .await
            .unwrap()
            .1,
        json!({"chains":[]})
    );
    assert_eq!(
        b.handle
            .request("POST /hosted-chains", json!({"hostChain":["Nexus/Alpha"]}))
            .await
            .unwrap()
            .0,
        200
    );
    b.handle.shutdown().await;
    b.task.await.unwrap().unwrap();
    a.handle.shutdown().await;
    a.task.await.unwrap().unwrap();
    let reopened = NodeStorage::open(other.path(), &hash, "node").unwrap();
    assert!(
        reopened
            .journal()
            .has_executed_block(&child_cid, &path)
            .unwrap()
    );
    assert!(
        !reopened
            .journal()
            .saved_proofs(&path, std::slice::from_ref(&child_cid))
            .unwrap()[&child_cid]
            .is_empty()
    );
    drop(reopened);
    let restored = lattice_node::runtime::host::start_with_hosted_configuration(
        config(other.path().into(), 82, vec![]),
        Default::default(),
        Default::default(),
        Default::default(),
        json!({"hostChain":["Nexus/Alpha"]}),
    )
    .await
    .unwrap();
    let health = restored.handle.health("/health?chainPath=Nexus/Alpha");
    assert_eq!(health.0, 200);
    assert_eq!(health.1["tipCID"], child_cid);
    assert_eq!(health.1["height"], "0");
    assert_eq!(
        restored.handle.health("/health?chainPath=Nexus/Beta").0,
        404
    );
    assert_eq!(
        restored
            .handle
            .request("POST /hosted-chains", json!({"hostChain":["Nexus/Alpha"]}))
            .await
            .unwrap()
            .0,
        200
    );
    assert_eq!(
        restored
            .handle
            .request("/hosted-chains", json!({}))
            .await
            .unwrap()
            .1,
        json!({"chains":["Nexus/Alpha"]})
    );
    restored.handle.shutdown().await;
    restored.task.await.unwrap().unwrap();
}
