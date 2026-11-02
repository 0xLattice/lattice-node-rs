//! Real authenticated Ivy peer; no received admission facts are injected.
use lattice_block_tree::ChildBlockProof;
use lattice_cashew::{Header, MerkleDictionary, Volume, VolumeMerkleDictionary, cbor};
use lattice_node::{
    core::{sync::*, wire},
    runtime::{
        host::{HostConfig, start},
        network::{Network, NetworkEvent},
    },
    storage::NodeStorage,
};
use lattice_primitives::*;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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

async fn scenario(forged: bool) {
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
    let root = parent;
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    let root_cid = content_id(&root).unwrap();
    let mut child = self::root();
    child.parent_state = root.post_state.clone();
    let child_cid = content_id(&child).unwrap();
    let mut carrier = self::root();
    carrier.prev_state = root.post_state.clone();
    carrier.children = Header::new(BTreeMap::from([(
        "Alpha".into(),
        Volume::new(child.clone()).unwrap(),
    )]))
    .unwrap();
    let carrier_cid = content_id(&carrier).unwrap();
    let source = BTreeMap::from([
        (carrier_cid.clone(), cbor::encode(&carrier).unwrap()),
        (
            carrier.children.raw_cid.clone(),
            cbor::encode(carrier.children.node.as_deref().unwrap()).unwrap(),
        ),
        (child_cid.clone(), cbor::encode(&child).unwrap()),
    ]);
    let mut proof = ChildBlockProof::generate(&carrier_cid, "Alpha", &source).unwrap();
    let path = vec!["Nexus".into(), "Alpha".into()];
    lattice_validation::validate_child_genesis(
        &Volume::new(child.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
        &path,
    )
    .unwrap();
    assert!(proof.verify_securing_work(&child, &path).is_ok());
    if forged {
        proof.directory_path[0] = "Beta".into();
    }
    assert_eq!(proof.verify_securing_work(&child, &path).is_err(), forged);
    let target_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(NodeStorage::open(peer_dir.path(), &root_cid, "peer").unwrap());
    let volumes =
        lattice_cashew::volume::serialize_recursive(&Volume::new(child.clone()).unwrap(), &())
            .unwrap();
    storage
        .persist_materialized_batch(&[], &volumes, "content-only")
        .unwrap();
    let target = start(HostConfig {
        directory: target_dir.path().into(),
        genesis_cid: root_cid.clone(),
        bootstrap_volumes: lattice_cashew::volume::serialize_recursive(genesis.header(), &())
            .unwrap(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[99; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    })
    .await
    .unwrap();
    assert_eq!(
        target
            .handle
            .request(
                "POST /hosted-chains",
                serde_json::json!({"hostChain":["Nexus/Alpha"]})
            )
            .await
            .unwrap()
            .0,
        200
    );
    let mut peer = Network::bind(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[98; 32]),
        root_cid,
        storage,
    )
    .await
    .unwrap();
    peer.set_hosted(std::slice::from_ref(&path)).await.unwrap();
    peer.connect(ivy_core::PeerEndpoint {
        public_key: target.public_key.clone(),
        host: "127.0.0.1".into(),
        port: target.overlay_addr.port(),
    })
    .await
    .unwrap();
    let mut sent = false;
    let mut authenticated = false;
    let mut disconnected = false;
    tokio::time::timeout(Duration::from_secs(15),async {
        loop {
            tokio::select! {
                event = peer.next() => match event.unwrap().unwrap() {
                    NetworkEvent::Ready(_) => authenticated = true,
                    NetworkEvent::Closed(_) if sent && forged => {disconnected=true;break},
                    NetworkEvent::Message(id,topic,bytes) => {
                        if let Some((chain,message)) = wire::decode(&topic,&bytes).unwrap() {

                            let reply = match message {
                                SyncMessage::GetStream{request_id,..} => Some(SyncMessage::Stream(StreamPage{request_id,log_id:"adversarial-peer".into(),entries:if chain == path {vec![StreamEntry{position:1,entry:LogEntry::header(&child_cid)}]}else{vec![]},has_more:false})),
                                SyncMessage::GetData{request_id,..} if chain == path => {
                                    sent=true;
                                    Some(SyncMessage::Headers(HeadersResponse{request_id,entries:vec![HeaderEntry{block:child.clone(),children:Some(BTreeMap::new()),proofs:vec![proof.clone()],spec:child.spec.0.node.as_deref().cloned()}],has_more:false}))
                                },
                                _ => None,
                            };
                            if let Some(reply)=reply {
                                let(topic,bytes)=wire::encode(&reply,&chain).unwrap();
                                // Decode round-trip proves this is canonical wire data,
                                // not rejection caused by a malformed envelope.
                                assert!(wire::decode(topic,&bytes).unwrap().is_some());
                                peer.send(id,topic.into(),bytes,0).await.unwrap();
                            }
                        }
                    },
                    _ => {},
                },
                _ = tokio::time::sleep(Duration::from_millis(20)) => {
                    peer.tick().await.unwrap();
                    if !forged && sent {
                        let(status,value)=target.handle.request("/status?chainPath=Nexus%2FAlpha",serde_json::json!({})).await.unwrap();

                        if status == 200 && value["tipCID"] == child_cid {break;}
                    }
                },
            }
        }
    }).await.unwrap();
    assert!(authenticated && sent);
    assert_eq!(disconnected, forged);
    target.handle.shutdown().await;
    target.task.await.unwrap().unwrap();
    peer.stop().await;
    let stored = NodeStorage::open(target_dir.path(), content_id(&root).unwrap(), "node").unwrap();
    assert_eq!(
        stored
            .journal()
            .has_executed_block(&child_cid, &path)
            .unwrap(),
        !forged
    );
    if forged {
        assert!(
            stored
                .journal()
                .saved_proofs(&path, std::slice::from_ref(&child_cid))
                .unwrap()
                .get(&child_cid)
                .is_none_or(Vec::is_empty)
        );
    }
}
#[tokio::test]
async fn canonical_child_proof_is_admitted_from_authenticated_peer() {
    scenario(false).await;
}
#[tokio::test]
async fn wrong_directory_child_proof_disconnects_authenticated_peer() {
    scenario(true).await;
}
