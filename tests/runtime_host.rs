use lattice_cashew::{Header, Volume};
use lattice_node::runtime::host::*;
use lattice_primitives::Block;
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
#[tokio::test]
async fn real_host_mines_syncs_and_recovers_only_after_durable_execution() {
    let (cid, volumes) = fixture();
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let config = |directory: std::path::PathBuf, key: u8, seeds| HostConfig {
        directory,
        genesis_cid: cid.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[key; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds,
    };
    let a = start(config(a_dir.path().into(), 31, vec![]))
        .await
        .unwrap();
    let b = start(config(
        b_dir.path().into(),
        32,
        vec![ivy_core::PeerEndpoint {
            public_key: a.public_key.clone(),
            host: "127.0.0.1".into(),
            port: a.overlay_addr.port(),
        }],
    ))
    .await
    .unwrap();
    assert_eq!(
        a.handle
            .request("/hosted-chains", Value::Null)
            .await
            .unwrap(),
        (200, json!({"chains":[]}))
    );
    let (status, template) = a
        .handle
        .request("/mining/templates", json!({"recipients":[]}))
        .await
        .unwrap();
    assert_eq!(status, 200, "{template}");
    let block: Block = serde_json::from_value(template["block"].clone()).unwrap();
    let mut mined = block.clone();
    let nonce = (0..10000)
        .find(|nonce| {
            mined.nonce = *nonce;
            mined.has_valid_pow()
        })
        .unwrap();
    let (status, result) = a
        .handle
        .request(
            "/mining/work",
            json!({"workID":template["workID"],"nonce":nonce}),
        )
        .await
        .unwrap();
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["accepted"], true, "{result}");
    assert_eq!(result["disposition"], "canonicalized");
    let tip = result["tipCID"].as_str().unwrap().to_owned();
    assert_ne!(tip, cid);
    timeout(Duration::from_secs(10), async {
        loop {
            let (_, status) = b.handle.request("/status", Value::Null).await.unwrap();
            if status["tipCID"] == tip {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    a.handle.shutdown().await;
    b.handle.shutdown().await;
    a.task.await.unwrap().unwrap();
    b.task.await.unwrap().unwrap();
    let mut restart = config(b_dir.path().into(), 32, vec![]);
    restart.bootstrap_volumes.clear();
    let b = start(restart).await.unwrap();
    let (_, status) = b.handle.request("/status", Value::Null).await.unwrap();
    assert_eq!(status["tipCID"], tip);
    assert_eq!(status["height"], "1");
    b.handle.shutdown().await;
    b.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_journal_never_reports_success_or_restores_uncommitted_tip() {
    let (cid, volumes) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let config = || HostConfig {
        directory: dir.path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[41; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    };
    let host = start(config()).await.unwrap();
    for body in [
        json!({"rewards":null}),
        json!({"mode":null}),
        json!({"recipients":[{"chainPath":["Nexus","child"],"address":"a"}]}),
    ] {
        assert_eq!(
            host.handle
                .request("/mining/templates", body)
                .await
                .unwrap()
                .0,
            400
        );
    }
    let (code, template) = host
        .handle
        .request("/mining/templates", json!({}))
        .await
        .unwrap();
    assert_eq!(code, 200);
    let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_import BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'injected journal fault'); END;").unwrap();
    let response = host
        .handle
        .request(
            "/mining/work",
            json!({"workID":template["workID"],"nonce":0}),
        )
        .await;
    assert!(response.is_err());
    assert!(host.task.await.unwrap().is_err());
    db.execute_batch("DROP TRIGGER fail_import").unwrap();
    drop(db);
    let host = start(config()).await.unwrap();
    let (_, status) = host.handle.request("/status", Value::Null).await.unwrap();
    assert_eq!(status["tipCID"], cid);
    assert_eq!(status["height"], "0");
    host.handle.shutdown().await;
    host.task.await.unwrap().unwrap();
}

#[tokio::test]
async fn root_reads_operator_pins_binary_archive_and_credit_survive_restart() {
    use base64::Engine;
    let (cid, volumes) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let config = || HostConfig {
        directory: dir.path().into(),
        genesis_cid: cid.clone(),
        bootstrap_volumes: volumes.clone(),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[51; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![],
    };
    let host = start(config()).await.unwrap();
    // Merely held transaction content can be read but has no fabricated
    // canonical inclusion, even when its bytes are valid and pinned.
    let tx = Volume::new(lattice_primitives::Transaction {
        body: Header::new(lattice_primitives::TransactionBody {
            account_actions: vec![lattice_primitives::AccountAction {
                owner: "reader".into(),
                delta: -7,
            }],
            actions: vec![],
            deposit_actions: vec![],
            receipt_actions: vec![],
            withdrawal_actions: vec![],
            signers: vec![],
            nonce: u64::MAX,
            chain_path: vec!["Nexus".into()],
        })
        .unwrap(),
        signatures: BTreeMap::new(),
    })
    .unwrap();
    let volumes = lattice_cashew::volume::serialize_recursive(&tx, &()).unwrap();
    for volume in &volumes {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let entries: BTreeMap<_, _> = volume
            .entries
            .iter()
            .map(|(cid, bytes)| (cid.clone(), STANDARD.encode(bytes)))
            .collect();
        assert_eq!(
            host.handle
                .request("POST /pins", json!({"cid":volume.root,"entries":entries}))
                .await
                .unwrap()
                .0,
            200
        );
    }
    let (code, held) = host
        .handle
        .request(&format!("/api/transaction/{}", tx.0.raw_cid), Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200, "{held}");
    assert_eq!(held["nonce"], u64::MAX.to_string());
    assert_eq!(held["accountActions"][0]["delta"], "-7");
    assert!(held.get("blockHash").is_none());
    assert!(held.get("blockHeight").is_none());
    for volume in &volumes {
        host.handle
            .request(&format!("DELETE /pins/{}", volume.root), Value::Null)
            .await
            .unwrap();
    }
    let (code, info) = host
        .handle
        .request("/api/chain/info", Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(info["genesisHash"], cid);
    assert_eq!(info["acceptsSubmit"], true);
    assert_eq!(info["minRelayFee"], "1");
    assert!(info["height"].is_string());
    let (code, block) = host
        .handle
        .request("/api/block/0", Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(block["hash"], cid);
    assert_eq!(block["nonce"], "0");
    assert_eq!(block["rewardCredited"], "0");
    assert!(block.get("previousBlock").is_none());
    let (code, page) = host
        .handle
        .request("/api/blocks?limit=1", Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(page["blocks"].as_array().unwrap().len(), 1);
    assert!(page.get("nextBefore").is_none());
    let (code, account) = host
        .handle
        .request("/api/state/account/absent", Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(account["balance"], "0");
    assert_eq!(account["nonce"], "0");
    assert_eq!(
        host.handle
            .request("/api/chain/info?chainPath=Nexus%2Fchild", Value::Null)
            .await
            .unwrap()
            .0,
        404
    );
    assert_eq!(
        host.handle
            .request("/api/blocks?limit=-1", Value::Null)
            .await
            .unwrap()
            .0,
        400
    );
    for route in [
        "/api/deposits",
        "/api/deposit-state?key=alice%2F1%2F0",
        "/api/receipt-state?demander=alice&amount=1&nonce=0&chainPath=Nexus%2Fchild",
    ] {
        let (code, response) = host.handle.request(route, Value::Null).await.unwrap();
        assert_eq!(code, 200, "{response}");
        let proof = &response["proof"];
        let typed: lattice_light_client::StateDictionaryProof =
            serde_json::from_value(proof.clone()).unwrap();
        assert!(lattice_light_client::verify_dictionary(&typed));
        assert_eq!(proof["blockHash"], cid);
        assert_eq!(proof["blockHeight"], 0);
        for node in proof["witness"].as_array().unwrap() {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(node["data"].as_str().unwrap())
                .unwrap();
            lattice_cashew::validate_entry(node["cid"].as_str().unwrap(), &bytes).unwrap();
        }
        if route != "/api/deposits" {
            assert_eq!(response["exists"], false);
            assert!(proof["claims"][0].get("value").is_none());
        }
    }
    let bytes = b"operator controlled complete raw volume".to_vec();
    let root = lattice_cashew::cid_for_bytes(0x55, &bytes);
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let (code, pins) = host
        .handle
        .request(
            "POST /pins",
            json!({"cid":root,"entries":{root.clone():encoded}}),
        )
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(pins["pins"], json!([root]));
    let archive = host.handle.volume(&root).await.unwrap().unwrap();
    let volume = ivy_core::Volume::decode(&archive, &root, 1).unwrap();
    assert_eq!(volume.entries[0].data, bytes);
    let peer = hex::encode(
        ed25519_dalek::SigningKey::from_bytes(&[59; 32])
            .verifying_key()
            .as_bytes(),
    );
    let (code, credit) = host
        .handle
        .request("/serving-credit", json!({"peer":peer,"bytes":777}))
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(credit["standing"], 777);
    host.handle.shutdown().await;
    host.task.await.unwrap().unwrap();
    let host = start(config()).await.unwrap();
    assert_eq!(
        host.handle.request("/pins", Value::Null).await.unwrap().1["pins"],
        json!([root])
    );
    assert_eq!(
        host.handle
            .request(&format!("/serving-credit/{peer}"), Value::Null)
            .await
            .unwrap()
            .1["standing"],
        777
    );
    let (code, pins) = host
        .handle
        .request(&format!("DELETE /pins/{root}"), Value::Null)
        .await
        .unwrap();
    assert_eq!(code, 200);
    assert_eq!(pins["pins"], json!([]));
    assert!(host.handle.volume(&root).await.unwrap().is_none());
    assert!(host.handle.volume(&cid).await.unwrap().is_some());
    host.handle.shutdown().await;
    host.task.await.unwrap().unwrap();
}
