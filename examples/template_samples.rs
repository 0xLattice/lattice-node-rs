use lattice_cashew::{Header, Volume};
use lattice_node::core::mining::{TemplateJob, TemplateRequest};
use lattice_node::runtime::template;
use lattice_primitives::*;
use lattice_validation::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn tx(key: &str, old: Option<&str>, value: &str) -> Transaction {
    Transaction::new(
        BTreeMap::new(),
        Header::new(TransactionBody {
            account_actions: vec![],
            actions: vec![Action {
                key: key.into(),
                old_value: old.map(str::to_owned),
                new_value: Some(value.into()),
            }],
            deposit_actions: vec![],
            receipt_actions: vec![],
            withdrawal_actions: vec![],
            signers: vec![],
            nonce: 0,
            chain_path: vec!["Nexus".into()],
        })
        .unwrap(),
    )
}
fn main() {
    let rows: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let original: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[0]["entries"].clone()).unwrap();
    let cid = rows[0]["cid"].as_str().unwrap();
    let previous: Block = lattice_cashew::cbor::decode(&original[cid]).unwrap();
    let mut output = Vec::new();
    for name in [
        "unresolved-conflict",
        "anchor",
        "anchor-future",
        "valid-reward",
        "empty",
        "timestamp-floor",
        "timestamp-forward",
        "conflict",
        "all-conflict",
        "count-limit",
        "byte-limit",
        "impossible-budget",
        "root-work",
        "descendant-work",
        "multi-work",
        "reward",
        "timestamp-overflow",
        "height-overflow",
    ] {
        let mut source = original.clone();
        let mut prev = previous.clone();
        let mut spec: ChainSpec =
            lattice_cashew::cbor::decode(&source[&prev.spec.0.raw_cid]).unwrap();
        let mut transactions = Vec::new();
        let mut plan = BTreeMap::new();
        let mut now = prev.timestamp + 1;
        let mut recipient = None;
        let mut anchor = None;
        match name {
            "anchor" | "anchor-future" => {
                anchor = Some(DifficultyAnchor {
                    block_height: if name == "anchor" { 0 } else { 100 },
                    timestamp: prev.timestamp,
                    target: UInt256([127; 32]),
                });
                now += 10000;
            }
            "valid-reward" => {
                recipient = Some(
                    crypto::create_address(
                        "ed01d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                    )
                    .unwrap(),
                )
            }
            "timestamp-floor" => now = 0,
            "timestamp-forward" => now += 10000,
            "timestamp-overflow" => prev.timestamp = i64::MAX,
            "height-overflow" => prev.height = u64::MAX,
            "conflict" | "unresolved-conflict" => {
                transactions = vec![
                    tx("a", None, "1"),
                    tx("a", Some("wrong"), "2"),
                    tx("b", None, "3"),
                ]
            }
            "all-conflict" => transactions = vec![tx("missing", Some("wrong"), "x")],
            "count-limit" => {
                spec.max_number_of_transactions_per_block = 1;
                transactions = vec![tx("a", None, "1"), tx("b", None, "2")];
            }
            "byte-limit" => {
                spec.max_block_size = 5000;
                transactions = vec![tx("a", None, "1"), tx("b", None, &"z".repeat(10000))];
            }
            "impossible-budget" => spec.max_block_size = 1,
            "root-work" => {
                let mut w = [0; 32];
                w[0] = 128;
                plan.insert(vec!["Nexus".into()], UInt256(w));
            }
            "descendant-work" => {
                plan.insert(
                    vec!["Nexus".into(), "child".into()],
                    UInt256([
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 3,
                    ]),
                );
            }
            "multi-work" => {
                for (path, w) in [
                    (vec!["Nexus".into()], 2),
                    (vec!["Nexus".into(), "x".into()], 3),
                ] {
                    let mut b = [0; 32];
                    b[31] = w;
                    plan.insert(path, UInt256(b));
                }
            }
            "reward" => recipient = Some("miner".to_owned()),
            _ => {}
        }
        prev.spec = Volume::new(spec).unwrap();
        source.insert(
            prev.spec.0.raw_cid.clone(),
            prev.spec.0.bytes(None).unwrap(),
        );
        let tip = content_id(&prev).unwrap();
        source.insert(tip.clone(), canonical_bytes(&prev).unwrap());
        for t in &transactions {
            source.insert(t.body.raw_cid.clone(), t.body.bytes(None).unwrap());
        }
        if name == "unresolved-conflict" {
            transactions = transactions
                .iter()
                .map(|t| lattice_cashew::cbor::decode(&canonical_bytes(t).unwrap()).unwrap())
                .collect();
        }
        let job = TemplateJob {
            id: 1,
            tip_cid: tip.clone(),
            tip_epoch: 0,
            pool_version: 0,
            transactions: transactions.clone(),
            request: TemplateRequest {
                reward_recipient: recipient.clone(),
                minimum_work: plan.clone(),
                ..Default::default()
            },
        };
        let expected = match template::build_root_template(&job, &source, now, anchor) {
            Ok(out) => {
                let content = resolve_block_content(&out.build.block, &source, i64::MAX).unwrap();
                json!({"built":true,"cid":content_id(&out.build.block).unwrap(),"workID":out.build.work_id,"searchTarget":out.build.search_target,"targets":out.build.targets,"digest":out.build.digest,"refused":out.refused,"selected":content.transactions.iter().map(|t|content_id(t).unwrap()).collect::<Vec<_>>()})
            }
            Err(_) => json!({"built":false}),
        };
        output.push(json!({"name":name,"anchor":anchor.map(|a|json!({"height":a.block_height,"timestamp":a.timestamp,"target":a.target})),"materializeBodies":name!="unresolved-conflict","entries":source,"tipCID":tip,"transactions":transactions.iter().map(|t|canonical_bytes(t).unwrap()).collect::<Vec<_>>(),"now":now,"recipient":recipient,"minWork":plan.iter().map(|(path,work)|json!({"path":path,"work":work})).collect::<Vec<_>>(),"expected":expected}));
    }
    let policy_rows: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/preflight-swift.json"
    ))
    .unwrap();
    for row in policy_rows.as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        if ![
            "policy_accept",
            "policy_reject_ready",
            "policy_reject_future",
            "missing_policy_ready",
            "stale_nonce_before_missing_policy",
            "bad_balance_before_reject_policy",
            "missing_state_before_reject_policy",
            "reject_policy_missing_parent",
            "ready",
        ]
        .contains(&name)
        {
            continue;
        }
        let source: BTreeMap<String, Vec<u8>> =
            serde_json::from_value(row["entries"].clone()).unwrap();
        let tip = row["tip"].as_str().unwrap().to_owned();
        let previous: Block = lattice_cashew::cbor::decode(&source[&tip]).unwrap();
        let bytes = source[row["transaction"].as_str().unwrap()].clone();
        let transaction: Transaction = lattice_cashew::cbor::decode(&bytes).unwrap();
        let now = previous.timestamp + 1;
        let job = TemplateJob {
            id: 1,
            tip_cid: tip.clone(),
            tip_epoch: 0,
            pool_version: 0,
            transactions: vec![transaction],
            request: TemplateRequest::default(),
        };
        let expected = match template::build_root_template(&job, &source, now, None) {
            Ok(out) => {
                let content = resolve_block_content(&out.build.block, &source, i64::MAX).unwrap();
                json!({"built":true,"cid":content_id(&out.build.block).unwrap(),"workID":out.build.work_id,"searchTarget":out.build.search_target,"targets":out.build.targets,"digest":out.build.digest,"refused":out.refused,"selected":content.transactions.iter().map(|t|content_id(t).unwrap()).collect::<Vec<_>>()})
            }
            Err(_) => json!({"built":false}),
        };
        output.push(json!({"name":format!("preflight-{name}"),"materializeBodies":false,"entries":source,"tipCID":tip,"transactions":[bytes],"now":now,"recipient":null,"minWork":[],"expected":expected}));
    }
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
