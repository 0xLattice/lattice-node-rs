use lattice_node::core::mining::{TemplateJob, TemplateRequest};
use lattice_node::runtime::template;
use lattice_primitives::*;
use lattice_validation::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
#[test]
fn actual_swift_root_template_corpus() {
    let rows: Vec<Value> = serde_json::from_str(include_str!("template-swift.json")).unwrap();
    assert_eq!(rows.len(), 27);
    for row in rows {
        let name = row["name"].as_str().unwrap();
        let source: BTreeMap<String, Vec<u8>> =
            serde_json::from_value(row["entries"].clone()).unwrap();
        let transactions = row["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                let bytes: Vec<u8> = serde_json::from_value(v.clone()).unwrap();
                {
                    let mut tx: Transaction = lattice_cashew::cbor::decode(&bytes).unwrap();
                    if row["materializeBodies"] == true {
                        tx.body = tx.body.resolve(&source, None).unwrap();
                    }
                    tx
                }
            })
            .collect();
        let minimum_work = row["minWork"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                (
                    serde_json::from_value(v["path"].clone()).unwrap(),
                    serde_json::from_value(v["work"].clone()).unwrap(),
                )
            })
            .collect();
        let job = TemplateJob {
            id: 1,
            tip_cid: row["tipCID"].as_str().unwrap().into(),
            tip_epoch: 0,
            pool_version: 0,
            transactions,
            request: TemplateRequest {
                reward_recipient: serde_json::from_value(row["recipient"].clone()).unwrap(),
                minimum_work,
                ..Default::default()
            },
        };
        let actual = match template::build_root_template(
            &job,
            &source,
            row["now"].as_i64().unwrap(),
            row.get("anchor")
                .filter(|a| !a.is_null())
                .map(|a| DifficultyAnchor {
                    block_height: a["height"].as_u64().unwrap(),
                    timestamp: a["timestamp"].as_i64().unwrap(),
                    target: serde_json::from_value(a["target"].clone()).unwrap(),
                }),
        ) {
            Ok(out) => {
                let content = resolve_block_content(&out.build.block, &source, i64::MAX).unwrap();
                json!({"built":true,"cid":content_id(&out.build.block).unwrap(),"workID":out.build.work_id,"searchTarget":out.build.search_target,"targets":out.build.targets,"digest":out.build.digest,"refused":out.refused,"selected":content.transactions.iter().map(|t|content_id(t).unwrap()).collect::<Vec<_>>()})
            }
            Err(_) => json!({"built": false}),
        };
        assert_eq!(actual, row["swift"], "{name}");
        if [
            "anchor",
            "valid-reward",
            "empty",
            "timestamp-floor",
            "timestamp-forward",
            "conflict",
            "all-conflict",
            "count-limit",
            "byte-limit",
            "root-work",
            "descendant-work",
            "multi-work",
            "preflight-policy_accept",
            "preflight-policy_reject_ready",
            "preflight-missing_policy_ready",
            "preflight-ready",
        ]
        .contains(&name)
        {
            assert_eq!(actual["built"], true, "positive {name}");
        }
        if name == "unresolved-conflict" {
            assert!(actual["selected"].as_array().unwrap().is_empty());
        }
        if ["preflight-ready", "preflight-policy_accept", "byte-limit"].contains(&name) {
            assert_eq!(
                actual["selected"].as_array().unwrap().len(),
                1,
                "selected {name}"
            );
        }
        if name == "conflict" {
            assert_eq!(actual["selected"].as_array().unwrap().len(), 2);
        }
        if name == "count-limit" {
            assert_eq!(actual["selected"].as_array().unwrap().len(), 1);
        }
        if name == "preflight-policy_reject_ready" {
            assert_eq!(actual["refused"].as_array().unwrap().len(), 1);
        }
    }
}
