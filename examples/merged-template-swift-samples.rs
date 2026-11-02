#[path = "../tests/merged-template-swift-support/mod.rs"]
mod support;
use lattice_cashew::{Header, Volume};
use lattice_primitives::*;
use lattice_validation::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn main() {
    let rows: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let original: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[0]["entries"].clone()).unwrap();
    let previous: Block =
        lattice_cashew::cbor::decode(&original[rows[0]["cid"].as_str().unwrap()]).unwrap();
    let mut output = Vec::new();
    for name in [
        "leaves",
        "child-work",
        "root-work",
        "unresolved-descendant",
        "absent-direct-filter",
        "nonleaf",
        "hosted-empty",
        "hosted-supplied",
        "malformed-hosted-value",
        "malformed-hosted-empty",
        "count-limit",
        "numeric-order-12",
        "oversize-transaction",
        "impossible-budget",
        "rotate0",
        "rotate1",
        "rotate2",
        "fit0",
        "fit1",
        "fit2",
        "fit3",
        "fit4",
        "fit5",
        "fit6",
    ] {
        let mut source = original.clone();
        let mut prev = previous.clone();
        let mut spec: ChainSpec =
            lattice_cashew::cbor::decode(&source[&prev.spec.0.raw_cid]).unwrap();
        if name.starts_with("rotate") {
            prev.height = name.chars().last().unwrap().to_digit(10).unwrap() as u64;
        }
        spec.max_number_of_transactions_per_block = if name == "count-limit" { 1 } else { 16 };
        spec.max_block_size = match name {
            "impossible-budget" => 1,
            "rotate0" | "rotate1" | "rotate2" => 900,
            "fit0" => 700,
            "fit1" => 800,
            "fit2" => 850,
            "fit3" => 900,
            "fit4" => 950,
            "fit5" => 1000,
            "fit6" => 1500,
            "oversize-transaction" => 5000,
            _ => 1000000,
        };
        prev.spec = Volume::new(spec.clone()).unwrap();
        support::gather(&prev.spec, &mut source);
        let empty = BTreeMap::new();
        let mut children = BTreeMap::new();
        for (n, d) in ["a", "b", "c"].iter().enumerate() {
            let target = UInt256([40 + (n as u8) * 70; 32]);
            let child = GenesisTemplate {
                spec: &spec,
                transactions: &[],
                children: &empty,
                parent_state: Some(&prev.post_state),
                timestamp: prev.timestamp + 1,
                target,
                nonce: 0,
                version: prev.version,
            }
            .build(&source)
            .unwrap()
            .block;
            children.insert(d.to_string(), child);
        }
        if name == "nonleaf" {
            let nested = BlockHeader::new(children.get("c").unwrap().clone()).unwrap();
            let child = children.get_mut("a").unwrap();
            child.children = Header::new(BTreeMap::from([("nested".into(), nested)])).unwrap();
        }
        let mut transactions = vec![
            support::transaction("one", "1"),
            support::transaction("two", "2"),
        ];
        if name == "numeric-order-12" {
            transactions = (0..12)
                .map(|i| support::transaction(&format!("key-{i}"), &i.to_string()))
                .collect();
        }
        let mut hosted = vec![];
        if name.starts_with("hosted") || name.starts_with("malformed-hosted") {
            hosted.push("a");
            let value = if name.starts_with("malformed-hosted") {
                "invalid".into()
            } else {
                format!(
                    "{}@{}",
                    content_id(&children["a"]).unwrap(),
                    prev.post_state.0.raw_cid
                )
            };
            transactions.insert(0, support::transaction("child/a", &value));
            if name.ends_with("hosted-empty") {
                children.clear();
            }
        }
        if name == "oversize-transaction" {
            transactions.push(support::transaction("huge", &"z".repeat(10000)));
        }
        let mut plan = Vec::new();
        for (condition, path, work) in [
            (name == "child-work", vec!["Nexus", "a"], 8u8),
            (name == "root-work", vec!["Nexus"], 8),
            (
                name == "unresolved-descendant",
                vec!["Nexus", "b", "missing"],
                16,
            ),
            (name == "absent-direct-filter", vec!["Nexus", "absent"], 32),
        ] {
            if condition {
                let mut w = [0; 32];
                w[31] = work;
                plan.push(json!({"path":path,"work":UInt256(w)}));
            }
        }
        for child in children.values() {
            support::gather(&BlockHeader::new(child.clone()).unwrap(), &mut source);
        }
        for tx in &transactions {
            support::gather(&tx.body, &mut source);
        }
        let tip = content_id(&prev).unwrap();
        source.insert(tip.clone(), canonical_bytes(&prev).unwrap());
        let mut row = json!({"name":name,"entries":source,"tipCID":tip,"children":children.iter().map(|(d,c)|(d.clone(),content_id(c).unwrap())).collect::<BTreeMap<_,_>>(),"transactions":transactions.iter().map(|t|canonical_bytes(t).unwrap()).collect::<Vec<_>>(),"hosted":hosted,"minWork":plan});
        row["expected"] = support::observe(&row);
        output.push(row);
    }
    println!("{}", serde_json::to_string(&output).unwrap());
}
