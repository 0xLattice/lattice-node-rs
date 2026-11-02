use lattice_cashew::Header;
use lattice_node::runtime::merged_template::*;
use lattice_primitives::*;
use lattice_validation::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn observe(row: &Value) -> Value {
    let source: BTreeMap<String, Vec<u8>> = serde_json::from_value(row["entries"].clone()).unwrap();
    let previous: Block =
        lattice_cashew::cbor::decode(&source[row["tipCID"].as_str().unwrap()]).unwrap();
    let spec: ChainSpec = lattice_cashew::cbor::decode(&source[&previous.spec.0.raw_cid]).unwrap();
    let children: BTreeMap<String, Block> = row["children"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(d, c)| {
            (
                d.clone(),
                lattice_cashew::cbor::decode(&source[c.as_str().unwrap()]).unwrap(),
            )
        })
        .collect();
    let transactions: Vec<Transaction> = row["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let bytes: Vec<u8> = serde_json::from_value(v.clone()).unwrap();
            let mut tx: Transaction = lattice_cashew::cbor::decode(&bytes).unwrap();
            tx.body = tx.body.resolve(&source, None).unwrap();
            tx
        })
        .collect();
    let hosted: BTreeSet<String> = serde_json::from_value(row["hosted"].clone()).unwrap();
    let plan: BTreeMap<Vec<String>, UInt256> = row["minWork"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                serde_json::from_value(p["path"].clone()).unwrap(),
                serde_json::from_value(p["work"].clone()).unwrap(),
            )
        })
        .collect();
    let path = vec!["Nexus".into()];
    let result = (|| -> Result<Value, Box<dyn std::error::Error>> {
        let block = fit_level(
            &LevelTemplateInput {
                path: &path,
                previous: &previous,
                transactions: &transactions,
                children: &children,
                hosted: &hosted,
                parent_carrier: None,
                timestamp: previous.timestamp + 1,
                recipient: None,
                anchor: Some(DifficultyAnchor {
                    block_height: previous.height,
                    timestamp: previous.timestamp,
                    target: previous.target,
                }),
                spec: &spec,
            },
            &source,
        )?;
        let targets = local_search_targets(&block, &path, &children, &plan)?;
        let content = resolve_block_content(&block, &source, i64::MAX)?;
        let mut selected: Vec<_> = content
            .block
            .transactions
            .node
            .as_deref()
            .unwrap()
            .entries()?
            .into_iter()
            .map(|(index, tx)| (index.parse::<u64>().unwrap(), tx.0.raw_cid))
            .collect();
        selected.sort_by_key(|(index, _)| *index);
        let selected: Vec<_> = selected.into_iter().map(|(_, cid)| cid).collect();
        Ok(
            json!({"built":true,"cid":content_id(&block)?,"targets":targets,"selected":selected,"children":block.children.node.as_deref().unwrap().keys().collect::<Vec<_>>()}),
        )
    })();
    result.unwrap_or_else(|_| json!({"built":false}))
}
pub fn gather(header: &dyn lattice_cashew::DagHeader, source: &mut BTreeMap<String, Vec<u8>>) {
    if let Ok(bytes) = header.serialized(&()) {
        source.insert(header.cid().into(), bytes);
    }
    if let Ok(links) = header.links() {
        for link in links {
            gather(link, source);
        }
    }
}
pub fn transaction(key: &str, value: &str) -> Transaction {
    Transaction::new(
        BTreeMap::new(),
        Header::new(TransactionBody {
            account_actions: vec![],
            actions: vec![Action {
                key: key.into(),
                old_value: None,
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
