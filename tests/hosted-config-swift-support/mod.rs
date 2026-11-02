use lattice_cashew::Header;
use lattice_node::runtime::hosted_config::{check_premine, premine_transactions};
use lattice_primitives::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
pub const KEY: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
pub const PUBLIC: &str = "ed01d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
pub fn signed(body: TransactionBody) -> Transaction {
    let signature = crypto::sign(&body.signing_preimage().unwrap(), KEY).unwrap();
    Transaction::new(
        BTreeMap::from([(PUBLIC.into(), signature)]),
        Header::new(body).unwrap(),
    )
}
pub fn observe(row: &Value) -> Value {
    let spec: ChainSpec = serde_json::from_value(row["spec"].clone()).unwrap();
    let path: Vec<String> = serde_json::from_value(row["path"].clone()).unwrap();
    let source: BTreeMap<String, Vec<u8>> = serde_json::from_value(row["entries"].clone()).unwrap();
    let txs: Vec<Transaction> = row["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let bytes: Vec<u8> = serde_json::from_value(v.clone()).unwrap();
            let mut tx: Transaction = lattice_cashew::cbor::decode(&bytes).unwrap();
            if row["materialized"] == true {
                tx.body = tx.body.resolve(&source, None).unwrap();
            }
            tx
        })
        .collect();
    let modules: Vec<Vec<u8>> = serde_json::from_value(row["modules"].clone()).unwrap();
    let recipient = row["recipient"].as_str();
    match check_premine(&spec, recipient, &txs, &path, &modules, 1) {
        Ok(()) => {
            json!({"accepted":true,"premine":premine_transactions(&spec,recipient,&path).unwrap().iter().map(content_id).collect::<Result<Vec<_>,_>>().unwrap()})
        }
        Err(_) => json!({"accepted":false}),
    }
}
pub fn module(result: u8) -> Vec<u8> {
    let mut out =
        hex::decode("0061736d01000000010c0260017f017f60027f7f017f03030200010503010001").unwrap();
    let mut exports = vec![3];
    for (name, kind, index) in [("memory", 2, 0), ("lattice_alloc", 0, 0), ("policy", 0, 1)] {
        exports.push(name.len() as u8);
        exports.extend_from_slice(name.as_bytes());
        exports.extend([kind, index]);
    }
    out.extend([7, exports.len() as u8]);
    out.extend(exports);
    out.extend([10, 11, 2, 4, 0, 65, 0, 11, 4, 0, 65, result, 11]);
    out
}
