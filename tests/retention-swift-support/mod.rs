use lattice_cashew::{DagHeader, Volume};
use lattice_node::storage::plan_tip_state;
use lattice_primitives::LatticeState;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn observe(row: &Value) -> Value {
    let source: BTreeMap<String, Vec<u8>> = serde_json::from_value(row["entries"].clone()).unwrap();
    let counts: BTreeMap<String, i64> = serde_json::from_value(row["counts"].clone()).unwrap();
    let new = Volume::<LatticeState>::from_cid(row["new"].as_str().unwrap()).unwrap();
    let old = row["old"]
        .as_str()
        .map(|cid| Volume::<LatticeState>::from_cid(cid).unwrap());
    match plan_tip_state(&new, old.as_ref(), &source, |cid| {
        Ok(*counts.get(cid).unwrap_or(&0))
    }) {
        Ok(plan) => json!({"changes":plan.changes,"entering":plan.entering}),
        Err(_) => json!({"error":true}),
    }
}
pub fn graph(root: &dyn DagHeader) -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, i64>) {
    fn walk(
        h: &dyn DagHeader,
        seen: &mut BTreeSet<String>,
        bytes: &mut BTreeMap<String, Vec<u8>>,
        counts: &mut BTreeMap<String, i64>,
    ) {
        *counts.entry(h.cid().into()).or_default() += 1;
        if !seen.insert(h.cid().into()) {
            return;
        }
        bytes.insert(h.cid().into(), h.serialized(&()).unwrap());
        for child in h.links().unwrap() {
            walk(child, seen, bytes, counts);
        }
    }
    let (mut bytes, mut counts) = (BTreeMap::new(), BTreeMap::new());
    walk(root, &mut BTreeSet::new(), &mut bytes, &mut counts);
    (bytes, counts)
}
