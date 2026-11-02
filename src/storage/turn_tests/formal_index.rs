//! Read-only observations for the finite pinned Recovery operator bridge.
use super::*;
use serde_json::{Value, json};

pub(super) fn observe(
    storage: &NodeStorage,
    tree: &ChainTree,
    path: &[String],
    universe: &[String],
) -> Value {
    let snapshot = storage.journal().recovery_snapshot(path).unwrap();
    let mut nodes = BTreeMap::new();
    for cid in universe {
        if let Some(block) = tree.consensus_block(cid) {
            let contributions: BTreeMap<_, _> = block
                .work_contributions
                .iter()
                .map(|(id, work)| (id.clone(), lattice_block_tree::target_hex(work.work())))
                .collect();
            nodes.insert(cid.clone(), json!({
                "timestamp":tree.header_snapshot(cid).unwrap().timestamp,
                "validated":tree.is_executed(cid), "excluded":tree.is_excluded_root(cid),
                "weight":block.work.to_hex(), "cumulative":tree.subtree_work(cid).unwrap().to_hex(),
                "contributions":contributions
            }));
        }
    }
    assert_eq!(
        nodes.len(),
        tree.block_count(),
        "export universe must cover every held block"
    );
    json!({"path":path,"batches":snapshot.batches,"nodes":nodes,"tip":tree.canonical_tip(),
        "revision":tree.current_revision(),"floor":snapshot.revision_floor})
}

pub(super) fn grouped(storage: &NodeStorage, path: &[String], cids: &[String]) -> Value {
    let snapshot = storage.journal().recovery_snapshot(path).unwrap();
    let headers: Vec<_> = cids
        .iter()
        .map(|cid| storage.journal().header(cid).unwrap().is_some())
        .collect();
    let proofs = storage.journal().saved_proofs(path, cids).unwrap();
    json!({"batches":snapshot.batches.len(),"headers":headers,"proofs":proofs.len(),
        "cursor":snapshot.cursors.get("proof-peer").map_or(0,|v|v.position)})
}
