//! Export observations from production SQLite staging and ChainTree replay.
use lattice_block_tree::{BlockImportBatch, ChainTree};
use lattice_node::storage::{NodeFactBatch, NodeStore};
use lattice_primitives::ChainRuntimeContext;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn cid(s: &str) -> String {
    lattice_cashew::cid_for_bytes(0x71, s.as_bytes())
}
fn model(k: &str, b: &str, g: &str, q: u64, v: bool, m: &str) -> Value {
    json!({"k":k,"b":b,"g":g,"q":q,"v":v,"m":m})
}
fn batch(x: &Value) -> BlockImportBatch {
    let b = x["b"].as_str().unwrap();
    let k = x["k"].as_str().unwrap();
    let mut facts = vec![];
    if k == "block" {
        facts.push(json!({"block":{"_0":{"blockHash":cid(b),"parentBlockHash":if b=="r"{None}else{Some(cid("r"))},"blockHeight":if b=="r"{0}else{1},"postStateCID":cid(&format!("post/{b}")),"prevStateCID":cid("pre"),"parentStateCID":cid("parent"),"specCID":cid("spec"),"target":"ff","nextTarget":"ff","timestamp":if x["m"]=="m1"{0}else{1},"stateDiff":{"replaced":{},"created":{}}}}}));
    }
    if k == "block" || k == "work" {
        facts.push(json!({"work":{"_0":{"blockHash":cid(b),"contribution":{"id":cid(x["g"].as_str().unwrap()),"work":format!("0x{:x}",x["q"].as_u64().unwrap())}}}}));
    }
    if x["v"] == true || k == "valid" {
        facts.push(json!({"validation":{"_0":{"blockHash":cid(b)}}}));
    }
    if k == "excl" {
        facts.push(json!({"exclusion":{"_0":{"blockHash":cid(b)}}}));
    }
    serde_json::from_value(json!({"facts":facts})).unwrap()
}
fn level(batch: BlockImportBatch) -> NodeFactBatch {
    NodeFactBatch {
        proofs: vec![],
        path: vec!["Nexus".into()],
        facts: vec![batch],
        headers: vec![],
        volume_roots: vec![],
        cursors: BTreeMap::new(),
    }
}
fn context() -> ChainRuntimeContext {
    ChainRuntimeContext::new(vec!["Nexus".into()], Some(&cid("r"))).unwrap()
}
fn observe(tree: &ChainTree, log: &[Value], phase: &str, seen: u64) -> Value {
    let labels = ["r", "a", "b"];
    let grinds = ["g_r", "g_a", "g_b", "g_x"];
    let mut held = vec![];
    let mut meta = BTreeMap::new();
    let mut loc = BTreeMap::new();
    let mut qty = BTreeMap::new();
    let mut val = vec![];
    let mut exc = vec![];
    let mut weights = BTreeMap::new();
    let mut cumulative = BTreeMap::new();
    for g in grinds {
        loc.insert(g, "none".to_string());
        qty.insert(g, 0);
    }
    for b in labels {
        meta.insert(b, "none");
        if let Some(block) = tree.consensus_block(&cid(b)) {
            held.push(b);
            meta.insert(
                b,
                if tree.header_snapshot(&cid(b)).unwrap().timestamp == 0 {
                    "m1"
                } else {
                    "m2"
                },
            );
            weights.insert(b, u64::from_str_radix(&block.work.to_hex(), 16).unwrap());
            cumulative.insert(
                b,
                u64::from_str_radix(&tree.subtree_work(&cid(b)).unwrap().to_hex(), 16).unwrap(),
            );
            for g in grinds {
                if let Some(w) = block.work_contributions.get(&cid(g)) {
                    loc.insert(g, b.to_string());
                    qty.insert(g, u64::from_be_bytes(w.work()[24..].try_into().unwrap()));
                }
            }
            if tree.is_executed(&cid(b)) {
                val.push(b)
            }
            if tree.is_excluded_root(&cid(b)) {
                exc.push(b)
            }
        }
    }
    let tip = labels
        .into_iter()
        .find(|b| tree.canonical_tip() == cid(b))
        .unwrap_or("none");
    json!({"phase":phase,"log":log,"graph":{"held":held,"meta":meta,"loc":loc,"qty":qty,"val":val,"exc":exc},"tip":tip,"weights":weights,"cumulative":cumulative,"revision":tree.current_revision(),"seen":seen})
}
fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut rows = vec![];
    let genesis = model("block", "r", "g_r", 1, true, "m1");
    let a = model("block", "a", "g_a", 1, false, "m1");
    let b = model("block", "b", "g_b", 2, false, "m1");
    let w1 = model("work", "a", "g_x", 1, false, "none");
    let w2 = model("work", "a", "g_x", 2, false, "none");
    let av = model("block", "a", "g_x", 2, true, "m1");
    let vb = model("valid", "b", "none", 0, false, "none");
    let xa = model("excl", "a", "none", 0, false, "none");
    let conflict = model("block", "a", "g_a", 1, false, "m2");
    for (scenario, sequence) in [
        vec![
            genesis.clone(),
            a.clone(),
            b.clone(),
            w1.clone(),
            w2.clone(),
            av.clone(),
            vb.clone(),
        ],
        vec![
            genesis.clone(),
            b.clone(),
            a.clone(),
            w1.clone(),
            xa.clone(),
            vb.clone(),
        ],
    ]
    .into_iter()
    .enumerate()
    {
        let file = dir.path().join(format!("s{scenario}.sqlite"));
        let mut store = NodeStore::open(&file, cid("r")).unwrap();
        let mut tree = ChainTree::empty(context());
        let mut log = vec![];
        let mut seen = 0;
        for (index, x) in sequence.into_iter().enumerate() {
            let prepared = batch(&x);
            store
                .stage_node_facts(&[level(prepared.clone())], "formal")
                .unwrap();
            // The pending durable suffix is not yet visible: the observed graph must
            // equal the previous durable prefix (Recovery.VisibleLog).
            if !log.is_empty() {
                rows.push(observe(&tree, &log, "durable-before-apply", seen));
            }
            log.push(x);
            assert_eq!(
                store.import_batches(&["Nexus".into()]).unwrap(),
                log.iter().map(batch).collect::<Vec<_>>()
            );
            tree.replay(&prepared).unwrap();
            seen = seen.max(tree.current_revision());
            rows.push(observe(&tree, &log, "applied", seen));
            if index == 2 {
                assert!(
                    store
                        .stage_node_facts(&[level(batch(&conflict))], "formal")
                        .is_err()
                );
                assert_eq!(
                    store.import_batches(&["Nexus".into()]).unwrap(),
                    log.iter().map(batch).collect::<Vec<_>>()
                );
                rows.push(observe(&tree, &log, "conflict-refused", seen));
            }
            // Throw away all volatile state and reopen the actual durable journal.
            drop(store);
            store = NodeStore::open(&file, cid("r")).unwrap();
            let (recovered, _) = store.restore_chain(context(), None, vec![]).unwrap();
            rows.push(observe(&recovered, &log, "restored", seen));
            // Also replay every reverse-rotated journal ordering, exercising the same
            // production reducer independently of SQLite enumeration order.
            let snapshot = store.recovery_snapshot(&["Nexus".into()]).unwrap();
            for shift in 0..snapshot.batches.len() {
                let mut perm = snapshot.batches.clone();
                perm.rotate_left(shift);
                perm.reverse();
                let restored =
                    ChainTree::restore(&perm, snapshot.revision_floor, context()).unwrap();
                rows.push(observe(&restored, &log, "permuted", seen));
            }
            // Repeated replay must change neither graph nor revision.
            let before = tree.current_revision();
            for batch in &snapshot.batches {
                tree.replay(batch).unwrap();
            }
            assert_eq!(before, tree.current_revision());
            rows.push(observe(&tree, &log, "duplicate-replay", seen));
        }
    }
    let mut ranked = ["r", "a", "b"];
    ranked.sort_by_key(|b| cid(b).parse::<cid::Cid>().unwrap().to_bytes());
    let rank: BTreeMap<_, _> = ranked
        .into_iter()
        .enumerate()
        .map(|(i, b)| (b, i + 1))
        .collect();
    // Keep a real production counterexample: sorted replay without a floor
    // collapses two live work raises into one block restatement.
    let probe = [genesis.clone(), a.clone(), w1, w2, av];
    let batches: Vec<_> = probe.iter().map(batch).collect();
    let mut live = ChainTree::empty(context());
    for b in &batches {
        live.replay(b).unwrap();
    }
    let bad = ChainTree::restore(&batches, 0, context()).unwrap();
    let negative = observe(&bad, &probe, "floor-omitted", live.current_revision());
    println!(
        "{}",
        json!({"revision":"a9f4c2879b6565ae11c17d35efe14475c5830978","cidRank":rank,"rows":rows,"negativeFloor":negative})
    );
}
