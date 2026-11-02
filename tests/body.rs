use lattice_block_tree::{BlockImportBatch, ChainTree};
use lattice_node::core::body::*;
use lattice_primitives::ChainRuntimeContext;
fn cid(s: &str) -> String {
    lattice_cashew::cid_for_bytes(0x71, s.as_bytes())
}
fn batch(name: &str, parent: Option<&str>, height: u64, qty: u64) -> BlockImportBatch {
    serde_json::from_value(serde_json::json!({"facts":[{"block":{"_0":{"blockHash":cid(name),"parentBlockHash":parent.map(cid),"blockHeight":height,"postStateCID":cid("post"),"prevStateCID":cid("prev"),"parentStateCID":cid("parent"),"specCID":cid("spec"),"target":"ff","nextTarget":"ff","timestamp":height,"stateDiff":{"replaced":{},"created":{}}}}},{"work":{"_0":{"blockHash":cid(name),"contribution":{"id":cid(&format!("grind/{name}")),"work":format!("0x{qty:x}")}}}}]})).unwrap()
}
fn tree() -> ChainTree {
    ChainTree::restore(
        &[
            batch("r", None, 0, 1),
            batch("a", Some("r"), 1, 2),
            batch("b", Some("r"), 1, 1),
            batch("c", Some("a"), 2, 3),
        ],
        0,
        ChainRuntimeContext::new(vec!["Nexus".into()], Some(&cid("r"))).unwrap(),
    )
    .unwrap()
}
#[test]
fn unexecuted_root_window_connects_in_parent_order() {
    let mut tree = tree();
    assert_eq!(best_window(&tree, 2), vec![cid("r"), cid("a")]);
    assert_eq!(next_to_connect(&tree, 4), vec![cid("r")]);
    let mut pipeline = BodyPipeline::default();
    let effects = pipeline.schedule(&tree, 4, 0);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e,BodyEffect::Fetch(c) if c==&cid("r")))
    );
    pipeline.body_fetched(&cid("c"));
    assert!(
        !pipeline
            .schedule(&tree, 4, 1)
            .iter()
            .any(|e| matches!(e, BodyEffect::Connect(_)))
    );
    pipeline.body_fetched(&cid("r"));
    let jobs = pipeline.schedule(&tree, 4, 2);
    assert!(
        jobs.iter()
            .any(|e| matches!(e,BodyEffect::Connect(job) if job.block_hash()==cid("r")))
    );
    pipeline.completed(&cid("r"));
    tree.replay(&BlockImportBatch::validation(cid("r")))
        .unwrap();
    let windows = body_window(&tree, 1);
    assert!(windows.contains(&cid("a")) && windows.contains(&cid("b")));
    assert_eq!(connect_distances(&tree, 4)[&cid("c")], 1);
}
#[test]
fn availability_retry_is_not_an_invalidity_verdict() {
    let tree = tree();
    let mut pipeline = BodyPipeline::default();
    pipeline.schedule(&tree, 4, 0);
    pipeline.body_fetched(&cid("r"));
    pipeline.schedule(&tree, 4, 1);
    pipeline.park_unavailable(&cid("r"), 2, 100, 500);
    assert!(
        !pipeline
            .schedule(&tree, 4, 101)
            .iter()
            .any(|e| matches!(e,BodyEffect::Fetch(c) if c==&cid("r")))
    );
    assert!(
        pipeline
            .schedule(&tree, 4, 102)
            .iter()
            .any(|e| matches!(e,BodyEffect::Fetch(c) if c==&cid("r")))
    );
    assert!(!tree.is_excluded_root(&cid("r")));
    assert_eq!(pipeline.next_retry(2), Some(102));
}
