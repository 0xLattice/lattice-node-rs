use lattice_cashew::Header;
use lattice_node::core::mining::{TemplateJob, TemplateRequest};
use lattice_node::runtime::{merged_template::*, template};
use lattice_primitives::*;
use lattice_validation::*;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn level_fit_preserves_root_and_carries_bound_child() {
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let source: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[9]["entries"].clone()).unwrap();
    let cid = rows[9]["cid"].as_str().unwrap().to_owned();
    let previous: Block = lattice_cashew::cbor::decode(&source[&cid]).unwrap();
    let resolved = previous.spec.0.resolve(&source, None).unwrap();
    let spec = resolved.node.as_deref().unwrap();
    let path = vec!["Nexus".to_owned()];
    let empty = BTreeMap::new();
    let hosted = BTreeSet::from(["child".to_owned()]);
    let mut input = LevelTemplateInput {
        path: &path,
        previous: &previous,
        transactions: &[],
        children: &empty,
        hosted: &hosted,
        parent_carrier: None,
        timestamp: previous.timestamp + 1,
        recipient: None,
        anchor: None,
        spec,
    };
    let root = fit_level(&input, &source).unwrap();
    let job = TemplateJob {
        id: 1,
        tip_cid: cid,
        tip_epoch: 0,
        pool_version: 0,
        transactions: vec![],
        request: TemplateRequest::default(),
    };
    let expected = template::build_root_template(&job, &source, input.timestamp, None).unwrap();
    assert_eq!(
        content_id(&root).unwrap(),
        content_id(&expected.build.block).unwrap()
    );
    let child_path = vec!["Nexus".to_owned(), "child".to_owned()];
    let context = ChildTemplateContext {
        tip_cid: None,
        best_header_tip: String::new(),
        previous: None,
        anchor: None,
        transactions: vec![],
        genesis_spec: Some(spec.clone()),
        genesis_premine_recipient: None,
        genesis_transactions: vec![],
        genesis_target: previous.target,
        below_act_on_tip: false,
    };
    let contexts = BTreeMap::from([(child_path.clone(), context.clone())]);
    let built = build_merged_template(&job, &source, input.timestamp, None, &contexts).unwrap();
    assert_eq!(
        built.build.chains.len(),
        2,
        "configured valid child genesis is carried"
    );
    let mut changed_contexts = contexts.clone();
    changed_contexts
        .get_mut(&child_path)
        .unwrap()
        .best_header_tip = "a-different-header-tip".into();
    let changed =
        build_merged_template(&job, &source, input.timestamp, None, &changed_contexts).unwrap();
    assert_eq!(
        content_id(&changed.build.block).unwrap(),
        content_id(&built.build.block).unwrap()
    );
    assert_ne!(
        changed.build.digest, built.build.digest,
        "unexecuted best-header changes invalidate status digest"
    );
    let genesis = built.build.block.children.node.as_deref().unwrap()["child"]
        .0
        .node
        .as_deref()
        .unwrap();
    assert_eq!(
        genesis.parent_state.0.raw_cid,
        built.build.block.prev_state.0.raw_cid
    );
    let mut extending = context.clone();
    extending.tip_cid = Some(content_id(genesis).unwrap());
    extending.best_header_tip = extending.tip_cid.clone().unwrap();
    extending.previous = Some(lattice_cashew::Volume::new(genesis.clone()).unwrap());
    extending.transactions = vec![Transaction::new(
        BTreeMap::new(),
        Header::new(TransactionBody {
            account_actions: vec![],
            actions: vec![Action {
                key: "child-data".into(),
                old_value: None,
                new_value: Some("value".into()),
            }],
            deposit_actions: vec![],
            receipt_actions: vec![],
            withdrawal_actions: vec![],
            signers: vec![],
            nonce: 0,
            chain_path: child_path.clone(),
        })
        .unwrap(),
    )];
    for below in [false, true] {
        extending.below_act_on_tip = below;
        let contexts = BTreeMap::from([(child_path.clone(), extending.clone())]);
        let next = build_merged_template(&job, &source, input.timestamp, None, &contexts).unwrap();
        let next_child = next.build.block.children.node.as_deref().unwrap()["child"]
            .0
            .node
            .as_deref()
            .unwrap();
        assert_eq!(next_child.height, 1);
        assert_eq!(
            next_child.parent_state.0.raw_cid,
            next.build.block.prev_state.0.raw_cid
        );
        let content = resolve_block_content(next_child, &source, i64::MAX).unwrap();
        assert_eq!(
            content.transactions.len(),
            usize::from(!below),
            "ancestor fallback never carries tip pool"
        );
    }
    let mut waiting = context;
    waiting.genesis_spec = None;
    let waiting = BTreeMap::from([(child_path, waiting)]);
    let built = build_merged_template(&job, &source, input.timestamp, None, &waiting).unwrap();
    assert_eq!(
        built.build.chains.len(),
        1,
        "unexecuted child waits instead of creating genesis"
    );
    let child = GenesisTemplate {
        spec,
        transactions: &[],
        children: &empty,
        parent_state: Some(&previous.post_state),
        timestamp: input.timestamp,
        target: previous.target,
        nonce: 0,
        version: previous.version,
    }
    .build(&source)
    .unwrap()
    .block;
    let children = BTreeMap::from([("child".to_owned(), child.clone())]);
    input.children = &children;
    let merged = fit_level(&input, &source).unwrap();
    let carried = merged.children.node.as_deref().unwrap()["child"]
        .0
        .node
        .as_deref()
        .unwrap();
    assert_eq!(carried.parent_state.0.raw_cid, merged.prev_state.0.raw_cid);
    assert_eq!(content_id(carried).unwrap(), content_id(&child).unwrap());
    let committed = content_id(&merged).unwrap();
    let unfiltered = local_search_targets(&merged, &path, &children, &BTreeMap::new()).unwrap();
    assert_eq!(unfiltered[0], merged.target.max(child.target));
    let mut hardest = [0; 32];
    hardest[0] = 0x80;
    let policy = BTreeMap::from([(
        vec!["Nexus".into(), "child".into(), "unresolved".into()],
        UInt256(hardest),
    )]);
    let capped = local_search_targets(&merged, &path, &children, &policy).unwrap();
    let mut one = [0; 32];
    one[31] = 1;
    assert_eq!(capped, vec![UInt256(one)]);
    assert_eq!(content_id(&merged).unwrap(), committed);
    let bad = BTreeMap::from([(path.clone(), UInt256([0; 32]))]);
    assert!(local_search_targets(&merged, &path, &children, &bad).is_err());
    assert_eq!(merged.target, root.target);
    assert_eq!(merged.post_state.0.raw_cid, root.post_state.0.raw_cid);

    // A hosted slot cannot be stolen by a pool commitment, even when the
    // host supplies no candidate this round. An unrelated action still fits.
    let tx = |key: &str| {
        Transaction::new(
            BTreeMap::new(),
            Header::new(TransactionBody {
                account_actions: vec![],
                actions: vec![Action {
                    key: key.into(),
                    old_value: None,
                    new_value: Some(format!(
                        "{}@{}",
                        content_id(&child).unwrap(),
                        previous.post_state.0.raw_cid
                    )),
                }],
                deposit_actions: vec![],
                receipt_actions: vec![],
                withdrawal_actions: vec![],
                signers: vec![],
                nonce: 0,
                chain_path: path.clone(),
            })
            .unwrap(),
        )
    };
    let transactions = vec![tx("child/child"), tx("unrelated")];
    input.children = &empty;
    input.transactions = &transactions;
    let fitted = fit_level(&input, &source).unwrap();
    let content = resolve_block_content(&fitted, &source, i64::MAX).unwrap();
    assert_eq!(content.transactions.len(), 1);
    assert_eq!(
        content_id(&content.transactions[0]).unwrap(),
        content_id(&transactions[1]).unwrap()
    );
    assert!(fitted.children.node.as_deref().unwrap().is_empty());
}
