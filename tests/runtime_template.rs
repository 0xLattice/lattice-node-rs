use core::mining::{TemplateJob, TemplateRequest};
use lattice_cashew::{Header, Volume};
pub use lattice_node::core;
use lattice_node::runtime::template;
use lattice_primitives::*;
use lattice_validation::*;
use std::collections::BTreeMap;
fn fixture() -> (TemplateJob, BTreeMap<String, Vec<u8>>, Block) {
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let entries: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[0]["entries"].clone()).unwrap();
    let cid = rows[0]["cid"].as_str().unwrap().to_owned();
    let block = lattice_cashew::cbor::decode(&entries[&cid]).unwrap();
    (
        TemplateJob {
            id: 1,
            tip_cid: cid,
            tip_epoch: 0,
            pool_version: 0,
            transactions: vec![],
            request: TemplateRequest::default(),
        },
        entries,
        block,
    )
}
#[test]
fn minimum_work_only_changes_search_and_work_identity() {
    let (mut job, source, prev) = fixture();
    let now = prev.timestamp + 1000;
    let plain = template::build_root_template(&job, &source, now, None)
        .unwrap()
        .build;
    let mut w = [0; 32];
    w[0] = 0x80;
    job.request
        .minimum_work
        .insert(vec!["Nexus".into()], UInt256(w));
    let filtered = template::build_root_template(&job, &source, now, None)
        .unwrap()
        .build;
    assert_eq!(
        content_id(&plain.block).unwrap(),
        content_id(&filtered.block).unwrap()
    );
    assert_eq!(filtered.search_target.0[31], 1);
    assert_eq!(filtered.search_target.0[..31], [0; 31]);
    assert_eq!(plain.block.target, prev.next_target);
    assert_ne!(plain.work_id, filtered.work_id);
    assert_eq!(plain.digest, filtered.digest);
    assert_eq!(filtered.chains.len(), 1);
    assert_eq!(filtered.targets, [filtered.search_target]);
    assert!(template::minimum_work_target(UInt256([0; 32])).is_none());
    assert!(template::minimum_work_target(UInt256([255; 32])).is_none());
}
#[test]
fn execution_bisection_keeps_valid_transactions_after_conflict() {
    let (mut job, source, prev) = fixture();
    let tx = |key: &str, old: Option<&str>, value: &str| {
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
    };
    job.transactions = vec![
        tx("a", None, "1"),
        tx("a", Some("wrong"), "2"),
        tx("b", None, "3"),
    ];
    let result = template::build_root_template(&job, &source, prev.timestamp + 1, None).unwrap();
    let content = resolve_block_content(&result.build.block, &source, i64::MAX).unwrap();
    assert_eq!(content.transactions.len(), 2);
    assert!(result.refused.is_empty());
    assert_eq!(
        content_id(&content.transactions[0]).unwrap(),
        content_id(&job.transactions[0]).unwrap()
    );
    assert_eq!(
        content_id(&content.transactions[1]).unwrap(),
        content_id(&job.transactions[2]).unwrap()
    );
}
#[test]
fn byte_budget_and_absent_content_fail_without_issuing_work() {
    let (mut job, mut source, mut prev) = fixture();
    let mut spec: ChainSpec = lattice_cashew::cbor::decode(&source[&prev.spec.0.raw_cid]).unwrap();
    spec.max_block_size = 1;
    prev.spec = Volume::new(spec).unwrap();
    source.insert(
        prev.spec.0.raw_cid.clone(),
        prev.spec.0.bytes(None).unwrap(),
    );
    job.tip_cid = content_id(&prev).unwrap();
    source.insert(job.tip_cid.clone(), canonical_bytes(&prev).unwrap());
    assert!(matches!(
        template::build_root_template(&job, &source, prev.timestamp + 1, None),
        Err(template::TemplateBuildError::TooLarge)
    ));
    source.remove(&job.tip_cid);
    assert!(matches!(
        template::build_root_template(&job, &source, 0, None),
        Err(template::TemplateBuildError::Content(_))
    ));
}
