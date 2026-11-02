use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_node::core::work::*;
use lattice_primitives::*;
use std::collections::BTreeMap;
fn root() -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: None,
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume::new(ChainSpec {
            max_number_of_transactions_per_block: 100,
            max_state_growth: 100_000,
            max_block_size: 1_000_000,
            premine: 0,
            target_block_time: 1000,
            initial_reward: 1024,
            halving_interval: 210_000,
            half_life: 120,
            wasm_policies: vec![],
        })
        .unwrap(),
        parent_state: empty.clone(),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: 0,
        timestamp: 1000,
        reward_recipient: None,
        nonce: 0,
    }
}
fn template(id: &str, expires: i64) -> WorkTemplate {
    WorkTemplate {
        work_id: id.into(),
        block: root(),
        search_target: UInt256([255; 32]),
        targets: vec![],
        tip_cid: "tip".into(),
        pool_version: 0,
        expires_at: expires,
        digest: String::new(),
        chains: vec![],
    }
}
#[test]
fn work_issue_retains_live_id_refreshes_order_and_expires_exactly() {
    let mut book = WorkTemplateBook::new(100, 2);
    book.issue(template("a", 100), 0);
    book.issue(template("b", 200), 0);
    let old = book.issue(template("a", 900), 1);
    assert_eq!(old.expires_at, 100);
    book.issue(template("c", 200), 2);
    assert!(book.template("b").is_none());
    assert_eq!(book.submission("a", 123, 99).unwrap().nonce, 123);
    assert_eq!(
        book.submission("a", 123, 100).unwrap_err(),
        TemplateError::Expired
    );
    assert_eq!(
        book.submission("a", 123, 101).unwrap_err(),
        TemplateError::UnknownWork
    );
    let mut zero = template("zero", 200);
    zero.search_target = UInt256([0; 32]);
    book.issue(zero, 102);
    assert_eq!(
        book.submission("zero", 0, 103).unwrap_err(),
        TemplateError::MissesSearchTarget
    );
    assert!(book.template("zero").is_some());
    book.invalidate_all();
    assert_eq!(book.count(), 0);
}
