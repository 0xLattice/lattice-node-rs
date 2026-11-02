use lattice_block_tree::{ChainTree, ChildBlockProof};
use lattice_cashew::{Header, MerkleDictionary, Volume, VolumeMerkleDictionary, cbor};
use lattice_node::{
    core::{chain::RootChainCore, stream::StreamConfig},
    storage::*,
};
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
use lattice_node::core::mining::*;
#[test]
fn detached_child_preflight_retries_parent_revision_and_fences_child_epoch() {
    let source = BTreeMap::<String, Vec<u8>>::new();
    let mut parent = root();
    let mut spec = parent.spec.0.node.as_deref().unwrap().clone();
    spec.premine = 1;
    parent.spec = Volume::new(spec).unwrap();
    let body = TransactionBody {
        account_actions: vec![AccountAction {
            owner: "alice".into(),
            delta: 100,
        }],
        actions: vec![],
        deposit_actions: vec![],
        receipt_actions: vec![],
        withdrawal_actions: vec![],
        signers: vec![],
        nonce: 0,
        chain_path: vec!["Nexus".into()],
    };
    parent.transactions = Header::new(
        MerkleDictionary::default()
            .inserting(
                "tx",
                Volume::new(Transaction {
                    body: Header::new(body).unwrap(),
                    signatures: BTreeMap::new(),
                })
                .unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let mut state = LatticeState::empty().unwrap();
    state.account_state = Volume::new(
        VolumeMerkleDictionary::default()
            .inserting("alice", 100u64)
            .unwrap(),
    )
    .unwrap();
    parent.post_state = Volume::new(state).unwrap();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(parent.clone()).unwrap(),
        &source,
        i64::MAX,
    )
    .unwrap();
    let hash = content_id(&parent).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(tmp.path(), &hash, "node").unwrap();
    let storage = std::sync::Arc::new(storage);
    let mut root_core =
        RootChainCore::open(storage.clone(), &genesis, "log", StreamConfig::default(), 8).unwrap();
    let path = vec!["Nexus".into(), "Alpha".into()];
    let mut child = root();
    child.parent_state = parent.post_state.clone();
    let child_genesis = lattice_validation::validate_child_genesis(
        &Volume::new(child.clone()).unwrap(),
        &source,
        i64::MAX,
        &path,
    )
    .unwrap();
    let proof_for = |block: &Block| {
        let mut carrier = root();
        carrier.prev_state = parent.post_state.clone();
        carrier.nonce = block.height + 1;
        carrier.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(block.clone()).unwrap(),
        )]))
        .unwrap();
        let bytes = BTreeMap::from([
            (
                content_id(&carrier).unwrap(),
                cbor::encode(&carrier).unwrap(),
            ),
            (
                carrier.children.raw_cid.clone(),
                cbor::encode(carrier.children.node.as_deref().unwrap()).unwrap(),
            ),
            (content_id(block).unwrap(), cbor::encode(block).unwrap()),
        ]);
        ChildBlockProof::generate(&content_id(&carrier).unwrap(), "Alpha", &bytes).unwrap()
    };
    let proof = proof_for(&child);

    let mut tree = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
    storage
        .import_child_genesis(&mut tree, root_core.tree(), &child_genesis, &proof, "log")
        .unwrap();
    let child_cid = content_id(&child).unwrap();

    let mut mining = DurableMiningCore::new(
        MiningState::new(
            child_cid.clone(),
            Some(child.spec.0.node.as_deref().unwrap().clone()),
            MiningConfig::default(),
        ),
        storage.clone(),
    );
    let tx = Transaction::new(
        BTreeMap::new(),
        Header::new(TransactionBody {
            account_actions: vec![AccountAction {
                owner: "alice".into(),
                delta: -10,
            }],
            actions: vec![],
            deposit_actions: vec![],
            receipt_actions: vec![],
            withdrawal_actions: vec![],
            signers: vec!["alice".into()],
            nonce: 0,
            chain_path: path.clone(),
        })
        .unwrap(),
    );
    let cid = content_id(&tx).unwrap();
    let take_job = |effects: Vec<MiningEffect>| {
        effects
            .into_iter()
            .find_map(|e| match e {
                MiningEffect::Preflight(job) => Some(job),
                _ => None,
            })
            .unwrap()
    };
    let job = take_job(
        mining
            .step(
                MiningEvent::TransactionReceived(tx, TransactionOrigin::Submitted(7)),
                2000,
            )
            .unwrap(),
    );
    let task = mining
        .prepare_child_preflight(&job, &tree, root_core.tree())
        .unwrap()
        .unwrap();
    let old_revision = root_core.tree().current_revision();
    let mut next = parent.clone();
    next.parent = Some(Volume::new(parent.clone()).unwrap());
    next.height = 1;
    next.timestamp = 2000;
    next.prev_state = parent.post_state.clone();
    root_core
        .admit_header(&next, &BTreeMap::new(), 2000)
        .unwrap();
    assert!(root_core.tree().current_revision() > old_revision);
    assert_eq!(
        root_core.tree().heaviest_executed_tip(&Default::default()),
        hash
    );
    let done = std::thread::spawn(move || task.run(2000))
        .join()
        .unwrap()
        .unwrap();
    let retry = take_job(
        mining
            .complete_child_preflight(done, root_core.tree(), 2100)
            .unwrap(),
    );
    assert_eq!(retry.tip_epoch, job.tip_epoch);
    assert!(mining.state().is_pending(&cid));
    let task = mining
        .prepare_child_preflight(&retry, &tree, root_core.tree())
        .unwrap()
        .unwrap();
    let effects = mining
        .complete_child_preflight(task.run(2100).unwrap(), root_core.tree(), 2100)
        .unwrap();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, MiningEffect::TransactionRefused(7, _)))
    );
    assert!(!mining.state().is_pending(&cid));
    // A -> B -> A cannot revive an older completion even with identical CIDs.
    let old = mining
        .prepare_child_preflight(&retry, &tree, root_core.tree())
        .unwrap()
        .unwrap();
    for tip in ["other".to_owned(), child_cid] {
        mining
            .step(
                MiningEvent::TipMoved(TipMove {
                    tip_cid: tip,
                    confirmed: Default::default(),
                    returned: vec![],
                    left: vec![],
                }),
                2200,
            )
            .unwrap();
    }
    assert!(
        mining
            .complete_child_preflight(old.run(2100).unwrap(), root_core.tree(), 2200)
            .unwrap()
            .is_empty()
    );
}
