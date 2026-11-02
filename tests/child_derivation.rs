//! Actual header admission and proof derivation; these tests do not claim body execution.
use lattice_block_tree::ChainTree;
use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_node::{
    core::{levels::ChildChainCore, stream::StreamConfig, sync::HeaderEntry},
    storage::NodeStorage,
};
use lattice_primitives::*;
use std::{collections::BTreeMap, sync::Arc};
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

fn funded(path: &[&str]) -> Block {
    let mut block = root();
    let mut spec = block.spec.0.node.as_deref().unwrap().clone();
    spec.premine = 1;
    block.spec = Volume::new(spec).unwrap();
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
        chain_path: path.iter().map(|s| (*s).into()).collect(),
    };
    block.transactions = Header::new(
        MerkleDictionary::default()
            .inserting(
                "0",
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
    state.account_state =
        Volume::new(AccountState::default().inserting("alice", 100).unwrap()).unwrap();
    block.post_state = Volume::new(state).unwrap();
    block
}
fn setup() -> (tempfile::TempDir, Arc<NodeStorage>, ChainTree, Block) {
    let root = funded(&["Nexus"]);
    let cid = content_id(&root).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(NodeStorage::open(dir.path(), &cid, "derive").unwrap());
    let mut tree =
        ChainTree::empty(ChainRuntimeContext::new(vec!["Nexus".into()], Some(&cid)).unwrap());
    let genesis = lattice_validation::validate_root_genesis(
        &Volume::new(root.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    storage
        .import_root_genesis(&mut tree, &genesis, "derive")
        .unwrap();
    tree.hold_spec(
        root.spec.0.node.as_deref().unwrap().clone(),
        &root.spec.0.raw_cid,
    );
    tree.serve_runs("Alpha");
    (dir, storage, tree, root)
}
fn child_core(storage: Arc<NodeStorage>, parent: &ChainTree, path: &[&str]) -> ChildChainCore {
    ChildChainCore::open(
        storage,
        ChainRuntimeContext::new(path.iter().map(|s| (*s).into()).collect(), None).unwrap(),
        parent,
        vec![],
        "derive",
        StreamConfig::default(),
        8,
    )
    .unwrap()
}
fn carrier(
    storage: &NodeStorage,
    parent: &mut ChainTree,
    genesis: &Block,
    child: &Block,
    nonce: u64,
) -> String {
    let mut block = genesis.clone();
    block.parent = Some(Volume::new(genesis.clone()).unwrap());
    block.height = 1;
    block.prev_state = genesis.post_state.clone();
    block.timestamp = 2000;
    block.nonce = nonce;
    block.children = Header::new(BTreeMap::from([(
        "Alpha".into(),
        Volume::new(child.clone()).unwrap(),
    )]))
    .unwrap();
    let volumes =
        lattice_cashew::volume::serialize_recursive(&Volume::new(block.clone()).unwrap(), &())
            .unwrap();
    storage.cache_volumes("fixture", &volumes).unwrap();
    storage
        .import_root_header(
            parent,
            &block,
            block.children.node.as_deref().unwrap(),
            2000,
            "derive",
        )
        .unwrap();
    content_id(&block).unwrap()
}
fn entry(block: &Block) -> HeaderEntry {
    HeaderEntry {
        block: block.clone(),
        children: block.children.node.as_deref().cloned(),
        spec: block
            .parent
            .is_none()
            .then(|| block.spec.0.node.as_deref().unwrap().clone()),
        proofs: vec![],
    }
}
fn drain(core: &mut ChildChainCore, storage: &NodeStorage) -> Vec<usize> {
    let mut batches = vec![];
    loop {
        let jobs = core.derivation_work();
        if jobs.is_empty() {
            break;
        }
        batches.push(jobs.len());
        for job in jobs {
            let held = storage
                .journal()
                .header(job.child_cid())
                .unwrap()
                .map(|(b, _)| b);
            let upstream = if job.path().len() > 2 {
                storage
                    .journal()
                    .saved_proofs(&job.path()[..job.path().len() - 1], &[job.carrier().into()])
                    .unwrap()
                    .remove(job.carrier())
                    .unwrap_or_default()
            } else {
                vec![]
            };
            let completion = job.run(held.as_ref(), &upstream, storage);
            core.derivation_completed(completion, 3000).unwrap();
        }
    }
    batches
}
#[test]
fn forty_carriers_are_dispatched_in_bounded_batches_without_losing_tail_or_double_credit() {
    let (_dir, storage, mut parent, genesis) = setup();
    let mut child = funded(&["Nexus", "Alpha"]);
    child.parent_state = genesis.post_state.clone();
    let cid = content_id(&child).unwrap();
    for n in 0..40 {
        carrier(&storage, &mut parent, &genesis, &child, n);
    }
    let mut core = child_core(storage.clone(), &parent, &["Nexus", "Alpha"]);
    assert!(
        core.queue_header(entry(&child), None, Some(&parent), 3000)
            .unwrap()
    );
    assert_eq!(core.header_status(&cid), "awaiting");
    core.schedule_derivations(&parent, std::slice::from_ref(&cid));
    core.schedule_derivations(&parent, std::slice::from_ref(&cid));
    assert_eq!(drain(&mut core, &storage), [32, 8]);
    assert_eq!(
        core.tree()
            .consensus_block(&cid)
            .unwrap()
            .work_contributions
            .len(),
        40
    );
    core.schedule_derivations(&parent, std::slice::from_ref(&cid));
    assert!(core.derivation_work().is_empty());
    carrier(&storage, &mut parent, &genesis, &child, 40);
    core.parent_advanced(&parent, 3000).unwrap();
    assert_eq!(drain(&mut core, &storage), [1]);
    assert_eq!(
        core.tree()
            .consensus_block(&cid)
            .unwrap()
            .work_contributions
            .len(),
        41
    );
}
#[test]
fn nested_child_derives_every_upstream_grind_beyond_peer_per_header_cap() {
    let (_dir, storage, mut parent, genesis) = setup();
    let mut alpha0 = funded(&["Nexus", "Alpha"]);
    alpha0.parent_state = genesis.post_state.clone();
    carrier(&storage, &mut parent, &genesis, &alpha0, 100);
    let mut alpha = child_core(storage.clone(), &parent, &["Nexus", "Alpha"]);
    alpha
        .queue_header(entry(&alpha0), None, Some(&parent), 3000)
        .unwrap();
    drain(&mut alpha, &storage);
    let mut beta0 = funded(&["Nexus", "Alpha", "Beta"]);
    beta0.parent_state = alpha0.post_state.clone();
    let beta_cid = content_id(&beta0).unwrap();
    let mut alpha1 = alpha0.clone();
    alpha1.prev_state = alpha0.post_state.clone();
    alpha1.parent = Some(Volume::new(alpha0).unwrap());
    alpha1.height = 1;
    alpha1.timestamp = 2000;
    alpha1.children = Header::new(BTreeMap::from([(
        "Beta".into(),
        Volume::new(beta0.clone()).unwrap(),
    )]))
    .unwrap();
    for n in 0..12 {
        carrier(&storage, &mut parent, &genesis, &alpha1, n);
    }
    alpha
        .queue_header(entry(&alpha1), None, Some(&parent), 3000)
        .unwrap();
    drain(&mut alpha, &storage);
    assert_eq!(
        alpha
            .tree()
            .consensus_block(&content_id(&alpha1).unwrap())
            .unwrap()
            .work_contributions
            .len(),
        12
    );
    let mut alpha_parent = alpha.tree().clone();
    alpha_parent.serve_runs("Beta");
    let mut beta = child_core(storage.clone(), &alpha_parent, &["Nexus", "Alpha", "Beta"]);
    beta.queue_header(entry(&beta0), None, Some(&alpha_parent), 3000)
        .unwrap();
    // Locally derived corrupt upstream bytes cannot mint work or blame peers.
    let mut jobs = beta.derivation_work();
    assert_eq!(jobs.len(), 1);
    let job = jobs.pop().unwrap();
    let mut forged = storage
        .journal()
        .saved_proofs(&["Nexus".into(), "Alpha".into()], &[job.carrier().into()])
        .unwrap()
        .remove(job.carrier())
        .unwrap();
    for proof in &mut forged {
        proof.directory_path[0] = "Wrong".into();
    }
    let completion = job.run(None, &forged, storage.as_ref());
    let effects = beta.derivation_completed(completion, 3000).unwrap();
    assert!(effects.headers.is_empty());
    assert!(!beta.tree().contains(&beta_cid));
    beta.schedule_derivations(&alpha_parent, std::slice::from_ref(&beta_cid));
    assert_eq!(drain(&mut beta, &storage), [1]);
    assert_eq!(
        beta.tree()
            .consensus_block(&beta_cid)
            .unwrap()
            .work_contributions
            .len(),
        12
    );
    beta.schedule_derivations(&alpha_parent, &[beta_cid]);
    assert!(beta.derivation_work().is_empty());
}

#[test]
fn missing_local_content_grants_no_work_and_an_arrival_retries_without_busy_loop() {
    let (_dir, storage, mut parent, genesis) = setup();
    let mut child = funded(&["Nexus", "Alpha"]);
    child.parent_state = genesis.post_state.clone();
    let cid = content_id(&child).unwrap();
    carrier(&storage, &mut parent, &genesis, &child, 1);
    let mut core = child_core(storage.clone(), &parent, &["Nexus", "Alpha"]);
    core.queue_header(entry(&child), None, Some(&parent), 3000)
        .unwrap();
    let mut jobs = core.derivation_work();
    assert_eq!(jobs.len(), 1);
    let result = jobs
        .pop()
        .unwrap()
        .run(None, &[], &BTreeMap::<String, Vec<u8>>::new());
    core.derivation_completed(result, 3000).unwrap();
    assert!(!core.tree().contains(&cid));
    core.schedule_derivations(&parent, &[]);
    assert!(core.derivation_work().is_empty());
    core.queue_header(entry(&child), None, Some(&parent), 3000)
        .unwrap();
    assert_eq!(drain(&mut core, &storage), [1]);
    assert!(core.tree().contains(&cid));
}
