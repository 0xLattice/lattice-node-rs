//! Real authenticated header imports exercise the production turn boundary.
mod finality;
mod formal_index;
use super::*;
use lattice_block_tree::{ChainTree, ChildBlockProof};
use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_primitives::*;
use std::sync::Arc;
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

fn fixture(directory: &Path) -> (Arc<NodeStorage>, ChainTree, ChainTree, Block, Block, Block) {
    let genesis = funded(&["Nexus"]);
    let genesis_cid = content_id(&genesis).unwrap();
    let storage = Arc::new(NodeStorage::open(directory, &genesis_cid, "atomic").unwrap());
    let verified = lattice_validation::validate_root_genesis(
        &Volume::new(genesis.clone()).unwrap(),
        &BTreeMap::<String, Vec<u8>>::new(),
        i64::MAX,
    )
    .unwrap();
    let mut root = ChainTree::empty(
        ChainRuntimeContext::new(vec!["Nexus".into()], Some(&genesis_cid)).unwrap(),
    );
    storage
        .import_root_genesis(&mut root, &verified, "atomic")
        .unwrap();
    root.hold_spec(
        genesis.spec.0.node.as_deref().unwrap().clone(),
        &genesis.spec.0.raw_cid,
    );
    root.serve_runs("Alpha");
    let mut child = funded(&["Nexus", "Alpha"]);
    child.parent_state = genesis.post_state.clone();
    let mut carrier = genesis.clone();
    carrier.parent = Some(Volume::new(genesis.clone()).unwrap());
    carrier.height = 1;
    carrier.timestamp = 2000;
    carrier.prev_state = genesis.post_state.clone();
    carrier.children = Header::new(BTreeMap::from([(
        "Alpha".into(),
        Volume::new(child.clone()).unwrap(),
    )]))
    .unwrap();
    let child_tree = ChainTree::empty(
        ChainRuntimeContext::new(vec!["Nexus".into(), "Alpha".into()], None).unwrap(),
    );
    (storage, root, child_tree, genesis, carrier, child)
}
fn import_parent(
    storage: &NodeStorage,
    tree: &mut ChainTree,
    block: &Block,
) -> Result<(), StoreError> {
    let volumes = lattice_cashew::volume::serialize_recursive(&Volume::new(block.clone())?, &())?;
    storage.cache_volumes("turn-content", &volumes)?;
    storage.import_root_header(
        tree,
        block,
        block.children.node.as_deref().unwrap(),
        2000,
        "atomic",
    )?;
    Ok(())
}
fn import_child(
    storage: &NodeStorage,
    tree: &mut ChainTree,
    carrier: &Block,
    child: &Block,
) -> Result<(), StoreError> {
    let proof = ChildBlockProof::generate(&content_id(carrier)?, "Alpha", storage)?;
    storage.import_child_genesis_header(
        tree,
        child,
        child.children.node.as_deref().unwrap(),
        child.spec.0.node.as_deref().unwrap(),
        &proof,
        "atomic",
    )?;
    Ok(())
}
fn assert_recovered(
    directory: &Path,
    genesis: &Block,
    carrier: &Block,
    child: &Block,
    committed: bool,
) {
    let genesis_cid = content_id(genesis).unwrap();
    let storage = NodeStorage::open(directory, &genesis_cid, "atomic").unwrap();
    let (mut tree, _) = storage
        .journal()
        .restore_chain(
            ChainRuntimeContext::new(vec!["Nexus".into()], Some(&genesis_cid)).unwrap(),
            None,
            vec![genesis.spec.0.node.as_deref().unwrap().clone()],
        )
        .unwrap();
    tree.serve_runs("Alpha");
    assert_eq!(tree.contains(&content_id(carrier).unwrap()), committed);
    let path = vec!["Nexus".into(), "Alpha".into()];
    let storage = Arc::new(storage);
    let level = crate::core::levels::ChildChainCore::open(
        storage.clone(),
        ChainRuntimeContext::new(path.clone(), None).unwrap(),
        &tree,
        vec![child.spec.0.node.as_deref().unwrap().clone()],
        "atomic",
        crate::core::stream::StreamConfig::default(),
        8,
    )
    .unwrap();
    assert_eq!(
        level.tree().contains(&content_id(child).unwrap()),
        committed
    );
    assert_eq!(
        !storage
            .journal()
            .saved_proofs(&path, &[content_id(child).unwrap()])
            .unwrap()
            .is_empty(),
        committed
    );
}
#[test]
fn failed_second_level_rolls_back_parent_and_child_and_same_handle_observers() {
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (storage, mut root, mut level, genesis, carrier, child) = fixture(directory.path());
        if fail {
            storage.journal().db.lock().unwrap().execute_batch("CREATE TEMP TRIGGER reject_child BEFORE INSERT ON admission_batches WHEN NEW.chain_path='Nexus/Alpha' BEGIN SELECT RAISE(ABORT,'injected child journal failure'); END;").unwrap();
        }
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let observed = storage.clone();
        let observer = std::thread::spawn(move || {
            started_rx.recv().unwrap();
            let root = observed
                .journal()
                .import_batches(&["Nexus".into()])
                .unwrap();
            let child = observed
                .journal()
                .import_batches(&["Nexus".into(), "Alpha".into()])
                .unwrap();
            observed_tx.send((root.len(), child.len())).unwrap();
        });
        let (write_tx, write_rx) = std::sync::mpsc::channel();
        let (written_tx, written_rx) = std::sync::mpsc::channel();
        let writer_storage = storage.clone();
        let writer = std::thread::spawn(move || {
            write_rx.recv().unwrap();
            // Both retention and journal entry contend on the SAME storage.
            writer_storage
                .persist_materialized_batch(
                    &[NodeFactBatch {
                        path: vec!["Nexus".into(), "Other".into()],
                        facts: vec![],
                        headers: vec![],
                        proofs: vec![],
                        volume_roots: vec![],
                        cursors: BTreeMap::from([(
                            "observer-peer".into(),
                            StreamCursor {
                                log_id: "external".into(),
                                position: 7,
                            },
                        )]),
                    }],
                    &[],
                    "atomic",
                )
                .unwrap();
            written_tx.send(()).unwrap();
        });
        let before = storage
            .journal()
            .import_batches(&["Nexus".into()])
            .unwrap()
            .len();
        let result: Result<(), StoreError> = storage.atomic_fact_turn(|| {
            import_parent(&storage, &mut root, &carrier)?;
            assert!(storage.journal().header(&content_id(&carrier)?)?.is_some());
            started_tx.send(()).unwrap();
            write_tx.send(()).unwrap();
            assert!(observed_rx.recv_timeout(Duration::from_millis(50)).is_err());
            assert!(written_rx.recv_timeout(Duration::from_millis(50)).is_err());
            import_child(&storage, &mut level, &carrier, &child)?;
            Ok(())
        });
        assert_eq!(result.is_err(), fail);
        let (roots, children) = observed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(roots > before, !fail);
        assert_eq!(children > 0, !fail);
        written_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        observer.join().unwrap();
        writer.join().unwrap();
        assert_eq!(
            storage
                .journal()
                .chain_cursors(&["Nexus".into(), "Other".into()])
                .unwrap()["observer-peer"]
                .position,
            7
        );
        // Provisional in-memory trees are intentionally discarded on failure.
        drop(storage);
        drop(root);
        drop(level);
        assert_recovered(directory.path(), &genesis, &carrier, &child, !fail);
    }
}
#[test]
fn atomic_turn_crash_child() {
    let Ok(directory) = std::env::var("LATTICE_ATOMIC_TEST_DIRECTORY") else {
        return;
    };
    let after = std::env::var("LATTICE_ATOMIC_TEST_AFTER").unwrap() == "true";
    let (storage, mut root, mut level, _, carrier, child) = fixture(Path::new(&directory));
    let result: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        import_parent(&storage, &mut root, &carrier)?;
        import_child(&storage, &mut level, &carrier, &child)?;
        if !after {
            std::process::exit(77);
        }
        Ok(())
    });
    result.unwrap();
    std::process::exit(78);
}
#[test]
fn abrupt_process_exit_exposes_all_levels_or_none_on_recovery() {
    for after in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::turn_tests::atomic_turn_crash_child",
                "--nocapture",
            ])
            .env("LATTICE_ATOMIC_TEST_DIRECTORY", directory.path())
            .env("LATTICE_ATOMIC_TEST_AFTER", after.to_string())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if after { 78 } else { 77 }));
        // Derive identical fixture identifiers in a separate disposable store.
        let identifiers = tempfile::tempdir().unwrap();
        let (storage, _, _, genesis, carrier, child) = fixture(identifiers.path());
        drop(storage);
        assert_recovered(directory.path(), &genesis, &carrier, &child, after);
    }
}

#[test]
fn deferred_retention_preserves_previous_state_on_failure_and_later_readds() {
    use lattice_volume_broker::RetainedRootBroker;
    let directory = tempfile::tempdir().unwrap();
    let (storage, _, _, _, _, _) = fixture(directory.path());
    let a = Volume::new(1u64).unwrap();
    let b = Volume::new(2u64).unwrap();
    let mut volumes = lattice_cashew::volume::serialize_recursive(&a, &()).unwrap();
    volumes.extend(lattice_cashew::volume::serialize_recursive(&b, &()).unwrap());
    storage
        .cache_volumes("retention-fixture", &volumes)
        .unwrap();
    let a = [a.0.raw_cid].into_iter().collect();
    let b = [b.0.raw_cid].into_iter().collect();
    let path = vec!["Nexus".into()];
    let scope = RetentionScopes {
        chain: "atomic".into(),
    }
    .state(&path);
    storage
        .retain_execution_content(StateRetention::None, &path, &a, &Default::default())
        .unwrap();
    let result: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        storage.retain_execution_content(StateRetention::None, &path, &b, &Default::default())?;
        storage.retain_execution_content(StateRetention::None, &path, &a, &Default::default())?;
        Ok(())
    });
    result.unwrap();
    assert_eq!(storage.broker.retained_roots(&scope).unwrap(), a);
    let result: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        storage.retain_execution_content(StateRetention::None, &path, &b, &Default::default())?;
        Err(StoreError::Corrupt(
            "injected failure after retention".into(),
        ))
    });
    assert!(result.is_err());
    assert!(a.is_subset(&storage.broker.retained_roots(&scope).unwrap()));
}

#[test]
fn child_activation_preserves_other_levels_pins_and_rollback_restores_local_rows() {
    use crate::core::{
        mempool::MempoolItem,
        mining::{DurableMiningCore, MiningConfig, MiningState, PoolDelta},
    };
    let directory = tempfile::tempdir().unwrap();
    let (storage, _, _, _, _, child) = fixture(directory.path());
    let item = |nonce| {
        let tx = Transaction::new(
            BTreeMap::new(),
            Header::new(TransactionBody {
                account_actions: vec![AccountAction {
                    owner: "alice".into(),
                    delta: -1,
                }],
                actions: vec![],
                deposit_actions: vec![],
                receipt_actions: vec![],
                withdrawal_actions: vec![],
                signers: vec!["alice".into()],
                nonce,
                chain_path: vec!["Nexus".into()],
            })
            .unwrap(),
        );
        MempoolItem {
            cid: content_id(&tx).unwrap(),
            transaction: tx,
            added_at: 2000,
        }
    };
    let local = item(0);
    let volatile = item(1);
    storage
        .persist_pool_delta(&PoolDelta {
            added: vec![local.clone(), volatile.clone()],
            journaled: vec![local.clone()],
            removed: vec![],
        })
        .unwrap();
    let before = storage.live_mempool_roots().unwrap();
    assert_eq!(before.len(), 2);
    let activate = || {
        DurableMiningCore::restore_level(
            MiningState::new(
                content_id(&child).unwrap(),
                Some(child.spec.0.node.as_deref().unwrap().clone()),
                MiningConfig::default(),
            ),
            storage.clone(),
            &["Nexus".into(), "Alpha".into()],
            2000,
        )
    };
    let success: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        let (_, effects) = activate()?;
        assert!(effects.is_empty());
        assert_eq!(storage.live_mempool_roots()?, before);
        Ok(())
    });
    success.unwrap();
    assert_eq!(storage.live_mempool_roots().unwrap(), before);
    let failed: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        storage.persist_pool_delta(&PoolDelta {
            added: vec![],
            journaled: vec![],
            removed: vec![local.cid.clone()],
        })?;
        assert!(storage.journal().local_mempool_transactions()?.is_empty());
        let _ = activate()?;
        // Even accidental future use of startup reconciliation cannot release
        // the old committed pool's owners inside a provisional turn.
        assert!(storage.reconcile_mempool_roots().is_err());
        Err(StoreError::Corrupt(
            "injected activation turn failure".into(),
        ))
    });
    assert!(failed.is_err());
    assert_eq!(storage.live_mempool_roots().unwrap(), before);
    assert_eq!(
        storage.journal().local_mempool_transactions().unwrap()[0].transaction_cid,
        local.cid
    );
    assert!(storage.fetch_volume(&local.cid).is_some());
    // Process startup still drops volatile owners and retains durable rows.
    storage.reconcile_mempool_roots().unwrap();
    assert_eq!(
        storage.live_mempool_roots().unwrap(),
        [local.cid].into_iter().collect()
    );
}

#[test]
fn joined_failure_cannot_be_swallowed_and_commit_provisional_facts() {
    let directory = tempfile::tempdir().unwrap();
    let (storage, mut root, _, _, carrier, _) = fixture(directory.path());
    let before = storage.journal().import_batches(&["Nexus".into()]).unwrap();
    let result: Result<(), StoreError> = storage.atomic_fact_turn(|| {
        import_parent(&storage, &mut root, &carrier)?;
        let failed: Result<(), StoreError> =
            storage.fact_turn_or_join(|| Err(StoreError::Corrupt("nested failure".into())));
        assert!(failed.is_err());
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(
        storage.journal().import_batches(&["Nexus".into()]).unwrap(),
        before
    );
}

#[test]
fn one_verified_child_result_commits_unblocked_headers_proofs_and_cursor_together() {
    child_event_atomic_fixture(false);
}

#[test]
fn one_child_index_result_commits_unblocked_headers_proofs_and_cursor_together() {
    child_event_atomic_fixture(true);
}

fn child_event_atomic_fixture(via_index: bool) {
    let mut formal_rows = vec![];
    use crate::core::{
        levels::ChildChainCore,
        stream::{StreamConfig, SyncEffect},
        sync::{HeaderEntry, LogEntry, PeerId, StreamEntry, StreamPage, SyncMessage},
    };
    for failure in ["none", "second_header", "second_proof", "cursor"] {
        let directory = tempfile::tempdir().unwrap();
        let (storage, parent, _, genesis, carrier0, child0) = fixture(directory.path());
        let mut child1 = child0.clone();
        child1.parent = Some(Volume::new(child0.clone()).unwrap());
        child1.height = 1;
        child1.timestamp = 2000;
        child1.prev_state = child0.post_state.clone();
        let mut carrier1 = carrier0.clone();
        carrier1.nonce += 1;
        carrier1.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(child1.clone()).unwrap(),
        )]))
        .unwrap();
        for carrier in [&carrier0, &carrier1] {
            storage
                .cache_volumes(
                    "proof-fixture",
                    &lattice_cashew::volume::serialize_recursive(
                        &Volume::new(carrier.clone()).unwrap(),
                        &(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let path = vec!["Nexus".into(), "Alpha".into()];
        let mut core = ChildChainCore::open(
            storage.clone(),
            ChainRuntimeContext::new(path.clone(), None).unwrap(),
            &parent,
            vec![],
            "atomic",
            StreamConfig::default(),
            8,
        )
        .unwrap();
        let peer = PeerId {
            key: "proof-peer".into(),
            session: 1,
        };
        let ready = core.peer_ready(peer.clone(), 3000);
        let request_id = ready
            .sync
            .iter()
            .find_map(|effect| match effect {
                SyncEffect::Send(_, SyncMessage::GetStream { request_id, .. }) => Some(*request_id),
                _ => None,
            })
            .unwrap();
        let cid0 = content_id(&child0).unwrap();
        let cid1 = content_id(&child1).unwrap();
        core.stream_page(
            &peer,
            StreamPage {
                request_id,
                log_id: "remote".into(),
                has_more: false,
                entries: vec![
                    StreamEntry {
                        position: 1,
                        entry: LogEntry::header(&cid0),
                    },
                    StreamEntry {
                        position: 2,
                        entry: LogEntry::header(&cid1),
                    },
                ],
            },
            3000,
        )
        .unwrap();
        let entry = |child: &Block, carrier: &Block| HeaderEntry {
            block: child.clone(),
            children: if via_index {
                None
            } else {
                child.children.node.as_deref().cloned()
            },
            spec: child.spec.0.node.as_deref().cloned(),
            proofs: vec![
                ChildBlockProof::generate(&content_id(carrier).unwrap(), "Alpha", storage.as_ref())
                    .unwrap(),
            ],
        };
        // The successor is authenticated first but awaits its missing parent.
        core.queue_header(entry(&child1, &carrier1), Some(peer.clone()), None, 3000)
            .unwrap();
        let first = core.proof_work().checks.remove(0).run(None);
        storage
            .fact_turn_or_join(|| core.proof_completed(first, 3000))
            .unwrap();
        assert!(!core.tree().contains(&cid1));
        core.queue_header(entry(&child0, &carrier0), Some(peer.clone()), None, 3000)
            .unwrap();
        let mut completion = Some(core.proof_work().checks.remove(0).run(None));
        if via_index {
            storage
                .fact_turn_or_join(|| core.proof_completed(completion.take().unwrap(), 3000))
                .unwrap();
            assert!(!core.tree().contains(&cid0));
            assert!(!core.tree().contains(&cid1));
            assert_eq!(
                core.sync().peers[&peer].child_index.as_ref().unwrap().cid,
                child0.children.raw_cid
            );
        }
        let root_path = vec!["Nexus".to_owned()];
        let root_cids = vec![content_id(&genesis).unwrap()];
        let child_cids = vec![cid0.clone(), cid1.clone()];
        let before_root = formal_index::observe(storage.as_ref(), &parent, &root_path, &root_cids);
        let before_child = formal_index::observe(storage.as_ref(), core.tree(), &path, &child_cids);
        let before_group = formal_index::grouped(storage.as_ref(), &path, &child_cids);
        let trigger=match failure {
            "second_header"=>format!("CREATE TEMP TRIGGER fail_proof_turn BEFORE INSERT ON headers WHEN NEW.cid='{cid1}' BEGIN SELECT RAISE(ABORT,'second header'); END"),
            "second_proof"=>format!("CREATE TEMP TRIGGER fail_proof_turn BEFORE INSERT ON child_proofs WHEN NEW.child='{cid1}' BEGIN SELECT RAISE(ABORT,'second proof'); END"),
            "cursor"=>"CREATE TEMP TRIGGER fail_proof_turn BEFORE INSERT ON stream_cursors WHEN NEW.chain_path='Nexus/Alpha' AND NEW.position=2 BEGIN SELECT RAISE(ABORT,'cursor'); END".into(),
            _=>String::new(),
        };
        if !trigger.is_empty() {
            storage
                .journal()
                .db
                .lock()
                .unwrap()
                .execute_batch(&trigger)
                .unwrap();
        }
        let index = lattice_cashew::cbor::encode(child0.children.node.as_deref().unwrap()).unwrap();
        let result = storage.fact_turn_or_join(|| {
            if via_index {
                core.child_index_fetched(&peer, &child0.children.raw_cid, Some(&index), 3000)
            } else {
                core.proof_completed(completion.take().unwrap(), 3000)
            }
        });
        assert_eq!(result.is_ok(), failure == "none", "{failure}: {result:?}");
        let saved = storage.journal().recovery_snapshot(&path).unwrap();
        let committed = failure == "none";
        assert_eq!(!saved.batches.is_empty(), committed);
        assert_eq!(
            saved.cursors[&peer.key].position,
            if committed { 2 } else { 0 }
        );
        let proofs = storage
            .journal()
            .saved_proofs(&path, &[cid0.clone(), cid1.clone()])
            .unwrap();
        assert_eq!(proofs.len(), if committed { 2 } else { 0 });
        for cid in [&cid0, &cid1] {
            assert_eq!(storage.journal().header(cid).unwrap().is_some(), committed);
        }
        let after_group = formal_index::grouped(storage.as_ref(), &path, &child_cids);
        let live_child = committed
            .then(|| formal_index::observe(storage.as_ref(), core.tree(), &path, &child_cids));
        // Failed provisional cores are dropped, never reused after the error.
        drop(core);
        drop(storage);
        // Close all SQLite handles and reacquire the production directory lock.
        let storage = Arc::new(
            NodeStorage::open(directory.path(), content_id(&genesis).unwrap(), "atomic").unwrap(),
        );
        let (mut parent, _) = storage
            .journal()
            .restore_chain(
                ChainRuntimeContext::new(root_path.clone(), Some(&root_cids[0])).unwrap(),
                None,
                vec![genesis.spec.0.node.as_deref().unwrap().clone()],
            )
            .unwrap();
        parent.serve_runs("Alpha");
        let restored = ChildChainCore::open(
            storage.clone(),
            ChainRuntimeContext::new(path.clone(), None).unwrap(),
            &parent,
            vec![],
            "atomic",
            StreamConfig::default(),
            8,
        )
        .unwrap();
        assert_eq!(restored.tree().contains(&cid0), committed);
        assert_eq!(restored.tree().contains(&cid1), committed);
        let after_root = formal_index::observe(storage.as_ref(), &parent, &root_path, &root_cids);
        let recovered_child =
            formal_index::observe(storage.as_ref(), restored.tree(), &path, &child_cids);
        let recovered_group = formal_index::grouped(storage.as_ref(), &path, &child_cids);
        assert_eq!(
            before_root, after_root,
            "index completion must not change the parent"
        );
        assert_eq!(
            after_group, recovered_group,
            "reopen must preserve complete event footprint"
        );
        if !committed {
            assert_eq!(before_group, after_group);
        }
        formal_rows.push(serde_json::json!({"failure":failure,"committed":committed,
            "beforeRoot":before_root,"beforeChild":before_child,"liveChild":live_child,
            "recoveredRoot":after_root,"recoveredChild":recovered_child,
            "beforeGroup":before_group,"afterGroup":after_group,"recoveredGroup":recovered_group}));
    }
    if via_index && let Some(path) = std::env::var_os("LATTICE_FORMAL_INDEX_TRACE") {
        std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({"version":1,"event":"authenticated-child-index","rows":formal_rows})).unwrap()).unwrap();
    }
}

#[test]
fn one_derived_result_rolls_back_every_verified_grind_on_later_proof_failure() {
    use crate::core::{
        child_proofs::ChildDeriveJob, levels::ChildChainCore, stream::StreamConfig,
        sync::HeaderEntry,
    };
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (storage, _, _, _, mut root_carrier, mut alpha) = fixture(directory.path());
        let mut beta = funded(&["Nexus", "Alpha", "Beta"]);
        beta.parent_state = alpha.post_state.clone();
        alpha.parent = Some(Volume::new(alpha.clone()).unwrap());
        alpha.prev_state = alpha.post_state.clone();
        alpha.height = 1;
        alpha.timestamp = 2000;
        alpha.children = Header::new(BTreeMap::from([(
            "Beta".into(),
            Volume::new(beta.clone()).unwrap(),
        )]))
        .unwrap();
        root_carrier.children = Header::new(BTreeMap::from([(
            "Alpha".into(),
            Volume::new(alpha.clone()).unwrap(),
        )]))
        .unwrap();
        let mut upstream = vec![];
        let mut grinds = BTreeSet::new();
        for nonce in [101, 102] {
            root_carrier.nonce = nonce;
            storage
                .cache_volumes(
                    "derive-turn",
                    &lattice_cashew::volume::serialize_recursive(
                        &Volume::new(root_carrier.clone()).unwrap(),
                        &(),
                    )
                    .unwrap(),
                )
                .unwrap();
            let root = content_id(&root_carrier).unwrap();
            upstream.push(ChildBlockProof::generate(&root, "Alpha", storage.as_ref()).unwrap());
            grinds.insert(root);
        }
        let beta_cid = content_id(&beta).unwrap();
        let alpha_cid = content_id(&alpha).unwrap();
        let path = vec!["Nexus".into(), "Alpha".into(), "Beta".into()];
        let parent = ChainTree::empty(
            ChainRuntimeContext::new(vec!["Nexus".into(), "Alpha".into()], None).unwrap(),
        );
        let mut core = ChildChainCore::open(
            storage.clone(),
            ChainRuntimeContext::new(path.clone(), None).unwrap(),
            &parent,
            vec![],
            "atomic",
            StreamConfig::default(),
            8,
        )
        .unwrap();
        let hop = ChildBlockProof::generate(&alpha_cid, "Beta", storage.as_ref()).unwrap();
        upstream[0]
            .composing(&hop)
            .verify_securing_work(&beta, &path)
            .unwrap();
        core.queue_header(
            HeaderEntry {
                block: beta.clone(),
                children: beta.children.node.as_deref().cloned(),
                spec: beta.spec.0.node.as_deref().cloned(),
                proofs: vec![upstream[0].composing(&hop)],
            },
            None,
            None,
            3000,
        )
        .unwrap();
        // Private job authority still executes both real securing-proof checks.
        let completion = ChildDeriveJob::new(
            beta_cid.clone(),
            Some(beta),
            alpha_cid,
            grinds,
            path.clone(),
        )
        .run(None, &upstream, storage.as_ref());
        if fail {
            storage.journal().db.lock().unwrap().execute_batch("CREATE TEMP TRIGGER fail_second_derived_proof BEFORE INSERT ON child_proofs WHEN NEW.chain='Nexus/Alpha/Beta' AND (SELECT COUNT(*) FROM child_proofs WHERE chain=NEW.chain)>0 BEGIN SELECT RAISE(ABORT,'second derived proof'); END").unwrap();
        }
        let result = storage.fact_turn_or_join(|| core.derivation_completed(completion, 3000));
        assert_eq!(result.is_err(), fail);
        let saved = storage
            .journal()
            .saved_proofs(&path, std::slice::from_ref(&beta_cid))
            .unwrap();
        assert_eq!(
            saved.get(&beta_cid).map_or(0, Vec::len),
            if fail { 0 } else { 2 }
        );
        assert_eq!(
            storage.journal().import_batches(&path).unwrap().is_empty(),
            fail
        );
        if !fail {
            assert_eq!(
                core.tree()
                    .consensus_block(&beta_cid)
                    .unwrap()
                    .work_contributions
                    .len(),
                2
            );
        }
    }
}

#[test]
fn tick_rolls_back_authenticated_ready_headers_at_every_level() {
    use crate::core::{
        levels::ChildChainCore,
        stream::StreamConfig,
        sync::{HeaderEntry, HeadersResponse, LogEntry, PeerId, StreamEntry, StreamPage},
    };
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (storage, parent, _, root_genesis, carrier, _) = fixture(directory.path());
        let verified = lattice_validation::validate_root_genesis(
            &Volume::new(root_genesis.clone()).unwrap(),
            &BTreeMap::<String, Vec<u8>>::new(),
            i64::MAX,
        )
        .unwrap();
        let mut root = crate::core::chain::RootChainCore::open(
            storage.clone(),
            &verified,
            "atomic",
            StreamConfig::default(),
            8,
        )
        .unwrap();
        let mut future_root = carrier.clone();
        future_root.timestamp = 5000;
        let root_cid = content_id(&future_root).unwrap();
        let peer = PeerId {
            key: "tick-peer".into(),
            session: 1,
        };
        root.peer_ready(peer.clone(), 3000);
        let request_id = root.sync().peers[&peer].stream.as_ref().unwrap().request_id;
        root.stream_page(
            &peer,
            StreamPage {
                request_id,
                log_id: "remote".into(),
                has_more: false,
                entries: vec![StreamEntry {
                    position: 1,
                    entry: LogEntry::header(&root_cid),
                }],
            },
            3000,
        )
        .unwrap();
        let request_id = root.sync().peers[&peer].data.as_ref().unwrap().request_id;
        root.received_headers(
            &peer,
            HeadersResponse {
                request_id,
                has_more: false,
                entries: vec![HeaderEntry {
                    block: future_root.clone(),
                    children: future_root.children.node.as_deref().cloned(),
                    spec: None,
                    proofs: vec![],
                }],
            },
            3000,
        )
        .unwrap();
        assert!(!root.tree().contains(&root_cid));
        let mut cores = vec![];
        let mut children = vec![];
        for name in ["Alpha", "Beta"] {
            let path = vec!["Nexus".into(), name.into()];
            let mut parent = parent.clone();
            parent.serve_runs(name);
            let mut core = ChildChainCore::open(
                storage.clone(),
                ChainRuntimeContext::new(path.clone(), None).unwrap(),
                &parent,
                vec![],
                "atomic",
                StreamConfig::default(),
                8,
            )
            .unwrap();
            let mut genesis = funded(&["Nexus", name]);
            genesis.parent_state = carrier.prev_state.clone();
            let mut future = genesis.clone();
            future.parent = Some(Volume::new(genesis.clone()).unwrap());
            future.prev_state = genesis.post_state.clone();
            future.height = 1;
            future.timestamp = 5000;
            for block in [&genesis, &future] {
                let mut carrier = carrier.clone();
                carrier.children = Header::new(BTreeMap::from([(
                    name.to_owned(),
                    Volume::new(block.clone()).unwrap(),
                )]))
                .unwrap();
                storage
                    .cache_volumes(
                        "tick-fixture",
                        &lattice_cashew::volume::serialize_recursive(
                            &Volume::new(carrier.clone()).unwrap(),
                            &(),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                let proof = ChildBlockProof::generate(
                    &content_id(&carrier).unwrap(),
                    name,
                    storage.as_ref(),
                )
                .unwrap();
                core.queue_header(
                    HeaderEntry {
                        block: block.clone(),
                        children: block.children.node.as_deref().cloned(),
                        spec: block.spec.0.node.as_deref().cloned(),
                        proofs: vec![proof],
                    },
                    None,
                    None,
                    3000,
                )
                .unwrap();
                let completion = core.proof_work().checks.remove(0).run(None);
                storage
                    .fact_turn_or_join(|| core.proof_completed(completion, 3000))
                    .unwrap();
            }
            let cid = content_id(&future).unwrap();
            assert!(!core.tree().contains(&cid), "future header must await tick");
            assert!(core.tree().contains(&content_id(&genesis).unwrap()));
            children.push((path, cid, parent));
            cores.push(core);
        }
        if fail {
            storage.journal().db.lock().unwrap().execute_batch(&format!(
                "CREATE TEMP TRIGGER fail_tick BEFORE INSERT ON child_proofs WHEN NEW.child='{}' BEGIN SELECT RAISE(ABORT,'later level'); END", children[1].1)).unwrap();
        }
        let result = storage.atomic_fact_turn(|| -> Result<(), StoreError> {
            root.tick(6000)?;
            for core in &mut cores {
                core.tick(6000)?;
            }
            Ok(())
        });
        assert_eq!(result.is_ok(), !fail, "{result:?}");
        drop(cores);
        drop(root);
        assert_eq!(
            storage.journal().header(&root_cid).unwrap().is_some(),
            !fail
        );
        let root = crate::core::chain::RootChainCore::open(
            storage.clone(),
            &verified,
            "atomic",
            StreamConfig::default(),
            8,
        )
        .unwrap();
        assert_eq!(root.tree().contains(&root_cid), !fail);
        assert_eq!(
            root.sync().cursors[&peer.key].position,
            if fail { 0 } else { 1 }
        );
        for (path, cid, parent) in children {
            assert_eq!(storage.journal().header(&cid).unwrap().is_some(), !fail);
            assert_eq!(
                !storage
                    .journal()
                    .saved_proofs(&path, std::slice::from_ref(&cid))
                    .unwrap()
                    .is_empty(),
                !fail
            );
            let restored = ChildChainCore::open(
                storage.clone(),
                ChainRuntimeContext::new(path, None).unwrap(),
                &parent,
                vec![],
                "atomic",
                StreamConfig::default(),
                8,
            )
            .unwrap();
            assert_eq!(restored.tree().contains(&cid), !fail);
        }
    }
}
