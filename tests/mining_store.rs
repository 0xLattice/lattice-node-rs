use lattice_cashew::{Header, Volume};
use lattice_node::{
    core::{mempool::*, mining::*},
    storage::*,
};
use lattice_primitives::*;
use std::{collections::BTreeMap, sync::Arc};
fn input() -> (Transaction, ChainSpec) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/swift-mempool.json")).unwrap();
    let spec = serde_json::from_value(data[0]["scenario"]["spec"].clone()).unwrap();
    let body = TransactionBody {
        account_actions: vec![AccountAction {
            owner: "a".into(),
            delta: -10,
        }],
        actions: vec![],
        deposit_actions: vec![],
        receipt_actions: vec![],
        withdrawal_actions: vec![],
        signers: vec!["a".into()],
        nonce: 0,
        chain_path: vec!["Nexus".into()],
    };
    (
        Transaction::new(BTreeMap::new(), Header::new(body).unwrap()),
        spec,
    )
}
#[test]
fn pool_journal_failure_withholds_reply_and_retry_restores_durable_content() {
    let (tx, spec) = input();
    let cid = content_id(&tx).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(NodeStorage::open(dir.path(), &cid, "held").unwrap());
    let mut core = DurableMiningCore::new(
        MiningState::new("tip".into(), Some(spec), MiningConfig::default()),
        storage.clone(),
    );
    let effects = core
        .step(
            MiningEvent::TransactionReceived(tx, TransactionOrigin::Local(77)),
            2000,
        )
        .unwrap();
    let job = effects
        .into_iter()
        .find_map(|e| {
            if let MiningEffect::Preflight(j) = e {
                Some(j)
            } else {
                None
            }
        })
        .unwrap();
    let ready = MiningEvent::Preflighted(
        job.clone(),
        MempoolDisposition::Ready,
        BTreeMap::from([("a".into(), 100)]),
    );
    let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    db.execute_batch("CREATE TRIGGER pool_failure BEFORE INSERT ON local_mempool_transactions BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(core.step(ready.clone(), 2345).is_err());
    assert_eq!(core.state().mempool().count(), 0);
    assert!(core.state().is_pending(&cid));
    assert!(
        storage
            .journal()
            .local_mempool_transactions()
            .unwrap()
            .is_empty()
    );
    assert!(storage.live_mempool_roots().unwrap().contains(&cid));
    db.execute_batch("DROP TRIGGER pool_failure").unwrap();
    let effects = core.step(ready, 2345).unwrap();
    assert!(matches!(
        &effects[0],
        MiningEffect::TransactionAdmitted { reply_id: 77, .. }
    ));
    assert_eq!(core.state().mempool().count(), 1);
    let restored = storage.restored_mempool_items().unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].added_at, 2000);
    assert_eq!(content_id(&restored[0].transaction).unwrap(), cid);
    assert!(restored[0].transaction.body.node.is_some());
    db.execute_batch("CREATE TRIGGER pool_failure BEFORE DELETE ON local_mempool_transactions BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    let refuse = MiningEvent::PolicyRefused {
        tip_cid: "tip".into(),
        cids: vec![cid.clone()],
    };
    assert!(core.step(refuse.clone(), 3000).is_err());
    assert_eq!(core.state().mempool().count(), 1);
    assert!(storage.live_mempool_roots().unwrap().contains(&cid));
    db.execute_batch("DROP TRIGGER pool_failure").unwrap();
    core.step(refuse, 3000).unwrap();
    assert_eq!(core.state().mempool().count(), 0);
    assert!(storage.live_mempool_roots().unwrap().is_empty());
    assert!(
        storage
            .journal()
            .local_mempool_transactions()
            .unwrap()
            .is_empty()
    );
    assert!(storage.fetch_volume(&cid).is_none());
    assert_eq!(storage.sweep().unwrap(), 0);
}
#[test]
fn pool_rejects_mismatched_content_without_writing_journal() {
    let (tx, _) = input();
    let cid = content_id(&tx).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = NodeStorage::open(dir.path(), &cid, "held").unwrap();
    let item = MempoolItem {
        cid: Volume::new(LatticeState::empty().unwrap())
            .unwrap()
            .0
            .raw_cid,
        transaction: tx,
        added_at: 0,
    };
    assert!(
        storage
            .persist_pool_delta(&PoolDelta {
                added: vec![item.clone()],
                journaled: vec![item],
                removed: vec![]
            })
            .is_err()
    );
    assert!(
        storage
            .journal()
            .local_mempool_transactions()
            .unwrap()
            .is_empty()
    );
    assert!(storage.live_mempool_roots().unwrap().is_empty());
}

#[test]
fn root_executor_uses_authenticated_preflight_and_rejects_aba_completion() {
    use lattice_node::core::{chain::RootChainCore, stream::StreamConfig};
    let (_, mut spec) = input();
    spec.premine = 100;
    let (private, public) = crypto::generate_key_pair();
    let owner = crypto::create_address(&public).unwrap();
    let mut body = TransactionBody {
        account_actions: vec![AccountAction {
            owner: owner.clone(),
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
    let genesis_tx = Transaction::new(BTreeMap::new(), Header::new(body.clone()).unwrap());
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    let source: BTreeMap<_, _> = lattice_cashew::volume::serialize_recursive(&empty, &())
        .unwrap()
        .into_iter()
        .flat_map(|v| v.entries)
        .collect();
    let built = lattice_validation::GenesisTemplate {
        spec: &spec,
        transactions: &[genesis_tx],
        children: &BTreeMap::new(),
        parent_state: None,
        timestamp: 1000,
        target: UInt256([255; 32]),
        nonce: 0,
        version: 1,
    }
    .build(&source)
    .unwrap();
    let header = Volume::new(built.block).unwrap();
    let verified = lattice_validation::validate_root_genesis(&header, &source, i64::MAX).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let storage = Arc::new(NodeStorage::open(tmp.path(), verified.block_cid(), "held").unwrap());
    let chain = RootChainCore::open(
        storage.clone(),
        &verified,
        "log",
        StreamConfig::default(),
        64,
    )
    .unwrap();
    let mut core = DurableMiningCore::new(
        MiningState::new(
            verified.block_cid().into(),
            Some(spec),
            MiningConfig::default(),
        ),
        storage,
    );
    body.account_actions[0].delta = -10;
    body.signers = vec![owner];
    let signature = crypto::sign(&body.signing_preimage().unwrap(), &private).unwrap();
    let tx = Transaction::new(
        BTreeMap::from([(public, signature)]),
        Header::new(body).unwrap(),
    );
    let job = core
        .step(
            MiningEvent::TransactionReceived(tx, TransactionOrigin::Local(7)),
            2000,
        )
        .unwrap()
        .into_iter()
        .find_map(|e| {
            if let MiningEffect::Preflight(j) = e {
                Some(j)
            } else {
                None
            }
        })
        .unwrap();
    let stale = core
        .execute_root_preflight(&job, chain.tree(), 2000)
        .unwrap()
        .unwrap();
    let movement = |tip: String| {
        MiningEvent::TipMoved(TipMove {
            tip_cid: tip,
            confirmed: Default::default(),
            returned: vec![],
            left: vec![],
        })
    };
    core.step(movement("other".into()), 2100).unwrap();
    let jobs = core
        .step(movement(verified.block_cid().into()), 2200)
        .unwrap();
    assert!(
        core.complete_root_preflight(stale, 2200)
            .unwrap()
            .is_empty()
    );
    assert_eq!(core.state().mempool().count(), 0);
    assert!(
        core.execute_root_preflight(&job, chain.tree(), 2200)
            .unwrap()
            .is_none()
    );
    let fresh = jobs
        .into_iter()
        .find_map(|e| {
            if let MiningEffect::Preflight(j) = e {
                Some(j)
            } else {
                None
            }
        })
        .unwrap();
    let completion = core
        .execute_root_preflight(&fresh, chain.tree(), 2200)
        .unwrap()
        .unwrap();
    let effects = core.complete_root_preflight(completion, 2200).unwrap();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, MiningEffect::TransactionAdmitted { reply_id: 7, .. }))
    );
    assert_eq!(core.state().mempool().count(), 1);
}

#[test]
fn restart_rechecks_local_rows_and_drops_volatile_pool_pins() {
    let (tx, spec) = input();
    let cid = content_id(&tx).unwrap();
    let mut other = tx.body.node.as_deref().unwrap().clone();
    other.nonce = 1;
    let volatile = Transaction::new(BTreeMap::new(), Header::new(other).unwrap());
    let volatile_cid = content_id(&volatile).unwrap();
    let dir = tempfile::tempdir().unwrap();
    {
        let storage = Arc::new(NodeStorage::open(dir.path(), &cid, "held").unwrap());
        let mut core = DurableMiningCore::new(
            MiningState::new("tip".into(), Some(spec.clone()), MiningConfig::default()),
            storage.clone(),
        );
        for (transaction, origin) in [
            (tx, TransactionOrigin::Local(1)),
            (volatile, TransactionOrigin::Submitted(2)),
        ] {
            let job = core
                .step(MiningEvent::TransactionReceived(transaction, origin), 2700)
                .unwrap()
                .into_iter()
                .find_map(|e| {
                    if let MiningEffect::Preflight(j) = e {
                        Some(j)
                    } else {
                        None
                    }
                })
                .unwrap();
            core.step(
                MiningEvent::Preflighted(
                    job,
                    MempoolDisposition::Ready,
                    BTreeMap::from([("a".into(), 100)]),
                ),
                2700,
            )
            .unwrap();
        }
        assert_eq!(storage.live_mempool_roots().unwrap().len(), 2);
        assert_eq!(
            storage
                .journal()
                .local_mempool_transactions()
                .unwrap()
                .len(),
            1
        );
    }
    let storage = Arc::new(NodeStorage::open(dir.path(), &cid, "held").unwrap());
    let (mut core, effects) = DurableMiningCore::restore(
        MiningState::new("new-tip".into(), Some(spec), MiningConfig::default()),
        storage.clone(),
        &["Nexus".into()],
        4000,
    )
    .unwrap();
    assert_eq!(core.state().mempool().count(), 0);
    assert_eq!(core.state().pending_admissions(), 1);
    assert_eq!(storage.live_mempool_roots().unwrap(), [cid.clone()].into());
    assert!(
        !storage
            .live_mempool_roots()
            .unwrap()
            .contains(&volatile_cid)
    );
    let job = effects
        .into_iter()
        .find_map(|e| {
            if let MiningEffect::Preflight(j) = e {
                Some(j)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(job.tip_cid, "new-tip");
    let effects = core
        .step(
            MiningEvent::Preflighted(
                job,
                MempoolDisposition::Ready,
                BTreeMap::from([("a".into(), 100)]),
            ),
            4000,
        )
        .unwrap();
    assert!(effects.is_empty());
    assert_eq!(core.state().mempool().item(&cid).unwrap().added_at, 2000);
    assert!(core.state().journaled().contains(&cid));
}
