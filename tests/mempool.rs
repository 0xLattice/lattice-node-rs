use lattice_cashew::Header;
use lattice_node::core::mempool::*;
use lattice_primitives::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
#[test]
fn actual_swift_pool_selection_replacement_eviction_traces() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/swift-mempool.json")).unwrap();
    let mut count = 0;
    for case in cases {
        let input = &case["scenario"];
        let spec: ChainSpec = serde_json::from_value(input["spec"].clone()).unwrap();
        let limits = &input["limits"];
        let mut pool = Mempool::new(MempoolLimits {
            max_count: limits["count"].as_u64().unwrap() as usize,
            max_bytes: limits["bytes"].as_u64().unwrap() as usize,
            max_signatures: limits["signatures"].as_u64().unwrap() as usize,
            min_relay_fee: limits["floor"].as_u64().unwrap(),
        });
        let mut submitted = Vec::new();
        for (i, op) in input["operations"].as_array().unwrap().iter().enumerate() {
            count += 1;
            let balances: BTreeMap<String, u64> =
                serde_json::from_value(op.get("balances").cloned().unwrap_or(json!({}))).unwrap();
            let result = match op["op"].as_str().unwrap() {
                "submit" => {
                    let body: TransactionBody = serde_json::from_value(op["body"].clone()).unwrap();
                    let tx = Transaction::new(BTreeMap::new(), Header::new(body).unwrap());
                    submitted.push(content_id(&tx).unwrap());
                    pool.submit(&tx, &spec, op["at"].as_i64().unwrap_or(0), &balances)
                }
                "rebalance" => Ok(pool.rebalance(&balances)),
                "clear" => Ok(pool.clear()),
                "remove" => Ok(pool.remove(
                    op["remove"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(|v| submitted.get(v.as_u64().unwrap() as usize).cloned())
                        .collect::<Vec<_>>(),
                )),
                _ => panic!(),
            };
            let (result, mutation) = match result {
                Ok(m) => ("ok".to_owned(), m),
                Err(e) => {
                    let raw = format!("{e:?}");
                    (
                        format!("{}{}", raw[..1].to_lowercase(), &raw[1..]),
                        MempoolMutation::default(),
                    )
                }
            };
            let observed = json!({"result":result,"items":pool.items().iter().map(|i|&i.cid).collect::<Vec<_>>(),"bytes":pool.byte_count(),"version":pool.version(),"next":pool.next_block(&spec).iter().map(|tx|content_id(tx).unwrap()).collect::<Vec<_>>(),"projected":pool.projected_blocks(4,&spec).iter().map(|p|json!({"count":p.transaction_count,"bytes":p.bytes,"fee":p.lowest_fee.to_hex(),"weight":p.lowest_fee_weight,"feeBytes":p.lowest_fee_bytes,"full":p.full})).collect::<Vec<_>>(),"replaced":mutation.replaced.iter().map(|i|&i.cid).collect::<Vec<_>>(),"evicted":mutation.evicted.iter().map(|i|&i.cid).collect::<Vec<_>>(),"removed":mutation.removed.iter().map(|i|&i.cid).collect::<Vec<_>>()});
            assert_eq!(
                observed, case["observations"][i],
                "{} operation {i}",
                input["name"]
            );
        }
    }
    assert_eq!(count, 720);
}

#[test]
fn debited_balances_authenticate_sparse_content_and_ignore_unrelated_accounts() {
    use lattice_cashew::{Volume, volume::serialize_recursive};
    let body = TransactionBody {
        account_actions: vec![
            AccountAction {
                owner: "alice".into(),
                delta: -4,
            },
            AccountAction {
                owner: "alice".into(),
                delta: 1,
            },
        ],
        actions: vec![],
        deposit_actions: vec![],
        receipt_actions: vec![ReceiptAction {
            withdrawer: "bob".into(),
            demander: "alice".into(),
            amount_demanded: 1,
            nonce: 0,
            directory: "child".into(),
        }],
        withdrawal_actions: vec![],
        signers: vec!["alice".into()],
        nonce: 0,
        chain_path: vec!["Nexus".into()],
    };
    assert_eq!(
        Mempool::debits(&body).unwrap(),
        BTreeMap::from([("alice".into(), 2), ("bob".into(), 1)])
    );
    let mut state = LatticeState::empty().unwrap();
    state.account_state = Volume::new(
        AccountState::default()
            .inserting("alice", 20)
            .unwrap()
            .inserting("unrelated", 99)
            .unwrap(),
    )
    .unwrap();
    let full = Volume::new(state.clone()).unwrap();
    let content: BTreeMap<String, Vec<u8>> = serialize_recursive(&full, &())
        .unwrap()
        .into_iter()
        .flat_map(|v| v.entries)
        .collect();
    let sparse: LatticeState =
        lattice_cashew::cbor::decode(&lattice_cashew::cbor::encode(&state).unwrap()).unwrap();
    assert!(Mempool::balances(&body, &sparse, &BTreeMap::new()).is_none());
    assert_eq!(
        Mempool::balances(&body, &sparse, &content).unwrap(),
        BTreeMap::from([("alice".into(), 20), ("bob".into(), 0)])
    );
    state.account_state.0.raw_cid = lattice_cashew::cid_for_bytes(0x71, b"wrong");
    assert!(Mempool::balances(&body, &state, &content).is_none());
}

#[test]
fn exact_fee_weight_full_width_and_subtraction_limits() {
    let weight = FeeWeight {
        block_room: i64::MAX,
        growth_room: i64::MAX,
        max_transactions: 10,
        transaction_allowance: 256,
    };
    assert_eq!(weight.of(100, i64::MAX as usize - 1), i64::MAX as usize - 1);
    assert_eq!(weight.of(100, i64::MAX as usize), i64::MAX as usize);
    assert_eq!(weight.of(100, 0), i64::MAX as usize / 10);
}
