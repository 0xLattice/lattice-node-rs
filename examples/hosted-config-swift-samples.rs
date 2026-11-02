#[path = "../tests/hosted-config-swift-support/mod.rs"]
mod support;
use lattice_primitives::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn main() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let source: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(corpus[0]["entries"].clone()).unwrap();
    let prev: Block =
        lattice_cashew::cbor::decode(&source[corpus[0]["cid"].as_str().unwrap()]).unwrap();
    let base: ChainSpec = lattice_cashew::cbor::decode(&source[&prev.spec.0.raw_cid]).unwrap();
    let address = crypto::create_address(support::PUBLIC).unwrap();
    let path = vec!["Nexus".to_owned(), "child".to_owned()];
    let mut rows = vec![];
    for name in [
        "empty",
        "premine",
        "no-recipient",
        "zero-premine",
        "bad-recipient",
        "overflow",
        "explicit-signed",
        "unsigned",
        "wrong-path",
        "bad-signature",
        "wrong-signers",
        "duplicate-signers",
        "unresolved-body",
        "coin-action",
        "deposit-action",
        "receipt-action",
        "withdrawal-action",
        "count-limit",
        "accept-policy",
        "reject-policy",
        "wrong-module",
        "missing-module",
        "action-scope-ignored",
        "no-bodies-module-required",
        "no-bodies-invalid-module",
        "bad-abi",
        "bad-abi-no-bodies",
        "premine-policy-reject",
        "duplicate-modules",
    ] {
        let mut spec = base.clone();
        spec.premine = 1;
        spec.initial_reward = 100;
        spec.halving_interval = 1000;
        spec.max_number_of_transactions_per_block = 10;
        spec.wasm_policies.clear();
        let mut recipient = None;
        let mut txs = vec![];
        let mut modules = vec![];
        if [
            "premine",
            "bad-recipient",
            "overflow",
            "zero-premine",
            "count-limit",
            "premine-policy-reject",
        ]
        .contains(&name)
        {
            recipient = Some(address.clone());
        }
        if name == "bad-recipient" {
            recipient = Some("not-an-address".into());
        }
        if name == "zero-premine" {
            spec.premine = 0;
            recipient = Some("not-an-address".into());
        }
        if name == "overflow" {
            spec.initial_reward = u64::MAX;
        }
        if name == "count-limit" {
            spec.max_number_of_transactions_per_block = 1;
        }
        let needs_tx = ![
            "empty",
            "premine",
            "no-recipient",
            "zero-premine",
            "bad-recipient",
            "overflow",
            "no-bodies-module-required",
            "no-bodies-invalid-module",
            "bad-abi-no-bodies",
            "premine-policy-reject",
        ]
        .contains(&name);
        if needs_tx {
            let mut body = TransactionBody {
                account_actions: vec![],
                actions: vec![Action {
                    key: "configured".into(),
                    old_value: None,
                    new_value: Some("yes".into()),
                }],
                deposit_actions: vec![],
                receipt_actions: vec![],
                withdrawal_actions: vec![],
                signers: vec![address.clone()],
                nonce: 0,
                chain_path: path.clone(),
            };
            if name == "wrong-path" {
                body.chain_path = vec!["Other".into()];
            }
            if name == "wrong-signers" {
                body.signers = vec![];
            }
            if name == "duplicate-signers" {
                body.signers.push(address.clone());
            }
            if name == "coin-action" {
                body.account_actions.push(AccountAction {
                    owner: address.clone(),
                    delta: 1,
                });
            }
            match name {
                "deposit-action" => body.deposit_actions.push(DepositAction {
                    nonce: 0,
                    demander: address.clone(),
                    amount_demanded: 1,
                    amount_deposited: 1,
                }),
                "receipt-action" => body.receipt_actions.push(ReceiptAction {
                    withdrawer: address.clone(),
                    nonce: 0,
                    demander: address.clone(),
                    amount_demanded: 1,
                    directory: "child".into(),
                }),
                "withdrawal-action" => body.withdrawal_actions.push(WithdrawalAction {
                    withdrawer: address.clone(),
                    nonce: 0,
                    demander: address.clone(),
                    amount_demanded: 1,
                    amount_withdrawn: 1,
                }),
                _ => {}
            }
            let mut tx = support::signed(body);
            if name == "unsigned" {
                tx.signatures.clear();
            }
            if name == "bad-signature" {
                tx.signatures
                    .insert(support::PUBLIC.into(), "00".repeat(64));
            }
            txs.push(tx);
        }
        if name.contains("policy")
            || name.contains("module")
            || name.contains("abi")
            || name == "action-scope-ignored"
        {
            let reject = name == "reject-policy" || name == "premine-policy-reject";
            let bytes = if name == "no-bodies-invalid-module" {
                vec![0]
            } else {
                support::module(u8::from(!reject))
            };
            let cid = content_id(&WasmPolicyModule {
                bytes: bytes.clone(),
            })
            .unwrap();
            spec.wasm_policies.push(WasmPolicyRef {
                module_cid: cid,
                source_cid: None,
                abi_version: if name.contains("bad-abi") { 2 } else { 1 },
                scope: if name == "action-scope-ignored" {
                    PolicyScope::Action
                } else {
                    PolicyScope::Transaction
                },
                entrypoint: "policy".into(),
            });
            if ![
                "missing-module",
                "no-bodies-module-required",
                "action-scope-ignored",
            ]
            .contains(&name)
            {
                modules.push(if name == "wrong-module" {
                    support::module(0)
                } else {
                    bytes
                });
            }
            if name == "duplicate-modules" {
                modules.push(modules[0].clone());
            }
        }
        let entries: BTreeMap<String, Vec<u8>> = txs
            .iter()
            .map(|tx| (tx.body.raw_cid.clone(), tx.body.bytes(None).unwrap()))
            .collect();
        let mut row = json!({"name":name,"spec":spec,"path":path,"recipient":recipient,"entries":entries,"transactions":txs.iter().map(|t|canonical_bytes(t).unwrap()).collect::<Vec<_>>(),"materialized":name!="unresolved-body","modules":modules});
        row["expected"] = support::observe(&row);
        rows.push(row);
    }
    println!("{}", serde_json::to_string(&rows).unwrap());
}
