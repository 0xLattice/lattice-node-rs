use lattice_cashew::{Header, MerkleDictionary, Volume, VolumeMerkleDictionary};
use lattice_primitives::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn main() {
    let original: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let entries: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(original[0]["entries"].clone()).unwrap();
    let base: Block =
        lattice_cashew::cbor::decode(&entries[original[0]["cid"].as_str().unwrap()]).unwrap();
    let spec: ChainSpec = lattice_cashew::cbor::decode(&entries[&base.spec.0.raw_cid]).unwrap();
    let mut rows = Vec::new();
    for name in [
        "genesis",
        "bare-block",
        "account-and-nonce",
        "general",
        "deposit",
        "withdrawal",
        "receipt",
        "combined",
        "spec-is-not-block",
        "transaction-is-not-block",
        "missing-spec",
        "missing-state",
        "oversize",
        "policy",
        "missing-policy",
        "unresolved-child-block",
    ] {
        let mut block = base.clone();
        block.children = Header::new(BTreeMap::new()).unwrap();
        block.height = if name == "genesis" { 0 } else { 1 };
        block.parent = Some(Volume(Header::from_cid(&base.spec.0.raw_cid).unwrap())); // must never resolve a parent
        let mut spec = spec.clone();
        spec.max_block_size = if name == "oversize" { 1 } else { 1_000_000 };
        let module = Volume::new(WasmPolicyModule {
            bytes: vec![0, 97, 115, 109, 1, 0, 0, 0],
        })
        .unwrap();
        if name == "policy" || name == "missing-policy" {
            spec.wasm_policies.push(WasmPolicyRef {
                module_cid: module.0.raw_cid.clone(),
                source_cid: None,
                abi_version: 1,
                scope: PolicyScope::Transaction,
                entrypoint: "lattice_validate_transaction".into(),
            });
        }
        block.spec = Volume::new(spec).unwrap();
        if name == "unresolved-child-block" {
            block.children = Header::new(BTreeMap::from([(
                "kid".into(),
                Volume(Header::from_cid(&module.0.raw_cid).unwrap()),
            )]))
            .unwrap();
        }
        let receipt = ReceiptKey {
            directory: "Child".into(),
            demander: "demander".into(),
            amount_demanded: 7,
            nonce: 3,
        }
        .storage_key();
        let mut state = LatticeState::empty().unwrap();
        state.account_state = Volume::new(
            VolumeMerkleDictionary::default()
                .inserting("owner", 100)
                .unwrap()
                .inserting("_nonce_signer", 2)
                .unwrap()
                .inserting("unread", 999)
                .unwrap(),
        )
        .unwrap();
        state.general_state = Volume::new(
            VolumeMerkleDictionary::default()
                .inserting("key", "old".to_string())
                .unwrap()
                .inserting("unread", "private".into())
                .unwrap(),
        )
        .unwrap();
        state.deposit_state = Volume::new(
            VolumeMerkleDictionary::default()
                .inserting("demander/7/3", 9)
                .unwrap(),
        )
        .unwrap();
        state.receipt_state = Volume::new(
            VolumeMerkleDictionary::default()
                .inserting(&receipt, "withdrawer".to_string())
                .unwrap(),
        )
        .unwrap();
        block.prev_state = Volume::new(state.clone()).unwrap();
        block.parent_state = Volume::new(state).unwrap();
        let mut post = LatticeState::empty().unwrap();
        post.account_state = Volume::new(
            VolumeMerkleDictionary::default()
                .inserting("post-only", 55)
                .unwrap(),
        )
        .unwrap();
        block.post_state = Volume::new(post).unwrap();
        let mut body = TransactionBody {
            account_actions: vec![],
            actions: vec![],
            deposit_actions: vec![],
            receipt_actions: vec![],
            withdrawal_actions: vec![],
            signers: vec![],
            nonce: 0,
            chain_path: vec!["Nexus".into(), "Child".into()],
        };
        if name == "account-and-nonce" || name == "combined" {
            body.account_actions.push(AccountAction {
                owner: "owner".into(),
                delta: -1,
            });
            body.signers.push("signer".into());
            block.reward_recipient = Some("recipient".into());
        }
        if name == "general" || name == "combined" {
            body.actions.push(Action {
                key: "key".into(),
                old_value: Some("old".into()),
                new_value: Some("new".into()),
            });
        }
        if name == "deposit" || name == "combined" {
            body.deposit_actions.push(DepositAction {
                demander: "demander".into(),
                amount_demanded: 7,
                nonce: 3,
                amount_deposited: 1,
            });
        }
        if name == "withdrawal" || name == "combined" {
            body.withdrawal_actions.push(WithdrawalAction {
                withdrawer: "withdrawer".into(),
                demander: "demander".into(),
                amount_demanded: 7,
                nonce: 3,
                amount_withdrawn: 1,
            });
        }
        if name == "receipt" || name == "combined" {
            body.receipt_actions.push(ReceiptAction {
                withdrawer: "withdrawer".into(),
                directory: "Child".into(),
                demander: "demander".into(),
                amount_demanded: 7,
                nonce: 3,
            });
        }
        let tx = Volume::new(Transaction {
            body: Header::new(body).unwrap(),
            signatures: BTreeMap::new(),
        })
        .unwrap();
        let tx_cid = tx.0.raw_cid.clone();
        block.transactions = Header::new(if name == "bare-block" || name == "genesis" {
            MerkleDictionary::default()
        } else {
            MerkleDictionary::default().inserting("0", tx).unwrap()
        })
        .unwrap();
        let spec_cid = block.spec.0.raw_cid.clone();
        let state_cid = block.prev_state.0.raw_cid.clone();
        let header = Volume::new(block).unwrap();
        let mut volumes = lattice_cashew::volume::serialize_recursive(&header, &()).unwrap();
        if name == "policy" {
            volumes.extend(lattice_cashew::volume::serialize_recursive(&module, &()).unwrap());
        }
        if name == "missing-spec" {
            volumes.retain(|v| v.root != spec_cid);
        }
        if name == "missing-state" {
            volumes.retain(|v| v.root != state_cid);
        }
        let root = match name {
            "spec-is-not-block" => spec_cid,
            "transaction-is-not-block" => tx_cid,
            _ => header.0.raw_cid,
        };
        let entries: BTreeMap<String, Vec<u8>> =
            volumes.iter().flat_map(|v| v.entries.clone()).collect();
        let dir = tempfile::tempdir().unwrap();
        let store =
            lattice_node::storage::NodeStorage::open(dir.path(), "fixture", "test").unwrap();
        store.cache_volumes("fixture", &volumes).unwrap();
        rows.push(json!({"name":name,"root":root,"entries":entries,"volumes":volumes,"expected":store.serving_bundle(&root).unwrap()}));
    }
    println!("{}", serde_json::to_string(&rows).unwrap());
}
