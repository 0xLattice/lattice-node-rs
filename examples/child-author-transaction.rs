//! Disposable child-author integration identity; never use this key for funds.
use lattice_primitives::{AccountAction, Action, TransactionBody, crypto};
use serde_json::json;
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let private = "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb";
    let public = "ed013d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c";
    let owner = crypto::create_address(public).unwrap();
    let nonce: u64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(0);
    let previous = args.get(4).cloned();
    let body = TransactionBody {
        account_actions: vec![AccountAction {
            owner: owner.clone(),
            delta: -1,
        }],
        actions: vec![Action {
            key: "child/Alpha".into(),
            old_value: previous,
            new_value: Some(format!("{}@{}", args[1], args[2])),
        }],
        deposit_actions: vec![],
        receipt_actions: vec![],
        withdrawal_actions: vec![],
        signers: vec![owner],
        nonce,
        chain_path: vec!["Nexus".into()],
    };
    let signature = crypto::sign(&body.signing_preimage().unwrap(), private).unwrap();
    let mut wire = serde_json::to_value(body).unwrap();
    wire["nonce"] = json!(nonce.to_string());
    wire["accountActions"][0]["delta"] = json!("-1");
    println!(
        "{}",
        json!({"transaction":{"body":wire,"signatures":{public:signature}}})
    );
}
