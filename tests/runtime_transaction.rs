use lattice_node::runtime::transaction::decode;
use serde_json::json;
#[test]
fn rpc_payload_binds_inline_body_and_preserves_full_width_decimal_integers() {
    let mut payload = json!({"signatures":{},"body":{"accountActions":[{"owner":"x","delta":"-9223372036854775808"}],"actions":[],"depositActions":[{"nonce":"340282366920938463463374607431768211455","demander":"x","amountDemanded":"18446744073709551615","amountDeposited":"0"}],"receiptActions":[],"withdrawalActions":[],"signers":[],"nonce":"18446744073709551615","chainPath":["Nexus"]}});
    assert!(
        decode(payload.clone()).is_err(),
        "Swift DAG-CBOR cannot address action nonces above u64"
    );
    payload["body"]["depositActions"][0]["nonce"] = json!(u64::MAX.to_string());
    let tx = decode(payload.clone()).unwrap();
    let body = tx.body.node.as_deref().unwrap();
    assert_eq!(body.nonce, u64::MAX);
    assert_eq!(body.account_actions[0].delta, i64::MIN);
    assert_eq!(body.deposit_actions[0].nonce, u128::from(u64::MAX));
    assert_eq!(body.deposit_actions[0].amount_demanded, u64::MAX);
    assert_eq!(
        tx.body.raw_cid,
        lattice_primitives::content_id(body).unwrap()
    );
    for bad in [
        json!(0),
        json!("00"),
        json!("+1"),
        json!("-0"),
        json!("18446744073709551616"),
    ] {
        let mut modified = payload.clone();
        modified["body"]["nonce"] = bad;
        assert!(decode(modified).is_err());
    }
    let mut reference_only = payload;
    reference_only["body"] = serde_json::to_value(tx.body.removing_node()).unwrap();
    assert!(decode(reference_only).is_err());
}
