#[allow(dead_code)]
#[path = "hosted-config-swift-support/mod.rs"]
mod support;
use serde_json::Value;
#[test]
fn actual_swift_hosted_premine_precheck() {
    let rows: Vec<Value> = serde_json::from_str(include_str!("hosted-config-swift.json")).unwrap();
    assert_eq!(rows.len(), 29);
    for row in &rows {
        assert_eq!(support::observe(row), row["swift"], "{}", row["name"]);
    }
    let find = |name: &str| &rows.iter().find(|r| r["name"] == name).unwrap()["swift"];
    for name in [
        "empty",
        "premine",
        "no-recipient",
        "zero-premine",
        "explicit-signed",
        "duplicate-signers",
        "accept-policy",
        "action-scope-ignored",
        "no-bodies-invalid-module",
        "bad-abi-no-bodies",
        "duplicate-modules",
    ] {
        assert_eq!(find(name)["accepted"], true, "positive {name}");
    }
    for name in [
        "unsigned",
        "wrong-path",
        "bad-signature",
        "wrong-signers",
        "unresolved-body",
        "coin-action",
        "deposit-action",
        "receipt-action",
        "withdrawal-action",
        "count-limit",
        "reject-policy",
        "wrong-module",
        "missing-module",
        "no-bodies-module-required",
        "bad-abi",
        "premine-policy-reject",
        "overflow",
        "bad-recipient",
    ] {
        assert_eq!(find(name)["accepted"], false, "negative {name}");
    }
    assert_eq!(find("premine")["premine"].as_array().unwrap().len(), 1);
    assert!(
        find("zero-premine")["premine"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
