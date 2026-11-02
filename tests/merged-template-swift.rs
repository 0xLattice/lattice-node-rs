#[allow(dead_code)]
#[path = "merged-template-swift-support/mod.rs"]
mod support;
use serde_json::{Value, json};
#[test]
fn actual_swift_local_merged_template_fit_and_thresholds() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("merged-template-swift.json")).unwrap();
    assert_eq!(rows.len(), 24);
    for row in &rows {
        assert_eq!(support::observe(row), row["swift"], "{}", row["name"]);
    }
    let find = |name: &str| &rows.iter().find(|r| r["name"] == name).unwrap()["swift"];
    assert_eq!(find("leaves")["targets"].as_array().unwrap().len(), 4);
    assert_eq!(find("nonleaf")["targets"].as_array().unwrap().len(), 1);
    assert_eq!(
        find("unresolved-descendant")["targets"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        find("hosted-empty")["selected"].as_array().unwrap().len(),
        2
    );
    assert_eq!(find("count-limit")["selected"].as_array().unwrap().len(), 1);
    for (i, directory) in ["a", "b", "c"].iter().enumerate() {
        assert_eq!(find(&format!("rotate{i}"))["children"], json!([directory]));
    }
    assert_eq!(
        find("numeric-order-12")["selected"]
            .as_array()
            .unwrap()
            .len(),
        12
    );
    assert_eq!(find("fit0")["built"], false);
    assert_eq!(find("fit1")["children"], json!([]));
    assert_eq!(find("fit5")["children"], json!(["a", "b", "c"]));
    assert_eq!(find("fit6")["selected"].as_array().unwrap().len(), 1);
    assert_eq!(
        find("leaves")["cid"],
        find("root-work")["cid"],
        "minimum work changes scheduling, not committed block bytes"
    );
}
