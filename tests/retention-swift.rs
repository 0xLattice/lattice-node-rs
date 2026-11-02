#[allow(dead_code)]
#[path = "retention-swift-support/mod.rs"]
mod support;
use serde_json::Value;
#[test]
fn actual_swift_tip_state_walk() {
    let rows: Vec<Value> = serde_json::from_str(include_str!("retention-swift.json")).unwrap();
    assert_eq!(rows.len(), 19);
    for row in &rows {
        assert_eq!(support::observe(row), row["swift"], "{}", row["name"]);
    }
    let find = |name: &str| rows.iter().find(|r| r["name"] == name).unwrap();
    let initial = &find("empty-four-references")["swift"];
    assert_eq!(initial["entering"].as_array().unwrap().len(), 2);
    let mut counts: Vec<_> = initial["changes"]
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_i64().unwrap())
        .collect();
    counts.sort();
    assert_eq!(counts, vec![1, 4]);
    for name in ["noop-empty", "noop-split"] {
        let row = find(name);
        assert!(row["swift"]["entering"].as_array().unwrap().is_empty());
        let changes = row["swift"]["changes"].as_object().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[row["new"].as_str().unwrap()], 0);
    }
    assert!(
        find("new-root-missing")["swift"]["entering"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        find("shared-empty-missing")["swift"]["changes"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    for name in ["split", "collapse", "replace-value", "clear"] {
        let row = find(name);
        let changes = row["swift"]["changes"].as_object().unwrap();
        assert_eq!(changes[row["new"].as_str().unwrap()], 1);
        assert_eq!(changes[row["old"].as_str().unwrap()], -1);
    }
    let row = find("held-root");
    assert_eq!(row["swift"]["changes"][row["old"].as_str().unwrap()], -1);
    assert!(
        !find("held-branch")["swift"]["changes"]
            .as_object()
            .unwrap()
            .is_empty()
    );
}
