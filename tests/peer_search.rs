use lattice_node::core::peer_search::*;
use serde_json::{Value, json};
#[test]
fn unchanged_swift_peer_search_stateful_corpus() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/swift-peer-search.json")).unwrap();
    let mut search = PeerSearch::default();
    for (i, row) in fixture["rows"].as_array().unwrap().iter().enumerate() {
        let heights = row["heights"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| {
                (
                    serde_json::from_value(h["path"].clone()).unwrap(),
                    h["height"].as_u64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let paths = serde_json::from_value::<Vec<Vec<String>>>(row["paths"].clone()).unwrap();
        let (now, interval, connected) = (
            row["now"].as_i64().unwrap(),
            row["interval"].as_i64().unwrap(),
            row["connected"].as_bool().unwrap(),
        );
        let due = search.due(&heights, now, interval, connected);
        let top = search.top_ups(&paths, now, interval, connected);
        assert_eq!(
            json!({"due":due,"topUps":top}),
            fixture["expected"][i],
            "observation {i}"
        );
    }
    assert_ne!(
        rendezvous("network-a", &["Nexus".into(), "Alpha".into()]),
        rendezvous("network-b", &["Nexus".into(), "Alpha".into()])
    );
}
