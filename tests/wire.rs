use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_node::core::wire;
#[test]
fn pinned_swift_wire_corpus() {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/swift-wire.json")).unwrap();
    let mut mismatches = Vec::new();
    for row in &rows {
        let payload = STANDARD.decode(row["payload"].as_str().unwrap()).unwrap();
        let result = wire::decode(row["topic"].as_str().unwrap(), &payload);
        let outcome = match &result {
            Ok(None) => "unknown",
            Ok(Some(_)) => "valid",
            Err(wire::WireError::Oversized) => "oversized",
            Err(wire::WireError::Malformed) => "malformed",
            Err(wire::WireError::NonCanonical) => "noncanonical",
        };
        if outcome != row["outcome"].as_str().unwrap() {
            mismatches.push(format!(
                "{}: Rust {outcome}, Swift {}",
                row["name"], row["outcome"]
            ));
            continue;
        }
        if let Ok(Some((path, message))) = result {
            assert_eq!(
                wire::encode(&message, &path).unwrap().1,
                STANDARD.decode(row["encoded"].as_str().unwrap()).unwrap(),
                "{}",
                row["name"]
            );
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(rows.len() > 100);
}

#[test]
fn frame_size_is_checked_before_json_and_unknown_topics_are_not_consumed() {
    let huge = vec![b' '; wire::MAX_MESSAGE_BYTES + 1];
    assert_eq!(
        wire::decode(wire::STREAM_REQUEST, &huge).unwrap_err(),
        wire::WireError::Oversized
    );
    assert!(wire::decode("other", &huge).unwrap().is_none());
    assert_eq!(
        wire::decode(wire::STREAM_REQUEST, &huge[..wire::MAX_MESSAGE_BYTES]).unwrap_err(),
        wire::WireError::Malformed
    );
}
