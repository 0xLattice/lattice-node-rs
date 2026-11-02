use lattice_node::core::read_endpoints::*;
#[test]
fn actual_swift_url_verdicts() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("read-endpoint-url-swift.json")).unwrap();
    let mismatches = vectors["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| valid_url(row["url"].as_str().unwrap()) != row["valid"].as_bool().unwrap())
        .collect::<Vec<_>>();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}
#[test]
fn canonical_wire_roundtrips_and_refuses_unknown_or_noncanonical_fields() {
    assert!(valid_path(&["Nexus".into()]));
    let request = ReadEndpointRequest {
        chain_path: vec!["Nexus".into(), "Alpha".into()],
        request_id: 7,
    };
    let bytes = encode(&request).unwrap();
    assert_eq!(bytes, br#"{"chainPath":["Nexus","Alpha"],"requestID":7}"#);
    assert!(decode::<ReadEndpointRequest>(&bytes).is_some());
    for bytes in [
        br#"{"chainPath":["Nexus","Alpha"],"requestID":7,"extra":1}"#.as_slice(),
        br#"{ "chainPath":["Nexus","Alpha"],"requestID":7}"#,
        br#"{"chainPath":["Nexus","Alpha"],"requestID":7.0}"#,
    ] {
        assert!(decode::<ReadEndpointRequest>(bytes).is_none());
    }
    assert_eq!(
        rendezvous("genesis", &request.chain_path),
        "lattice.read-endpoint.v1:genesis:Nexus/Alpha"
    );
}
