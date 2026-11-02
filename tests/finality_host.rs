use lattice_cashew::{Header, Volume};
use lattice_node::runtime::host::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
#[tokio::test]
async fn finality_reads_survive_host_reconfiguration_and_restart() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let entries: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(fixture[0]["entries"].clone()).unwrap();
    let cid = fixture[0]["cid"].as_str().unwrap().to_owned();
    let verified = lattice_validation::validate_root_genesis(
        &Volume(Header::from_cid(&cid).unwrap()),
        &entries,
        i64::MAX,
    )
    .unwrap();
    let volumes = lattice_cashew::volume::serialize_recursive(verified.header(), &()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    for restart in [false, true] {
        let host = start_with_hosted_finality_configuration(
            HostConfig {
                directory: directory.path().into(),
                genesis_cid: cid.clone(),
                bootstrap_volumes: if restart { vec![] } else { volumes.clone() },
                signing_key: ed25519_dalek::SigningKey::from_bytes(&[71; 32]),
                listen: "127.0.0.1:0".parse().unwrap(),
                min_relay_fee: 1,
                seeds: vec![],
            },
            Default::default(),
            Default::default(),
            Default::default(),
            json!({"hostChain":["Nexus/A","Nexus/B","Nexus/B/C"]}),
            FinalityWindows::new(
                Some(7),
                BTreeMap::from([("Nexus/A".into(), 0), ("Nexus/B".into(), 2)]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        for (path, expected) in [
            ("Nexus", Some("7")),
            ("Nexus/A", None),
            ("Nexus/B", Some("2")),
            ("Nexus/B/C", Some("7")),
        ] {
            let (status, value) = host
                .handle
                .request(&format!("/api/chain/info?chainPath={path}"), Value::Null)
                .await
                .unwrap();
            assert_eq!(status, 200, "{value}");
            assert_eq!(
                value.get("finalityWindow").and_then(|v| v.as_str()),
                expected,
                "{value}"
            );
            assert_eq!(value["chain"], json!(path.split('/').collect::<Vec<_>>()));
        }
        for paths in [
            vec!["Nexus/B"],
            vec!["Nexus/A", "Nexus/B", "Nexus/B/C", "Nexus/D"],
        ] {
            assert_eq!(
                host.handle
                    .request("POST /hosted-chains", json!({"hostChain":paths}))
                    .await
                    .unwrap()
                    .0,
                200
            );
        }
        let (_, disabled) = host
            .handle
            .request("/api/chain/info?chainPath=Nexus/A", Value::Null)
            .await
            .unwrap();
        assert!(disabled.get("finalityWindow").is_none());
        let (_, inherited) = host
            .handle
            .request("/api/chain/info?chainPath=Nexus/D", Value::Null)
            .await
            .unwrap();
        assert_eq!(inherited["finalityWindow"], "7");
        host.handle.shutdown().await;
        host.task.await.unwrap().unwrap();
    }
}
