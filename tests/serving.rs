use lattice_node::runtime::serving::*;
#[test]
fn credit_is_written_before_visibility_and_serving_deltas_survive_save() {
    let dir = tempfile::Builder::new()
        .prefix("ledger-λ-信用-")
        .tempdir()
        .unwrap();
    let key = hex::encode(
        ed25519_dalek::SigningKey::from_bytes(&[9; 32])
            .verifying_key()
            .as_bytes(),
    );
    let mut ledger = ServingLedger::open(dir.path()).unwrap();
    assert_eq!(ledger.credit(&key, 100).unwrap(), 100);
    assert_eq!(
        ServingLedger::open(dir.path())
            .unwrap()
            .standing(&key)
            .unwrap(),
        100
    );
    assert!(ledger.credit(&key, 0).is_err());
    assert!(ledger.credit(&key.to_uppercase(), 10).is_err());
    ledger
        .tally()
        .debit_standing(&lattice_tally::PeerId::new(&key), 40);
    ledger.save().unwrap();
    assert_eq!(
        ServingLedger::open(dir.path())
            .unwrap()
            .standing(&key)
            .unwrap(),
        60
    );
    assert_eq!(ledger.credit(&key, i64::MAX).unwrap(), i64::MAX);
    let file = dir.path().join("serving.json");
    std::fs::remove_file(&file).unwrap();
    std::fs::create_dir(&file).unwrap();
    let other = hex::encode(
        ed25519_dalek::SigningKey::from_bytes(&[10; 32])
            .verifying_key()
            .as_bytes(),
    );
    assert!(matches!(
        ledger.credit(&other, 99),
        Err(ServingError::NotSaved)
    ));
    assert_eq!(ledger.standing(&other).unwrap(), 0);
    assert!(ServingLedger::open(dir.path()).is_err());
}
#[test]
fn malformed_or_unknown_version_saved_ledger_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    for saved in [
        "not json",
        r#"{"version":2,"balances":{}}"#,
        r#"{"version":1,"balances":{"bad":4}}"#,
    ] {
        std::fs::write(dir.path().join("serving.json"), saved).unwrap();
        assert!(ServingLedger::open(dir.path()).is_err());
    }
}
