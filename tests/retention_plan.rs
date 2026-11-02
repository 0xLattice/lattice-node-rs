use lattice_cashew::Volume;
use lattice_node::storage::{RetentionScopes, plan_tip_state};
use lattice_primitives::LatticeState;
use std::collections::BTreeMap;

#[test]
fn state_walk_counts_shared_edges_and_only_expands_zero_crossings() {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    let source = BTreeMap::<String, Vec<u8>>::new();
    let first = plan_tip_state(&empty, None, &source, |_| Ok(0)).unwrap();
    let empty_dictionary = empty.node.as_ref().unwrap().account_state.raw_cid.clone();
    assert_eq!(first.changes[&empty.raw_cid], 1);
    assert_eq!(first.changes[&empty_dictionary], 4);
    assert_eq!(first.entering.len(), 2);
    let counts = first.changes;
    let same = plan_tip_state(&empty, Some(&empty), &source, |cid| {
        Ok(*counts.get(cid).unwrap_or(&0))
    })
    .unwrap();
    assert!(same.entering.is_empty());
    assert_eq!(same.changes, BTreeMap::from([(empty.raw_cid.clone(), 0)]));

    let mut state = LatticeState::empty().unwrap();
    let accounts = state
        .account_state
        .node
        .as_ref()
        .unwrap()
        .inserting("balance", 7)
        .unwrap();
    state.account_state = Volume::new(accounts).unwrap();
    let new = Volume::new(state).unwrap();
    let moved = plan_tip_state(&new, Some(&empty), &source, |cid| {
        Ok(*counts.get(cid).unwrap_or(&0))
    })
    .unwrap();
    assert_eq!(moved.changes[&empty.raw_cid], -1);
    assert_eq!(moved.changes[&new.raw_cid], 1);
    assert_eq!(moved.changes[&empty_dictionary], -1);
    assert!(!moved.entering.contains(&empty_dictionary));
    assert!(moved.entering.contains(&new.raw_cid));
}

#[test]
fn missing_local_nodes_skip_and_overflow_does_not_produce_a_plan() {
    let state = Volume::new(LatticeState::empty().unwrap()).unwrap();
    let unresolved = state.removing_node();
    let source = BTreeMap::<String, Vec<u8>>::new();
    let missing = plan_tip_state(&unresolved, None, &source, |_| Ok(0)).unwrap();
    assert!(missing.changes.is_empty());
    assert!(missing.entering.is_empty());
    assert!(plan_tip_state(&state, None, &source, |_| Ok(i64::MAX)).is_err());
    assert!(plan_tip_state(&state, None, &source, |_| Ok(-1)).is_err());
    // A missing old node can release its own counted root, but cannot guess
    // which descendants it named; those pins stay until reconciliation.
    let new = Volume::new(LatticeState::empty().unwrap()).unwrap();
    let old =
        Volume::<LatticeState>::from_cid(lattice_cashew::cid_for_bytes(0x71, b"missing")).unwrap();
    let plan = plan_tip_state(&new, Some(&old), &source, |cid| {
        Ok(i64::from(cid == old.raw_cid))
    })
    .unwrap();
    assert_eq!(plan.changes[&old.raw_cid], -1);
}

#[test]
fn scope_names_round_trip_chain_paths_and_reject_other_namespaces() {
    let scopes = RetentionScopes {
        chain: "chain:Nexus".into(),
    };
    let path = vec!["Nexus".into(), "child".into()];
    let body = scopes.body("cid", &path);
    assert_eq!(body, "chain:Nexus:body:Nexus/child:cid");
    assert_eq!(
        scopes.block(&body, false),
        Some((path.clone(), "cid".into()))
    );
    assert_eq!(
        scopes.block(&scopes.reads("cid", &path), true),
        Some((path, "cid".into()))
    );
    assert!(scopes.block("other:body:Nexus:cid", false).is_none());
}
