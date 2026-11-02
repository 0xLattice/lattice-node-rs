#[path = "../tests/retention-swift-support/mod.rs"]
mod support;
use lattice_cashew::Volume;
use lattice_primitives::LatticeState;
use serde_json::json;
use std::collections::BTreeMap;
fn state(keys: &[(&str, u64)], general: bool) -> Volume<LatticeState> {
    let mut state = LatticeState::empty().unwrap();
    let mut accounts = state.account_state.node.as_deref().unwrap().clone();
    for (k, v) in keys {
        accounts = accounts.inserting(k, *v).unwrap();
    }
    state.account_state = Volume::new(accounts).unwrap();
    if general {
        state.general_state = Volume::new(
            state
                .general_state
                .node
                .as_deref()
                .unwrap()
                .inserting("setting", "value".into())
                .unwrap(),
        )
        .unwrap();
    }
    Volume::new(state).unwrap()
}
fn main() {
    let empty = state(&[], false);
    let one = state(&[("alice", 1)], false);
    let split = state(&[("alice", 1), ("alina", 2), ("bob", 3)], false);
    let changed = state(&[("alice", 1)], true);
    let replacement = state(&[("alice", 8), ("alina", 2), ("bob", 3)], false);
    let unicode = state(&[("é", 1), ("雪", 2), ("ab", 3)], true);
    let mut output = vec![];
    for (name, new, old) in [
        ("empty-four-references", &empty, None),
        ("initial-split", &split, None),
        ("initial-unicode", &unicode, None),
        ("noop-empty", &empty, Some(&empty)),
        ("noop-split", &split, Some(&split)),
        ("insert-one", &one, Some(&empty)),
        ("split", &split, Some(&one)),
        ("collapse", &one, Some(&split)),
        ("replace-value", &replacement, Some(&split)),
        ("clear", &empty, Some(&split)),
        ("held-branch", &changed, Some(&one)),
        ("held-root", &changed, Some(&one)),
        ("new-root-missing", &changed, Some(&one)),
        ("old-root-missing", &empty, Some(&split)),
        ("new-dictionary-missing", &changed, Some(&one)),
        ("shared-empty-missing", &empty, None),
        ("both-roots-missing", &changed, Some(&one)),
        ("new-root-already-counted-missing", &changed, Some(&one)),
        ("missing-old-no-count", &empty, Some(&split)),
    ] {
        let (mut entries, _) = support::graph(new);
        let mut counts = BTreeMap::new();
        if let Some(old) = old {
            let (bytes, old_counts) = support::graph(old);
            entries.extend(bytes);
            counts = old_counts;
        }
        match name {
            "held-branch" => {
                let cid = &one.node.as_deref().unwrap().account_state.raw_cid;
                *counts.get_mut(cid).unwrap() += 1;
                entries.remove(cid);
            }
            "held-root" => {
                *counts.get_mut(&one.raw_cid).unwrap() += 1;
            }
            "new-root-missing" => {
                entries.remove(&new.raw_cid);
            }
            "old-root-missing" => {
                entries.remove(&old.unwrap().raw_cid);
            }
            "new-dictionary-missing" => {
                entries.remove(&new.node.as_deref().unwrap().general_state.raw_cid);
            }
            "shared-empty-missing" => {
                entries.remove(&empty.node.as_deref().unwrap().account_state.raw_cid);
            }
            "both-roots-missing" => {
                entries.remove(&new.raw_cid);
                entries.remove(&old.unwrap().raw_cid);
            }
            "new-root-already-counted-missing" => {
                counts.insert(new.raw_cid.clone(), 1);
                entries.remove(&new.raw_cid);
            }
            "missing-old-no-count" => {
                counts.clear();
                entries.remove(&old.unwrap().raw_cid);
            }
            _ => {}
        }
        let mut row = json!({"name":name,"new":new.raw_cid,"old":old.map(|h|&h.raw_cid),"entries":entries,"counts":counts});
        row["expected"] = support::observe(&row);
        output.push(row);
    }
    println!("{}", serde_json::to_string(&output).unwrap());
}
