//! Observe production child-anchor decisions for the pinned Hierarchy oracle.
use lattice_block_tree::{BlockImportBatch, ChainTree, ParentAnchorFailure};
use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_primitives::*;
use serde_json::json;
use std::collections::BTreeMap;
fn cid(s: &str) -> String {
    lattice_cashew::cid_for_bytes(0x71, s.as_bytes())
}
fn empty() -> String {
    Volume::new(LatticeState::empty().unwrap())
        .unwrap()
        .0
        .raw_cid
}
fn state(s: &str) -> String {
    if s == "none" {
        empty()
    } else {
        cid(&format!("state/{s}"))
    }
}
fn fact(
    name: &str,
    parent: Option<&str>,
    height: u64,
    valid: bool,
    anchor: &str,
) -> BlockImportBatch {
    let mut facts = vec![
        json!({"block":{"_0":{"blockHash":cid(name),"parentBlockHash":parent.map(cid),"blockHeight":height,
        "postStateCID":state(name),"prevStateCID":parent.map(state).unwrap_or_else(empty),"parentStateCID":state(anchor),
        "specCID":cid("spec"),"target":"ff","nextTarget":"ff","timestamp":height,"stateDiff":{"replaced":{},"created":{}}}}}),
        json!({"work":{"_0":{"blockHash":cid(name),"contribution":{"id":cid(name),"work":if name=="b" || name=="bb" {"0x14"}else{"0x1"}}}}}),
    ];
    if valid {
        facts.push(json!({"validation":{"_0":{"blockHash":cid(name)}}}));
    }
    serde_json::from_value(json!({"facts":facts})).unwrap()
}
fn child(genesis: bool, new: &str) -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: if genesis {
            None
        } else {
            Some(Volume(Header::from_cid(cid("predecessor")).unwrap()))
        },
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume(Header::from_cid(cid("spec")).unwrap()),
        parent_state: Volume(Header::from_cid(state(new)).unwrap()),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: if genesis { 0 } else { 1 },
        timestamp: 10,
        reward_recipient: None,
        nonce: 0,
    }
}
fn main() {
    let blocks = [
        ("r", None, 0),
        ("a", Some("r"), 1),
        ("aa", Some("a"), 2),
        ("b", Some("r"), 1),
        ("bb", Some("b"), 2),
        ("u", Some("b"), 2),
    ];
    let scenarios = [
        vec![],
        vec!["r"],
        vec!["r", "a"],
        vec!["r", "a", "aa"],
        vec!["r", "b", "bb"],
        vec!["r", "a", "aa", "b", "bb"],
    ];
    let labels = ["none", "r", "a", "aa", "b", "bb", "u"];
    let context = ChainRuntimeContext::new(vec!["Nexus".into()], Some(&cid("r"))).unwrap();
    let child_context =
        ChainRuntimeContext::new(vec!["Nexus".into(), "Alpha".into()], None).unwrap();
    let mut rows = vec![];
    for valid in scenarios {
        let parent = ChainTree::restore(
            &blocks
                .iter()
                .map(|(b, p, h)| fact(b, *p, *h, valid.contains(b), "none"))
                .collect::<Vec<_>>(),
            0,
            context.clone(),
        )
        .unwrap();
        if valid.len() == 5 {
            assert_eq!(parent.canonical_tip(), cid("bb"));
        }
        for genesis in [true, false] {
            for old in labels {
                if genesis && old != "none" {
                    continue;
                }
                let tree = if genesis {
                    ChainTree::empty(child_context.clone())
                } else {
                    ChainTree::restore(
                        &[fact("predecessor", None, 0, false, old)],
                        0,
                        child_context.clone(),
                    )
                    .unwrap()
                };
                for new in labels {
                    let block = child(genesis, new);
                    let result = match tree.verify_child_parent_anchor(&block, None) {
                        Err(ParentAnchorFailure::Required(link)) => {
                            match parent.prove_parent_continuity(&link) {
                                Some(evidence) => {
                                    tree.verify_child_parent_anchor(&block, Some(&evidence))
                                }
                                None => Err(ParentAnchorFailure::Required(link)),
                            }
                        }
                        other => other,
                    };
                    let outcome = match result {
                        Ok(_) => "proven",
                        Err(ParentAnchorFailure::Required(_)) => "held",
                        Err(ParentAnchorFailure::ProtocolInvalid) => "invalid",
                        other => panic!("unexpected: {other:?}"),
                    };
                    rows.push(json!({"genesis":genesis,"old":old,"new":new,"validated":valid,"outcome":outcome}));
                }
            }
        }
    }
    println!(
        "{}",
        json!({"parents":blocks.into_iter().map(|(b,p,_)|(b,p.unwrap_or("none"))).collect::<BTreeMap<_,_>>(),"rows":rows})
    );
}
