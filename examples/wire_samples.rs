use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_node::core::{sync::*, wire};
use lattice_primitives::*;
use std::collections::BTreeMap;
fn root() -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: None,
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume::new(ChainSpec {
            max_number_of_transactions_per_block: 100,
            max_state_growth: 100_000,
            max_block_size: 1_000_000,
            premine: 0,
            target_block_time: 1000,
            initial_reward: 1024,
            halving_interval: 210_000,
            half_life: 120,
            wasm_policies: vec![],
        })
        .unwrap(),
        parent_state: empty.clone(),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: 0,
        timestamp: 1000,
        reward_recipient: None,
        nonce: 0,
    }
}
fn main() {
    let root = root();
    let cid = content_id(&root).unwrap();
    let messages = [
        SyncMessage::GetStream {
            request_id: u64::MAX,
            log_id: None,
            after: 0,
            own: String::new(),
        },
        SyncMessage::GetStream {
            request_id: 1,
            log_id: Some("log/\"".into()),
            after: u64::MAX,
            own: "local".into(),
        },
        SyncMessage::Stream(StreamPage {
            request_id: 0,
            log_id: "log".into(),
            entries: vec![StreamEntry {
                position: 1,
                entry: LogEntry {
                    kind: LogKind::Header,
                    cid: cid.clone(),
                    block: cid.clone(),
                },
            }],
            has_more: true,
        }),
        SyncMessage::GetData {
            request_id: 1,
            cids: vec![cid.clone()],
        },
        SyncMessage::GetAncestors {
            request_id: 1,
            cid: cid.clone(),
            max: 10,
        },
        SyncMessage::Headers(HeadersResponse {
            request_id: 1,
            entries: vec![HeaderEntry {
                block: root.clone(),
                children: Some(root.children.node.as_deref().unwrap().clone()),
                proofs: vec![],
                spec: Some(root.spec.0.node.as_deref().unwrap().clone()),
            }],
            has_more: false,
        }),
        SyncMessage::Headers(HeadersResponse {
            request_id: 0,
            entries: vec![],
            has_more: false,
        }),
    ];
    let rows:Vec<_>=messages.iter().enumerate().map(|(i,m)|{let (topic,bytes)=wire::encode(m,&["Nexus".into()]).unwrap();serde_json::json!({"name":format!("valid-{i}"),"topic":topic,"payload":STANDARD.encode(bytes)})}).collect();
    println!("{}", serde_json::to_string(&rows).unwrap());
}
