use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_cashew::Header;
use lattice_node::core::{mempool::*, mining::*, sync::PeerId};
use lattice_primitives::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
fn error(e: impl std::fmt::Debug) -> String {
    let s = format!("{e:?}");
    format!("{}{}", s[..1].to_lowercase(), &s[1..])
}
#[test]
fn actual_swift_mining_event_effect_trace() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/swift-mining.json")).unwrap();
    let input = &fixture["input"];
    let spec: ChainSpec = serde_json::from_value(input["spec"].clone()).unwrap();
    let transactions: Vec<_> = input["bodies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            Transaction::new(
                BTreeMap::new(),
                Header::new(serde_json::from_value::<TransactionBody>(v.clone()).unwrap()).unwrap(),
            )
        })
        .collect();
    let block: Block =
        lattice_cashew::cbor::decode(&STANDARD.decode(input["block"].as_str().unwrap()).unwrap())
            .unwrap();
    let mut state = MiningState::new(
        "A".into(),
        Some(spec),
        MiningConfig {
            max_pending_peer_admissions: 2,
            max_pending_per_peer: 1,
            max_pending_returned: 2,
            max_pending_submitted: 1,
            max_reissues: 2,
            max_waiting_template_requests: 2,
            ..Default::default()
        },
    );
    let peer = PeerId {
        key: "peer".into(),
        session: 1,
    };
    let mut preflights: Vec<PreflightJob> = Vec::new();
    let mut builds: Vec<TemplateJob> = Vec::new();
    for (number, c) in input["commands"].as_array().unwrap().iter().enumerate() {
        let index = c["index"].as_u64().unwrap() as usize;
        let id = c["id"].as_u64().unwrap();
        let value = c["value"].as_str();
        let now = c["now"].as_i64().unwrap();
        let event = match c["op"].as_str().unwrap() {
            "receive" => Some(MiningEvent::TransactionReceived(
                transactions[index % transactions.len()].clone(),
                match value {
                    Some("local") => TransactionOrigin::Local(id),
                    Some("submitted") => TransactionOrigin::Submitted(id),
                    Some("restored") => TransactionOrigin::Restored(1000),
                    Some("returned") => TransactionOrigin::Returned,
                    _ => TransactionOrigin::Peer(peer.clone()),
                },
            )),
            "verdict" => preflights.get(index).map(|j| {
                MiningEvent::Preflighted(
                    j.clone(),
                    match value {
                        Some("future") => MempoolDisposition::Future,
                        Some("invalid") => MempoolDisposition::Invalid,
                        Some("unavailable") => MempoolDisposition::Unavailable,
                        _ => MempoolDisposition::Ready,
                    },
                    BTreeMap::from([("a".into(), 200), ("b".into(), 200), ("c".into(), 200)]),
                )
            }),
            "tip" => Some(MiningEvent::TipMoved(TipMove {
                tip_cid: value.unwrap_or("A").into(),
                confirmed: BTreeSet::new(),
                returned: vec![],
                left: vec![],
            })),
            "confirm" => Some(MiningEvent::TipMoved(TipMove {
                tip_cid: value.unwrap_or("B").into(),
                confirmed: [content_id(&transactions[index % transactions.len()]).unwrap()].into(),
                returned: vec![],
                left: vec![],
            })),
            "template" => Some(MiningEvent::TemplateRequested(
                id,
                Box::new(TemplateRequest {
                    reward_recipient: value.map(str::to_owned),
                    ..Default::default()
                }),
            )),
            "built" => builds.get(index).map(|j| {
                MiningEvent::TemplateBuilt(
                    Box::new(j.clone()),
                    if value == Some("fail") {
                        None
                    } else {
                        Some(Box::new(TemplateBuild {
                            work_id: format!("w{index}"),
                            block: block.clone(),
                            search_target: block.target,
                            targets: vec![block.target],
                            digest: String::new(),
                            chains: vec![],
                        }))
                    },
                )
            }),
            "submit" => Some(MiningEvent::SubmitWork {
                reply_id: id,
                work_id: format!("w{index}"),
                nonce: 0,
            }),
            "refuse" => Some(MiningEvent::PolicyRefused {
                tip_cid: value.unwrap_or("A").into(),
                cids: vec![content_id(&transactions[index % transactions.len()]).unwrap()],
            }),
            _ => panic!(),
        };
        let effects = event.map(|e| state.step(e, now)).unwrap_or_default();
        let encoded:Vec<_>=effects.into_iter().map(|effect|match effect {
 MiningEffect::PoolChanged(d)=>json!({"kind":"pool","added":d.added.iter().map(|i|&i.cid).collect::<Vec<_>>(),"journaled":d.journaled.iter().map(|i|&i.cid).collect::<Vec<_>>(),"removed":d.removed}),
 MiningEffect::TransactionAdmitted{reply_id,cid,count,bytes}=>json!({"kind":"admitted","id":reply_id,"cid":cid,"count":count,"bytes":bytes}),
 MiningEffect::TransactionRefused(id,e)=>json!({"kind":"refused","id":id,"error":error(e)}),
 MiningEffect::AnnounceTransaction(cid)=>json!({"kind":"announce","cid":cid}),
 MiningEffect::Preflight(j)=>{let value=json!({"kind":"preflight","cid":j.cid,"epoch":j.tip_epoch,"tip":j.tip_cid,"version":j.pool_version,"policies":j.checks_policies});preflights.push(j);value},
 MiningEffect::BuildTemplate(j)=>{let value=json!({"kind":"build","id":j.id,"epoch":j.tip_epoch,"tip":j.tip_cid,"version":j.pool_version,"transactions":j.transactions.iter().map(|tx|content_id(tx).unwrap()).collect::<Vec<_>>()});builds.push(j);value},
 MiningEffect::TemplateIssued(id,t)=>json!({"kind":"issued","id":id,"work":t.work_id,"expires":t.expires_at,"tip":t.tip_cid,"version":t.pool_version}),
 MiningEffect::TemplateRefused(id,e)=>json!({"kind":"templateRefused","id":id,"error":error(e)}),
 MiningEffect::WorkRefused(id,e)=>json!({"kind":"workRefused","id":id,"error":error(e)}),
 MiningEffect::Mined(id,b)=>json!({"kind":"mined","id":id,"cid":content_id(&b).unwrap()}),
 MiningEffect::ReturnTransactions{left,carried}=>json!({"kind":"returned","left":left,"carried":carried}),
 }).collect();
        let observed = json!({"effects":encoded,"tip":state.tip_cid(),"epoch":state.tip_epoch(),"items":state.mempool().items().iter().map(|i|&i.cid).collect::<Vec<_>>(),"version":state.mempool().version(),"journaled":state.journaled(),"pending":state.pending_admissions(),"origins":state.pending_origins(),"peer":state.pending_peer_admissions(),"submitted":state.pending_submitted(),"returned":state.pending_returned(),"waiting":state.waiting_template_requests(),"outstanding":state.outstanding_preflights(),"templates":state.templates().count()});
        assert_eq!(
            observed, fixture["observations"][number],
            "turn {number} {c}"
        );
    }
}

#[test]
fn child_context_retries_preserve_coalesced_waiters_and_bound_reissues() {
    let mut state = MiningState::new(
        "same-root".into(),
        None,
        MiningConfig {
            max_reissues: 2,
            ..Default::default()
        },
    );
    let take_job = |effects: Vec<MiningEffect>| {
        effects
            .into_iter()
            .find_map(|e| match e {
                MiningEffect::BuildTemplate(job) => Some(job),
                _ => None,
            })
            .unwrap()
    };
    let mut job = take_job(state.step(MiningEvent::TemplateRequested(41, Box::default()), 100));
    assert!(
        state
            .step(MiningEvent::TemplateRequested(42, Box::default()), 101)
            .is_empty()
    );
    for _ in 0..2 {
        let old = job.clone();
        job = take_job(state.step(MiningEvent::TemplateContextChanged(Box::new(job)), 102));
        assert_ne!(old.id, job.id);
        assert_eq!(old.tip_epoch, job.tip_epoch);
        assert_eq!(state.waiting_template_requests(), 2);
        // A late completion from the abandoned generation cannot consume the
        // waiters now owned by its replacement.
        assert!(
            state
                .step(MiningEvent::TemplateBuilt(Box::new(old), None), 103)
                .is_empty()
        );
    }
    let refused = state.step(MiningEvent::TemplateContextChanged(Box::new(job)), 104);
    let ids: BTreeSet<_> = refused
        .into_iter()
        .filter_map(|e| match e {
            MiningEffect::TemplateRefused(
                id,
                lattice_node::core::work::TemplateError::ContextChanged,
            ) => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, [41, 42].into());
    assert_eq!(state.waiting_template_requests(), 0);
}
