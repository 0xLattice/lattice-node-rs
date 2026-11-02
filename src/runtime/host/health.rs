//! Immutable read snapshots: HTTP never enters the command queue or replays the
//! journal. Publication follows completed actor work and durable recovery.
use super::*;
use lattice_block_tree::ChainTree;
use std::sync::RwLock;

#[derive(Clone, Default)]
pub(super) struct PublishedHealth(
    Arc<RwLock<BTreeMap<String, Value>>>,
    Arc<std::sync::atomic::AtomicUsize>,
);
impl PublishedHealth {
    pub(super) fn read(&self, route: &str) -> (u16, Value) {
        let query = route.split_once('?').map_or("", |(_, q)| q);
        let path = form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "chainPath")
            .map_or_else(|| "Nexus".into(), |(_, value)| value.into_owned());
        // Deliberately exact: an unhosted path, even with retained facts, is 404.
        match self.0.read().unwrap().get(&path).cloned() {
            Some(value) => (200, value),
            None => (404, json!({"error":"not found"})),
        }
    }
}
fn snapshot(tree: &ChainTree, path: &[String], genesis: &str, count: usize, bytes: usize) -> Value {
    let tip = tree.heaviest_executed_tip(&BTreeSet::new());
    let mut value = json!({
        "phase":if tip.is_empty() {"awaitingGenesis"} else {"active"},
        "chainPath":path,"nexusGenesisCID":genesis,
        "mempoolCount":count,"mempoolBytes":bytes
    });
    if !tip.is_empty() {
        value["height"] = json!(tree.height(&tip).unwrap_or(0).to_string());
        value["tipCID"] = json!(tip);
    }
    if let Some(height) = tree.height(tree.canonical_tip()) {
        value["bestHeaderHeight"] = json!(height.to_string());
    }
    value
}
impl Host {
    pub(super) fn publish_health(&self) {
        let pool = self.mining.state().mempool();
        let mut values = BTreeMap::from([(
            "Nexus".into(),
            snapshot(
                self.chain.tree(),
                &["Nexus".into()],
                &self.genesis,
                pool.count(),
                pool.byte_count(),
            ),
        )]);
        for path in self.levels.ordered() {
            let pool = self
                .child_mining
                .get(&path)
                .map(|m| m.core.state().mempool());
            values.insert(
                path.join("/"),
                snapshot(
                    self.levels.get(&path).unwrap().tree(),
                    &path,
                    &self.genesis,
                    pool.map_or(0, |p| p.count()),
                    pool.map_or(0, |p| p.byte_count()),
                ),
            );
        }
        // Compute first; readers hold this lock only to clone a small DTO.
        *self.health.0.write().unwrap() = values;
        self.health.1.store(
            self.network.peers().len(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl HostHandle {
    /// Root operator metrics from published state, never the actor mailbox.
    pub fn metrics(&self, process_start_time: f64) -> String {
        let values = self.health.0.read().unwrap();
        let root = values.get("Nexus");
        let height = |field: &str| {
            root.and_then(|v| v[field].as_str())
                .and_then(|v| v.parse().ok())
        };
        super::super::metrics::render(&super::super::metrics::Sample {
            chain_path: &["Nexus".into()],
            validated_height: height("height"),
            weighed_height: height("bestHeaderHeight"),
            peers: self.health.1.load(std::sync::atomic::Ordering::Relaxed),
            mempool_transactions: root.and_then(|v| v["mempoolCount"].as_u64()).unwrap_or(0),
            process_start_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_block_tree::{BlockImportBatch, ChainBlockFact, ChainFact, ChainWorkFact};
    use lattice_primitives::{ChainRuntimeContext, StateDiff};
    fn cid(name: &str) -> String {
        lattice_cashew::cid_for_bytes(0x71, name.as_bytes())
    }
    // Trusted journal fixtures exercise selection, not network admission.
    fn batch(
        name: &str,
        parent: Option<&str>,
        height: u64,
        work: u64,
        executed: bool,
    ) -> Vec<BlockImportBatch> {
        let hash = cid(name);
        let facts = vec![
            ChainFact::Block(Box::new(ChainBlockFact {
                block_hash: hash.clone(),
                parent_block_hash: parent.map(cid),
                block_height: height,
                post_state_cid: cid(&format!("post/{name}")),
                prev_state_cid: parent
                    .map(|p| cid(&format!("post/{p}")))
                    .unwrap_or_else(|| cid("empty")),
                parent_state_cid: Some(cid("parent-state")),
                spec_cid: cid("spec"),
                target: "ff".into(),
                next_target: "ff".into(),
                timestamp: height as i64,
                state_diff: StateDiff::default(),
                child_commitments: None,
            })),
            ChainFact::Work(ChainWorkFact {
                block_hash: hash.clone(),
                contribution: serde_json::from_value(
                    json!({"id":cid(&format!("grind/{name}")),"work":format!("{work:064x}")}),
                )
                .unwrap(),
            }),
        ];
        let mut batches = vec![serde_json::from_value(json!({"facts":facts})).unwrap()];
        if executed {
            batches.push(BlockImportBatch::validation(hash));
        }
        batches
    }
    #[test]
    fn snapshot_uses_executed_selection_and_canonical_header_height() {
        let path = vec!["Nexus".into()];
        let batches = [
            batch("g", None, 0, 1, true),
            batch("a", Some("g"), 1, 10, true),
            batch("a2", Some("a"), 2, 1, true),
            batch("b", Some("g"), 1, 100, false),
        ]
        .concat();
        let tree = ChainTree::restore(
            &batches,
            0,
            ChainRuntimeContext::new(path.clone(), Some(&cid("g"))).unwrap(),
        )
        .unwrap();
        assert_eq!(tree.canonical_tip(), cid("b"));
        let value = snapshot(&tree, &path, &cid("g"), 3, 99);
        assert_eq!(value["tipCID"], cid("a2"));
        assert_eq!(value["height"], "2");
        assert_eq!(value["bestHeaderHeight"], "1");
        assert_eq!(value["mempoolCount"], 3);
        assert!(value.get("templateDigest").is_none());
        assert!(value.get("revision").is_none());
    }
    #[test]
    fn exact_hosted_scopes_are_independent_of_actor_capacity() {
        let path = vec!["Nexus".into(), "Alpha".into()];
        let empty = ChainTree::empty(ChainRuntimeContext::new(path.clone(), None).unwrap());
        let value = snapshot(&empty, &path, &cid("g"), 0, 0);
        assert_eq!(value["phase"], "awaitingGenesis");
        assert!(value.get("height").is_none());
        assert!(value.get("tipCID").is_none());
        assert!(value.get("bestHeaderHeight").is_none());
        let published = PublishedHealth::default();
        published
            .0
            .write()
            .unwrap()
            .insert("Nexus/Alpha".into(), value.clone());
        let (commands, _unpolled_actor) = mpsc::channel(1);
        commands.try_send(Command::Shutdown).unwrap();
        let handle = HostHandle {
            commands,
            health: published.clone(),
        };
        assert_eq!(
            handle.health("/health?chainPath=Nexus%2FAlpha"),
            (200, value)
        );
        let metrics = handle.metrics(123.5);
        assert!(metrics.contains("process_start_time_seconds{chain=\"Nexus\"} 123.5"));
        assert!(!metrics.contains("lattice_chain_tip_height{"));
        for path in ["Nexus//Alpha", "Nexus/Alpha/", "Nexus/Beta", ""] {
            assert_eq!(handle.health(&format!("/health?chainPath={path}")).0, 404);
        }
        published.0.write().unwrap().clear();
        assert_eq!(handle.health("/health?chainPath=Nexus/Alpha").0, 404);
    }
}
