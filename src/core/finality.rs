//! Node-local header acceptance windows; never consensus finality.
use lattice_block_tree::ChainTree;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FinalityWindows {
    default: Option<u64>,
    overrides: BTreeMap<Vec<String>, u64>,
}
impl FinalityWindows {
    /// Zero disables the window, including an explicit override of a default.
    pub fn new(default: Option<u64>, overrides: BTreeMap<String, u64>) -> anyhow::Result<Self> {
        let mut parsed = BTreeMap::new();
        for (path, window) in overrides {
            let path: Vec<String> = path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect();
            anyhow::ensure!(
                path.first().is_some_and(|s| s == "Nexus")
                    && path
                        .iter()
                        .skip(1)
                        .all(|s| lattice_primitives::child_directory(s)),
                "invalid finality chain path"
            );
            anyhow::ensure!(
                parsed.insert(path, window).is_none(),
                "duplicate normalized finality chain path"
            );
        }
        Ok(Self {
            default: default.filter(|n| *n > 0),
            overrides: parsed,
        })
    }
    pub fn window(&self, path: &[String]) -> Option<u64> {
        self.overrides
            .get(path)
            .copied()
            .or(self.default)
            .filter(|n| *n > 0)
    }
    pub fn decorate_info(&self, path: &[String], mut info: serde_json::Value) -> serde_json::Value {
        if let Some(window) = self.window(path) {
            info["finalityWindow"] = serde_json::json!(window.to_string());
        }
        info
    }
}
pub(crate) fn height(tree: &ChainTree) -> u64 {
    tree.height(tree.canonical_tip()).unwrap_or(0)
}
pub(crate) fn past(tree: &ChainTree, height: u64, window: Option<u64>) -> bool {
    past_height(self::height(tree), height, window)
}
fn past_height(tip: u64, height: u64, window: Option<u64>) -> bool {
    window
        .filter(|n| *n > 0)
        .is_some_and(|window| tip.saturating_sub(height) > window)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shorter_heavier_tip_reopens_window_despite_taller_side_branch() {
        use lattice_block_tree::{BlockImportBatch, ChainTree};
        use lattice_primitives::ChainRuntimeContext;
        use serde_json::json;
        let cid = |label: &str| lattice_cashew::cid_for_bytes(0x71, label.as_bytes());
        let batch = |label: &str,
                     parent: Option<&str>,
                     height: u64,
                     weight: u64|
         -> BlockImportBatch {
            serde_json::from_value(json!({"facts":[
                {"block":{"_0":{"blockHash":cid(label),"parentBlockHash":parent.map(cid),"blockHeight":height,"postStateCID":cid(label),"prevStateCID":cid("state"),"parentStateCID":cid("state"),"specCID":cid("spec"),"target":"ff","nextTarget":"ff","timestamp":height,"stateDiff":{"replaced":{},"created":{}}}}},
                {"work":{"_0":{"blockHash":cid(label),"contribution":{"id":cid(label),"work":format!("0x{weight:x}")}}}}
            ]})).unwrap()
        };
        let mut batches = vec![
            batch("root", None, 0, 1),
            batch("a1", Some("root"), 1, 1),
            batch("a2", Some("a1"), 2, 1),
            batch("a3", Some("a2"), 3, 1),
            batch("a4", Some("a3"), 4, 1),
            batch("a5", Some("a4"), 5, 1),
        ];
        let context =
            || ChainRuntimeContext::new(vec!["Nexus".into()], Some(&cid("root"))).unwrap();
        let long = ChainTree::restore(&batches, 0, context()).unwrap();
        assert!(past(&long, 0, Some(2)));
        let mut pool = crate::core::headers::RootHeaderPool::default();
        pool.refresh_finality(&long);
        pool.declined.insert(cid("previously-old"));
        batches.push(batch("heavy", Some("root"), 1, 100));
        let short = ChainTree::restore(&batches, 0, context()).unwrap();
        assert_eq!(short.highest_block_height(), 5);
        assert_eq!(height(&short), 1);
        assert!(!past(&short, 0, Some(2)));
        pool.refresh_finality(&short);
        assert!(pool.declined.is_empty());
        crate::core::chain::assert_shorter_tip_reopens_before_cursor(long, short);
    }
    #[test]
    fn pinned_swift_windows_and_boundary_vectors() {
        let rows: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/finality-swift.json")).unwrap();
        for row in rows.as_array().unwrap() {
            let default = row["default"].as_u64();
            let overrides = serde_json::from_value(row["overrides"].clone()).unwrap();
            let windows = FinalityWindows::new(default, overrides).unwrap();
            let path: Vec<String> = row["path"]
                .as_str()
                .unwrap()
                .split('/')
                .map(str::to_owned)
                .collect();
            let selected = windows.window(&path);
            assert_eq!(selected, row["window"].as_u64(), "{row}");
            let tip = row["tip"].as_u64().unwrap();
            let candidate = row["candidate"].as_u64().unwrap();
            assert_eq!(
                past_height(tip, candidate, selected),
                row["declined"].as_bool().unwrap(),
                "{row}"
            );
            let dto = windows.decorate_info(&path, serde_json::json!({}));
            assert_eq!(
                dto.get("finalityWindow").and_then(|v| v.as_str()),
                selected.map(|n| n.to_string()).as_deref()
            );
        }
        assert!(
            FinalityWindows::new(
                None,
                BTreeMap::from([("Nexus//A".into(), 1), ("Nexus/A".into(), 2)])
            )
            .is_err()
        );
        assert!(FinalityWindows::new(None, BTreeMap::from([("Other/A".into(), 1)])).is_err());
    }
}
