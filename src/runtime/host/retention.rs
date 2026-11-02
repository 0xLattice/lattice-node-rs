use super::*;
use crate::storage::StateRetention;
impl Host {
    /// Operational retention failures keep extra data and never change a
    /// consensus verdict. Maintenance runs after the entire queued node turn,
    /// so returned transactions and child wakeups see their working content.
    pub(super) fn maintain_retention(&self) {
        let policy = self.storage.retention_policy();
        if policy == crate::storage::RetentionPolicy::default() {
            return;
        }
        let mut tips = BTreeMap::new();
        let root_tip = self.chain.tree().heaviest_executed_tip(&BTreeSet::new());
        if !root_tip.is_empty() {
            tips.insert(vec!["Nexus".into()], root_tip);
        }
        for path in self.levels.ordered() {
            if let Some(level) = self.levels.get(&path) {
                let tip = level.tree().heaviest_executed_tip(&BTreeSet::new());
                if !tip.is_empty() {
                    tips.insert(path, tip);
                }
            }
        }
        if policy.state == StateRetention::Tip {
            for (path, tip) in &tips {
                if let Err(e) = self.storage.keep_tip_state(tip, path) {
                    eprintln!("retention state maintenance: {e}");
                }
            }
        }
        if let Err(e) = self.storage.reconcile_read_scopes(policy.state) {
            eprintln!("retention reads maintenance: {e}");
        }
        if let Err(e) = self.storage.release_old_bodies(policy.bodies, &tips) {
            eprintln!("retention bodies maintenance: {e}");
        }
    }
}
