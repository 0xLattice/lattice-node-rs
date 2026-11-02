use super::*;
use lattice_block_tree::{ChainBlockFact, ChainFactId};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum BlockStatus {
    Header = 0,
    Executed = 1,
    ExecutedAndPinned = 2,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCountChanges {
    pub entered: Vec<String>,
    pub left: Vec<String>,
}
impl NodeStore {
    pub fn block_fact(
        &self,
        cid: &str,
        path: &[String],
    ) -> Result<Option<ChainBlockFact>, StoreError> {
        let payload: Option<Vec<u8>> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT payload FROM admission_facts WHERE chain_path=?1 AND fact_id=?2",
                params![path_key(path)?, json(&ChainFactId::Block(cid.into()))?],
                |r| r.get(0),
            )
            .optional()?;
        match payload.map(|p| serde_json::from_slice(&p)).transpose()? {
            Some(ChainFact::Block(fact)) => Ok(Some(*fact)),
            _ => Ok(None),
        }
    }
    pub fn state_count(&self, cid: &str, path: &[String]) -> Result<i64, StoreError> {
        let value: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM core_meta WHERE key=?1",
                [format!("state_pin:{}:{cid}", path_key(path)?)],
                |r| r.get(0),
            )
            .optional()?;
        Ok(value.and_then(|v| v.parse().ok()).unwrap_or(0))
    }
    pub fn state_tip(&self, path: &[String]) -> Result<Option<String>, StoreError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM core_meta WHERE key=?1",
                [format!("state_tip:{}", path_key(path)?)],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub fn forget_state_counts(&self) -> Result<(), StoreError> {
        self.db.lock().unwrap().execute(
            "DELETE FROM core_meta WHERE substr(key,1,10) IN ('state_pin:','state_tip:')",
            [],
        )?;
        Ok(())
    }
    /// Atomic reference-count adjustment. Swift traps on signed overflow; Rust returns
    /// an error and rolls the complete transition back instead of wrapping.
    pub fn move_state_counts(
        &self,
        path: &[String],
        tip: &str,
        changes: &BTreeMap<String, i64>,
    ) -> Result<StateCountChanges, StoreError> {
        let path = path_key(path)?;
        let mut db = self.db.lock().unwrap();
        let tx = turn::transaction(&mut db, rusqlite::TransactionBehavior::Immediate)?;
        let mut result = StateCountChanges {
            entered: vec![],
            left: vec![],
        };
        for (cid, change) in changes.iter().filter(|(_, n)| **n != 0) {
            let key = format!("state_pin:{path}:{cid}");
            let value: Option<String> = tx
                .query_row("SELECT value FROM core_meta WHERE key=?1", [&key], |r| {
                    r.get(0)
                })
                .optional()?;
            let held: i64 = value.and_then(|s| s.parse().ok()).unwrap_or(0);
            let count = held
                .checked_add(*change)
                .ok_or_else(|| StoreError::Corrupt("state reference count overflow".into()))?
                .max(0);
            if count > 0 {
                tx.execute(
                    "INSERT OR REPLACE INTO core_meta(key,value) VALUES(?1,?2)",
                    params![key, count.to_string()],
                )?;
                if held == 0 {
                    result.entered.push(cid.clone());
                }
            } else {
                tx.execute("DELETE FROM core_meta WHERE key=?1", [key])?;
                result.left.push(cid.clone());
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO core_meta(key,value) VALUES(?1,?2)",
            params![format!("state_tip:{path}"), tip],
        )?;
        tx.commit()?;
        Ok(result)
    }
    pub fn block_status(
        &self,
        cid: &str,
        path: &[String],
    ) -> Result<Option<BlockStatus>, StoreError> {
        if lattice_primitives::canonical_cid(cid).as_deref() != Some(cid) {
            return Err(StoreError::Corrupt("noncanonical block lookup".into()));
        }
        let raw: Option<i64> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT validated FROM accepted_blocks WHERE chain_path=?1 AND block_cid=?2",
                params![path_key(path)?, cid],
                |r| r.get(0),
            )
            .optional()?;
        match raw {
            None => Ok(None),
            Some(0) => Ok(Some(BlockStatus::Header)),
            Some(1) => Ok(Some(BlockStatus::Executed)),
            Some(2) => Ok(Some(BlockStatus::ExecutedAndPinned)),
            _ => Err(StoreError::Corrupt("invalid block status".into())),
        }
    }
    pub fn is_genesis(&self, cid: &str, path: &[String]) -> Result<bool, StoreError> {
        Ok(self.db.lock().unwrap().query_row("SELECT 1 FROM accepted_blocks WHERE chain_path=?1 AND block_cid=?2 AND parent_cid IS NULL",params![path_key(path)?,cid],|r|r.get::<_,i64>(0)).optional()?.is_some())
    }
}
