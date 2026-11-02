use super::*;
use lattice_block_tree::ChainTree;
use lattice_primitives::{ChainRuntimeContext, ChainSpec};
#[derive(Debug, Clone)]
pub struct ChainRecoverySnapshot {
    pub batches: Vec<BlockImportBatch>,
    pub cursors: BTreeMap<String, StreamCursor>,
    pub revision_floor: u64,
    pub log_id: Option<String>,
}
impl NodeStore {
    /// One coherent journal view. The read transaction protects this snapshot even
    /// if another connection stages a turn concurrently in WAL mode.
    pub fn recovery_snapshot(&self, path: &[String]) -> Result<ChainRecoverySnapshot, StoreError> {
        let path = path_key(path)?;
        let mut db = self.db.lock().unwrap();
        let tx = turn::transaction(&mut db, rusqlite::TransactionBehavior::Deferred)?;
        let mut batches = vec![];
        let mut after = 0;
        loop {
            let mut stmt=tx.prepare("SELECT seq,payload FROM admission_batches INDEXED BY admission_batches_by_level WHERE chain_path=?1 AND seq>?2 ORDER BY seq LIMIT 512")?;
            let mut count = 0;
            for row in stmt.query_map(params![path, after], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })? {
                let (seq, payload) = row?;
                if seq <= after {
                    return Err(StoreError::Corrupt(
                        "non-increasing admission sequence".into(),
                    ));
                }
                after = seq;
                batches.push(serde_json::from_slice(&payload)?);
                count += 1;
            }
            if count < 512 {
                break;
            }
        }
        let revision_floor = u64::try_from(batches.len())
            .map_err(|_| StoreError::Corrupt("revision floor overflow".into()))?;
        let mut cursors = BTreeMap::new();
        {
            let mut stmt=tx.prepare("SELECT peer_key,log_id,position FROM stream_cursors WHERE chain_path=?1 ORDER BY peer_key")?;
            for row in stmt.query_map([path], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })? {
                let (peer, log_id, position) = row?;
                if peer.is_empty() || position < 0 {
                    return Err(StoreError::Corrupt("malformed recovery cursor".into()));
                }
                cursors.insert(
                    peer,
                    StreamCursor {
                        log_id,
                        position: position as u64,
                    },
                );
            }
        }
        let log_id = tx
            .query_row("SELECT value FROM core_meta WHERE key='log_id'", [], |r| {
                r.get(0)
            })
            .optional()?;
        tx.commit()?;
        Ok(ChainRecoverySnapshot {
            batches,
            cursors,
            revision_floor,
            log_id,
        })
    }
    /// Recovery boundary: only this store's trusted durable facts feed the reducer.
    /// Restore and configure served directories of the parent before child levels.
    pub fn restore_chain(
        &self,
        context: ChainRuntimeContext,
        parent: Option<&ChainTree>,
        specs: Vec<ChainSpec>,
    ) -> Result<(ChainTree, ChainRecoverySnapshot), StoreError> {
        let snapshot = self.recovery_snapshot(context.path())?;
        let tree = ChainTree::restore_with_parent(
            &snapshot.batches,
            snapshot.revision_floor,
            context,
            parent,
            specs,
        )?;
        Ok((tree, snapshot))
    }
}
