use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredHeader {
    pub block_cid: String,
    pub block: Vec<u8>,
    pub children_cid: String,
    pub children: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct NodeFactBatch {
    pub proofs: Vec<StoredProof>,
    pub path: Vec<String>,
    pub facts: Vec<BlockImportBatch>,
    pub headers: Vec<StoredHeader>,
    pub volume_roots: Vec<String>,
    pub cursors: BTreeMap<String, StreamCursor>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedImport {
    pub sequence: i64,
    pub chain_path: Vec<String>,
    pub batch: BlockImportBatch,
    pub volume_roots: Vec<String>,
}
impl NodeStore {
    /// Content must be committed/retained before this transaction. Every level's journal,
    /// normalized fact, header evidence, accepted-block index and cursor commits together.
    pub fn stage_node_facts(
        &self,
        levels: &[NodeFactBatch],
        log_id: &str,
    ) -> Result<(), StoreError> {
        let mut paths = BTreeSet::new();
        for level in levels {
            let path = path_key(&level.path)?;
            if !paths.insert(path) {
                return Err(StoreError::InvalidConfiguration(
                    "duplicate chain path in node fact transaction".into(),
                ));
            }
            for (peer, cursor) in &level.cursors {
                if peer.is_empty() || cursor.position > i64::MAX as u64 {
                    return Err(StoreError::InvalidConfiguration(
                        "malformed stream cursor".into(),
                    ));
                }
            }
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if !levels.is_empty() {
            tx.execute(
                "INSERT OR IGNORE INTO core_meta(key,value) VALUES('log_id',?1)",
                [log_id],
            )?;
        }
        for level in levels {
            let path = path_key(&level.path)?;
            for header in &level.headers {
                tx.execute(
                    "INSERT OR IGNORE INTO headers(cid,block,children_cid) VALUES(?1,?2,?3)",
                    params![header.block_cid, header.block, header.children_cid],
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO child_indexes(cid,bytes) VALUES(?1,?2)",
                    params![header.children_cid, header.children],
                )?;
            }
            for stored in &level.proofs {
                if level.path.len() < 2
                    || stored.proof.directory_path != level.path[1..]
                    || lattice_primitives::canonical_cid(&stored.child_cid).as_deref()
                        != Some(&stored.child_cid)
                    || lattice_primitives::canonical_cid(&stored.proof.root_cid).as_deref()
                        != Some(&stored.proof.root_cid)
                {
                    return Err(StoreError::Corrupt(
                        "child proof does not match its chain path".into(),
                    ));
                }
                let bytes = stored
                    .proof
                    .serialize()
                    .map_err(|_| StoreError::Corrupt("proof cannot serialize".into()))?;
                tx.execute("INSERT INTO child_proofs(chain,child,root,bytes) VALUES(?1,?2,?3,?4) ON CONFLICT(chain,child,root) DO UPDATE SET bytes=excluded.bytes",params![path,stored.child_cid,stored.proof.root_cid,bytes])?;
            }
            let roots: BTreeSet<_> = level.volume_roots.iter().collect();
            let roots_payload = json(&roots)?;
            for batch in &level.facts {
                let payload = json(batch)?;
                let existing: Option<i64> = tx
                    .query_row(
                        "SELECT seq FROM admission_batches WHERE chain_path=?1 AND payload=?2",
                        params![path, payload],
                        |r| r.get(0),
                    )
                    .optional()?;
                if existing.is_some() {
                    continue;
                }
                let mut facts = vec![];
                let mut blocks = BTreeMap::<String, Option<String>>::new();
                let mut validated = vec![];
                for fact in batch.facts() {
                    let id = json(&fact.id())?;
                    let encoded = json(fact)?;
                    let previous:Option<Vec<u8>>=tx.query_row("SELECT payload FROM admission_facts WHERE chain_path=?1 AND fact_id=?2",params![path,id],|r|r.get(0)).optional()?;
                    if let Some(previous) = previous
                        && previous != encoded
                    {
                        if !restates_weighed_block(&previous, &encoded)? {
                            return Err(StoreError::ConflictingImportFact);
                        }
                        tx.execute("UPDATE admission_facts SET payload=?3 WHERE chain_path=?1 AND fact_id=?2",params![path,id,encoded])?;
                    }
                    if let ChainFact::Block(block) = fact
                        && let Some(old) =
                            blocks.insert(block.block_hash.clone(), block.parent_block_hash.clone())
                        && old != block.parent_block_hash
                    {
                        return Err(StoreError::ConflictingImportFact);
                    }
                    if let ChainFact::Validation(v) = fact {
                        validated.push(v.block_hash.clone());
                    }
                    facts.push((id, encoded));
                }
                tx.execute("INSERT INTO admission_batches(chain_path,payload,volume_roots) VALUES(?1,?2,?3)",params![path,payload,roots_payload])?;
                let sequence = tx.last_insert_rowid();
                for (id, payload) in facts {
                    tx.execute("INSERT OR IGNORE INTO admission_facts(chain_path,fact_id,payload) VALUES(?1,?2,?3)",params![path,id,payload])?;
                }
                for (block, parent) in blocks {
                    let old:Option<(Option<String>,i64)>=tx.query_row("SELECT parent_cid,admission_seq FROM accepted_blocks WHERE chain_path=?1 AND block_cid=?2",params![path,block],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                    if let Some((old_parent, old_seq)) = old {
                        if old_parent != parent || old_seq <= 0 || old_seq > sequence {
                            return Err(StoreError::Corrupt(
                                "conflicting accepted-block index".into(),
                            ));
                        }
                        continue;
                    }
                    tx.execute("INSERT INTO accepted_blocks(chain_path,block_cid,parent_cid,admission_seq,validated,leaf) VALUES(?1,?2,?3,?4,0,CASE WHEN EXISTS(SELECT 1 FROM accepted_blocks WHERE chain_path=?1 AND parent_cid=?2) THEN 0 ELSE 1 END)",params![path,block,parent,sequence])?;
                    if let Some(parent) = parent {
                        tx.execute("UPDATE accepted_blocks SET leaf=0 WHERE chain_path=?1 AND block_cid=?2 AND leaf=1",params![path,parent])?;
                    }
                }
                for block in validated {
                    tx.execute("UPDATE accepted_blocks SET validated=1 WHERE chain_path=?1 AND block_cid=?2 AND validated=0",params![path,block])?;
                }
            }
            for (peer, cursor) in &level.cursors {
                tx.execute("INSERT INTO stream_cursors(chain_path,peer_key,log_id,position) VALUES(?1,?2,?3,?4) ON CONFLICT(chain_path,peer_key) DO UPDATE SET log_id=excluded.log_id,position=excluded.position",params![path,peer,cursor.log_id,cursor.position as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn has_staged_imports(&self) -> Result<bool, StoreError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT 1 FROM admission_batches LIMIT 1", [], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .is_some())
    }
    pub fn chain_log_id(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT value FROM core_meta WHERE key='log_id'", [], |r| {
                r.get(0)
            })
            .optional()?)
    }
    pub fn chain_cursors(
        &self,
        path: &[String],
    ) -> Result<BTreeMap<String, StreamCursor>, StoreError> {
        let path = path_key(path)?;
        let db = self.db.lock().unwrap();
        let mut result = BTreeMap::new();
        for row in db.prepare("SELECT peer_key,log_id,position FROM stream_cursors WHERE chain_path=?1 ORDER BY peer_key")?.query_map([path],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?)))?{let (peer,log_id,position)=row?;if peer.is_empty()||position<0{return Err(StoreError::Corrupt("malformed stream cursor row".into()))}result.insert(peer,StreamCursor{log_id,position:position as u64});}
        Ok(result)
    }
    pub fn import_batches(&self, path: &[String]) -> Result<Vec<BlockImportBatch>, StoreError> {
        let path = path_key(path)?;
        let db = self.db.lock().unwrap();
        let mut after = 0;
        let mut batches = vec![];
        loop {
            let mut count = 0;
            let mut stmt=db.prepare("SELECT seq,payload FROM admission_batches INDEXED BY admission_batches_by_level WHERE chain_path=?1 AND seq>?2 ORDER BY seq ASC LIMIT 512")?;
            for row in stmt.query_map(params![path, after], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })? {
                let (seq, payload) = row?;
                batches.push(serde_json::from_slice(&payload)?);
                after = seq;
                count += 1;
            }
            if count < 512 {
                break;
            }
        }
        Ok(batches)
    }
    pub fn staged_imports(&self, path: Option<&[String]>) -> Result<Vec<StagedImport>, StoreError> {
        let path = path.map(path_key).transpose()?;
        let db = self.db.lock().unwrap();
        let mut stmt=db.prepare("SELECT seq,chain_path,payload,volume_roots FROM admission_batches WHERE (?1 IS NULL OR chain_path=?1) ORDER BY seq ASC")?;
        let rows = stmt.query_map([path], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        })?;
        let mut result = vec![];
        for row in rows {
            let (sequence, path, payload, roots) = row?;
            let chain_path: Vec<_> = path.split('/').map(String::from).collect();
            path_key(&chain_path)
                .map_err(|_| StoreError::Corrupt("invalid admission chain path".into()))?;
            result.push(StagedImport {
                sequence,
                chain_path,
                batch: serde_json::from_slice(&payload)?,
                volume_roots: serde_json::from_slice(&roots)?,
            });
        }
        Ok(result)
    }
    /// Root-level recovery revision floor: count durable batches, never CAS volumes.
    pub fn revision_floor(&self, path: &[String]) -> Result<u64, StoreError> {
        Ok(self.db.lock().unwrap().query_row(
            "SELECT count(*) FROM admission_batches WHERE chain_path=?1",
            [path_key(path)?],
            |r| r.get::<_, i64>(0),
        )? as u64)
    }
    pub fn repair_leaf_flags(&self) -> Result<(), StoreError> {
        self.db.lock().unwrap().execute("UPDATE accepted_blocks SET leaf=NOT EXISTS(SELECT 1 FROM accepted_blocks child WHERE child.chain_path=accepted_blocks.chain_path AND child.parent_cid=accepted_blocks.block_cid) WHERE leaf IS NOT (NOT EXISTS(SELECT 1 FROM accepted_blocks child WHERE child.chain_path=accepted_blocks.chain_path AND child.parent_cid=accepted_blocks.block_cid))",[])?;
        Ok(())
    }
    pub fn header_block_bytes(&self, cid: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT block FROM headers WHERE cid=?1", [cid], |r| {
                r.get(0)
            })
            .optional()?)
    }
    pub fn child_index_bytes(&self, cid: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT bytes FROM child_indexes WHERE cid=?1", [cid], |r| {
                r.get(0)
            })
            .optional()?)
    }
}
fn restates_weighed_block(existing: &[u8], new: &[u8]) -> Result<bool, StoreError> {
    let old: ChainFact = serde_json::from_slice(existing)?;
    let new: ChainFact = serde_json::from_slice(new)?;
    if let (ChainFact::Block(old), ChainFact::Block(mut new)) = (old, new) {
        if !old.state_diff.is_empty() {
            return Ok(false);
        }
        new.state_diff = Default::default();
        Ok(old == new)
    } else {
        Ok(false)
    }
}
