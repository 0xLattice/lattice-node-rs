use super::*;
pub const SCHEMA_EPOCH: i64 = 47;
pub const TABLES: [&str; 10] = [
    "node_metadata",
    "admission_batches",
    "admission_facts",
    "accepted_blocks",
    "local_mempool_transactions",
    "core_meta",
    "stream_cursors",
    "headers",
    "child_indexes",
    "child_proofs",
];
pub fn initialize(db: &mut Connection, genesis: &str) -> Result<(), StoreError> {
    let tables: BTreeSet<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    if tables.is_empty() {
        let tx = db.transaction()?;
        tx.execute_batch("CREATE TABLE node_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_epoch INTEGER NOT NULL,nexus_genesis_cid TEXT NOT NULL);
CREATE TABLE admission_batches(seq INTEGER PRIMARY KEY AUTOINCREMENT,chain_path TEXT NOT NULL,payload BLOB NOT NULL,volume_roots BLOB NOT NULL,UNIQUE(chain_path,payload));
CREATE TABLE admission_facts(chain_path TEXT NOT NULL,fact_id BLOB NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(chain_path,fact_id)) WITHOUT ROWID;
CREATE TABLE accepted_blocks(chain_path TEXT NOT NULL,block_cid TEXT NOT NULL,parent_cid TEXT,admission_seq INTEGER NOT NULL,validated INTEGER NOT NULL DEFAULT 1,leaf INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(chain_path,block_cid)) WITHOUT ROWID;
CREATE TABLE local_mempool_transactions(transaction_cid TEXT PRIMARY KEY,added_at INTEGER NOT NULL CHECK(added_at>=0)) WITHOUT ROWID;
CREATE TABLE core_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE stream_cursors(chain_path TEXT NOT NULL,peer_key TEXT NOT NULL,log_id TEXT NOT NULL,position INTEGER NOT NULL CHECK(position>=0),PRIMARY KEY(chain_path,peer_key)) WITHOUT ROWID;
CREATE TABLE headers(cid TEXT PRIMARY KEY,block BLOB NOT NULL,children_cid TEXT NOT NULL);
CREATE TABLE child_indexes(cid TEXT PRIMARY KEY,bytes BLOB NOT NULL);
CREATE TABLE child_proofs(chain TEXT NOT NULL,child TEXT NOT NULL,root TEXT NOT NULL,bytes BLOB NOT NULL,PRIMARY KEY(chain,child,root));")?;
        tx.execute(
            "INSERT INTO node_metadata VALUES(1,?1,?2)",
            params![SCHEMA_EPOCH, genesis],
        )?;
        tx.commit()?;
    } else {
        if tables != TABLES.into_iter().map(String::from).collect() {
            return Err(StoreError::WipeRequired(
                "schema tables missing or unexpected".into(),
            ));
        }
        let metadata: Result<(i64, String), _> = db.query_row(
            "SELECT schema_epoch,nexus_genesis_cid FROM node_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        );
        if metadata.ok() != Some((SCHEMA_EPOCH, genesis.into())) {
            return Err(StoreError::WipeRequired(
                "schema epoch or Nexus genesis changed".into(),
            ));
        }
    }
    db.execute_batch("CREATE INDEX IF NOT EXISTS accepted_blocks_by_parent ON accepted_blocks(chain_path,parent_cid,admission_seq,block_cid);
CREATE INDEX IF NOT EXISTS accepted_blocks_frontier ON accepted_blocks(chain_path,admission_seq DESC,block_cid DESC) WHERE leaf=1;
CREATE INDEX IF NOT EXISTS admission_batches_by_level ON admission_batches(chain_path,seq);")?;
    Ok(())
}
