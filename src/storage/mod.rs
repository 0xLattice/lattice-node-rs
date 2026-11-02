use lattice_block_tree::{BlockImportBatch, ChainFact};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Mutex,
    time::Duration,
};
mod admission;
mod header;
mod turn;
pub use admission::*;
mod journal;
mod lifecycle;
mod materialized;
pub use materialized::*;
mod proof;
mod recovery;
pub use proof::*;
mod schema;
mod state;
pub use journal::*;
pub use lifecycle::*;
pub use recovery::*;
pub use state::*;
pub const CURRENT_SCHEMA_EPOCH: i64 = schema::SCHEMA_EPOCH;
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("parent anchor: {0}")]
    ParentAnchor(#[from] lattice_block_tree::ParentAnchorFailure),
    #[error("child proof: {0}")]
    Proof(#[from] lattice_block_tree::ChildProofFailure),
    #[error("header admission: {0}")]
    Admission(#[from] lattice_block_tree::AdmissionError),
    #[error("content: {0}")]
    Content(#[from] lattice_cashew::Error),
    #[error("volume storage: {0}")]
    Volume(#[from] lattice_volume_broker::Error),
    #[error("consensus recovery: {0}")]
    Restore(#[from] lattice_block_tree::RestoreError),
    #[error("storage directory already in use")]
    DirectoryInUse,
    #[error("storage I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid node configuration: {0}")]
    InvalidConfiguration(String),
    #[error("incompatible node store: {0}")]
    WipeRequired(String),
    #[error("conflicting immutable import fact")]
    ConflictingImportFact,
    #[error("corrupt node store: {0}")]
    Corrupt(String),
    #[error("SQLite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}
pub struct NodeStore {
    db: turn::GatedConnection,
    pub nexus_genesis_cid: String,
}
impl NodeStore {
    pub fn open(path: impl AsRef<Path>, genesis: impl Into<String>) -> Result<Self, StoreError> {
        let genesis = genesis.into();
        if genesis.is_empty() {
            return Err(StoreError::InvalidConfiguration(
                "Nexus genesis CID is empty".into(),
            ));
        }
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(5))?;
        schema::initialize(&mut db, &genesis)?;
        let journal: String = db.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if journal.to_lowercase() != "wal" {
            return Err(StoreError::Corrupt("WAL unavailable".into()));
        }
        db.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        Ok(Self {
            db: turn::GatedConnection::new(db),
            nexus_genesis_cid: genesis,
        })
    }
}
pub(crate) fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    // serde_json Value maps sort keys, matching Swift sortedKeys/withoutEscapingSlashes.
    Ok(serde_json::to_vec(&serde_json::to_value(value)?)?)
}
pub(crate) fn path_key(path: &[String]) -> Result<String, StoreError> {
    if path.first().map(String::as_str) != Some("Nexus")
        || path.len() > u16::MAX as usize + 1
        || path.iter().any(|p| !lattice_primitives::child_directory(p))
    {
        return Err(StoreError::InvalidConfiguration(
            "chain path must be Nexus-rooted and consensus-valid".into(),
        ));
    }
    Ok(path.join("/"))
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCursor {
    #[serde(rename = "logID")]
    pub log_id: String,
    pub position: u64,
}

mod retention;
pub use retention::*;

#[cfg(test)]
mod turn_tests;
