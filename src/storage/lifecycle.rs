use super::*;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
/// The guard owns the operating-system directory lock for its entire lifetime.
/// It must outlive the stores and every asynchronous storage operation.
pub struct StorageDirectoryLock {
    file: File,
}
impl StorageDirectoryLock {
    pub fn acquire(directory: impl AsRef<Path>) -> Result<Self, StoreError> {
        std::fs::create_dir_all(directory.as_ref())?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let path = directory.as_ref().join(".lattice-node.lock");
        let file = options.open(&path)?;
        Self::lock_candidate(file, &path)
    }
    fn lock_candidate(file: File, path: &Path) -> Result<Self, StoreError> {
        file.try_lock_exclusive().map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                StoreError::DirectoryInUse
            } else {
                StoreError::Io(e)
            }
        })?;
        // Wipe may have detached this directory after open but before flock.
        // Never protect the new pathname with a lease on an obsolete inode.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let held = file.metadata()?;
            let current = std::fs::symlink_metadata(path)?;
            if !current.is_file() || held.dev() != current.dev() || held.ino() != current.ino() {
                return Err(StoreError::DirectoryInUse);
            }
        }
        #[cfg(not(unix))]
        let _ = path;
        Ok(Self { file })
    }
}
impl Drop for StorageDirectoryLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMempoolTransactionRecord {
    pub transaction_cid: String,
    pub added_at: i64,
}
fn canonical(cid: &str) -> bool {
    lattice_primitives::canonical_cid(cid).as_deref() == Some(cid)
}
impl NodeStore {
    pub fn persist_local_mempool_transaction(
        &self,
        cid: &str,
        added_at: i64,
    ) -> Result<(), StoreError> {
        if !canonical(cid) || added_at < 0 {
            return Err(StoreError::InvalidConfiguration(
                "malformed local mempool reference".into(),
            ));
        }
        self.db.lock().unwrap().execute("INSERT OR IGNORE INTO local_mempool_transactions(transaction_cid,added_at) VALUES(?1,?2)",params![cid,added_at])?;
        Ok(())
    }
    pub fn remove_local_mempool_transaction(&self, cid: &str) -> Result<(), StoreError> {
        if !canonical(cid) {
            return Err(StoreError::InvalidConfiguration(
                "malformed local mempool CID".into(),
            ));
        }
        self.db.lock().unwrap().execute(
            "DELETE FROM local_mempool_transactions WHERE transaction_cid=?1",
            [cid],
        )?;
        Ok(())
    }
    pub fn local_mempool_transactions(
        &self,
    ) -> Result<Vec<LocalMempoolTransactionRecord>, StoreError> {
        let db = self.db.lock().unwrap();
        let mut stmt=db.prepare("SELECT transaction_cid,added_at FROM local_mempool_transactions ORDER BY added_at,transaction_cid")?;
        let mut records = vec![];
        for record in stmt.query_map([], |r| {
            Ok(LocalMempoolTransactionRecord {
                transaction_cid: r.get(0)?,
                added_at: r.get(1)?,
            })
        })? {
            let record = record?;
            if !canonical(&record.transaction_cid) || record.added_at < 0 {
                return Err(StoreError::Corrupt(
                    "malformed durable mempool reference".into(),
                ));
            }
            records.push(record);
        }
        Ok(records)
    }
}

impl NodeStore {
    /// One atomic local-pool journal mutation; timestamps are whole seconds,
    /// matching the pinned node database (the reducer itself uses milliseconds).
    pub(crate) fn apply_pool_journal(
        &self,
        delta: &crate::core::mining::PoolDelta,
    ) -> Result<(), StoreError> {
        for item in &delta.journaled {
            if !canonical(&item.cid) || item.added_at < 0 {
                return Err(StoreError::InvalidConfiguration(
                    "invalid pool journal item".into(),
                ));
            }
        }
        if delta.removed.iter().any(|cid| !canonical(cid)) {
            return Err(StoreError::InvalidConfiguration(
                "invalid removed pool CID".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = turn::transaction(&mut db, rusqlite::TransactionBehavior::Deferred)?;
        for item in &delta.journaled {
            tx.execute("INSERT OR IGNORE INTO local_mempool_transactions(transaction_cid,added_at) VALUES(?1,?2)",params![item.cid,item.added_at/1000])?;
        }
        for cid in &delta.removed {
            tx.execute(
                "DELETE FROM local_mempool_transactions WHERE transaction_cid=?1",
                [cid],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod directory_lock_tests {
    use super::*;

    #[test]
    fn startup_descriptor_opened_before_wipe_cannot_lease_recreated_storage() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("Nexus");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join(".lattice-node.lock");
        // Startup A pauses between opening its descriptor and taking flock.
        let stale = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let wipe = StorageDirectoryLock::acquire(&directory).unwrap();
        let detached = temp.path().join("detached");
        std::fs::rename(&directory, &detached).unwrap();
        std::fs::remove_dir_all(&detached).unwrap();
        drop(wipe);
        let fresh = StorageDirectoryLock::acquire(&directory).unwrap();
        // Startup A can flock its obsolete descriptor, but must fail validation.
        assert!(matches!(
            StorageDirectoryLock::lock_candidate(stale, &path),
            Err(StoreError::DirectoryInUse)
        ));
        assert!(matches!(
            StorageDirectoryLock::acquire(&directory),
            Err(StoreError::DirectoryInUse)
        ));
        drop(fresh);
        assert!(StorageDirectoryLock::acquire(&directory).is_ok());
    }
}
