//! Worker-owned dependency leases with an on-disk reachability frontier.
use super::*;
use lattice_cashew::{
    ContentSource, SerializedVolume,
    cbor::{self, CborValue},
};
use lattice_primitives::{Block, BlockHeader};
use lattice_volume_broker::{DiskReadSnapshot, RetainedRootBroker};
use std::sync::Arc;

pub(crate) struct ConnectionContent {
    snapshot: DiskReadSnapshot,
    state_jobs: Mutex<BTreeSet<String>>,
    read_job: String,
    original_jobs: Vec<String>,
    storage: Arc<NodeStorage>,
}
struct Sources<'a> {
    snapshot: &'a DiskReadSnapshot,
    entries: BTreeMap<&'a str, &'a [u8]>,
    failure: Mutex<Option<lattice_volume_broker::Error>>,
}
impl ContentSource for Sources<'_> {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        if let Some(bytes) = self.entries.get(cid) {
            return Ok(bytes.to_vec());
        }
        self.snapshot
            .fetch_data(&[cid.to_owned()].into_iter().collect())
            .map_err(|e| {
                *self.failure.lock().unwrap() = Some(e);
                lattice_cashew::Error::MissingContent(cid.into())
            })?
            .remove(cid)
            .ok_or_else(|| lattice_cashew::Error::MissingContent(cid.into()))
    }
}
impl Sources<'_> {
    fn check(&self) -> Result<(), StoreError> {
        match self.failure.lock().unwrap().take() {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}
impl ConnectionContent {
    pub(crate) fn new(
        snapshot: DiskReadSnapshot,
        storage: Arc<NodeStorage>,
        cid: &str,
        original_jobs: Vec<String>,
    ) -> Self {
        Self {
            snapshot,
            state_jobs: Mutex::new(BTreeSet::new()),
            read_job: format!("connection-read-{cid}"),
            original_jobs,
            storage,
        }
    }
    pub(crate) fn snapshot(&self) -> &DiskReadSnapshot {
        &self.snapshot
    }
    pub(crate) fn preserve(&self, header: &BlockHeader) -> Result<(), StoreError> {
        let mut owned = lattice_cashew::volume::serialize_recursive(header, &())?;
        owned.sort_by(|a, b| a.root.cmp(&b.root));
        let source = Sources {
            snapshot: &self.snapshot,
            failure: Mutex::new(None),
            entries: owned
                .iter()
                .flat_map(|v| v.entries.iter().map(|(c, b)| (c.as_str(), b.as_slice())))
                .collect(),
        };
        let db = Connection::open("")?;
        db.execute_batch("PRAGMA temp_store=FILE; PRAGMA cache_size=-1024; PRAGMA journal_mode=OFF; CREATE TABLE frontier(root TEXT PRIMARY KEY,done INTEGER NOT NULL DEFAULT 0); CREATE INDEX ready ON frontier(done,root); CREATE TABLE states(state TEXT,root TEXT,header BLOB,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(state,root)); CREATE INDEX state_ready ON states(state,done,root);")?;
        if let Some(block) = header.node.as_deref() {
            self.preserve_state(&db, &source, &block.post_state)?;
        }
        db.execute("INSERT INTO frontier(root) VALUES(?1)", [&header.raw_cid])?;
        loop {
            let root: Option<String> = db
                .query_row(
                    "SELECT root FROM frontier WHERE done=0 ORDER BY root LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(root) = root else {
                break;
            };
            db.execute("UPDATE frontier SET done=1 WHERE root=?1", [&root])?;
            let volume = match self.snapshot.fetch_volume(&root)? {
                Some(volume) => Some(volume),
                None => match owned.binary_search_by(|v| v.root.cmp(&root)) {
                    Ok(index) => Some(owned[index].clone()),
                    Err(_) => self.snapshot.fetch_covering_volume(&root)?,
                },
            };
            let Some(volume) = volume else {
                continue;
            };
            volume.validate()?;
            // Child bodies are anticipatory dependencies. Malformed/unavailable
            // child data cannot change the already-authenticated root verdict.
            for bytes in volume.entries.values() {
                if let Ok(block) = cbor::decode::<Block>(bytes) {
                    self.preserve_state(&db, &source, &block.post_state)?;
                }
            }
            self.storage
                .cache_volumes(&self.read_job, std::slice::from_ref(&volume))?;
            for cid in references(&volume)? {
                db.execute("INSERT OR IGNORE INTO frontier(root) VALUES(?1)", [&cid])?;
            }
        }
        Ok(())
    }
    fn preserve_state(
        &self,
        db: &Connection,
        source: &Sources<'_>,
        state: &lattice_primitives::LatticeStateHeader,
    ) -> Result<(), StoreError> {
        use super::retention::StateHeader;
        let job = format!("connection-state:{}:{}", state.raw_cid, self.read_job);
        if !self.state_jobs.lock().unwrap().insert(job.clone()) {
            return Ok(());
        }
        let first = StateHeader::State(state.clone());
        db.execute(
            "INSERT OR IGNORE INTO states(state,root,header) VALUES(?1,?2,?3)",
            params![state.raw_cid, first.cid(), serde_json::to_vec(&first)?],
        )?;
        loop {
            let next: Option<(String, Vec<u8>)> = db.query_row("SELECT root,header FROM states WHERE state=?1 AND done=0 ORDER BY root LIMIT 1", [&state.raw_cid], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
            let Some((cid, bytes)) = next else {
                break;
            };
            db.execute(
                "UPDATE states SET done=1 WHERE state=?1 AND root=?2",
                params![state.raw_cid, cid],
            )?;
            let node: StateHeader = serde_json::from_slice(&bytes)?;
            // Each typed state edge is an independent Volume. Resolving one
            // node recreates its exact boundary even when only a containing
            // complete Volume supplied the raw bytes. Missing nodes stay absent.
            let materialized = node.materialize(source);
            source.check()?;
            let Ok(mut volumes) = materialized else {
                continue;
            };
            for volume in &mut volumes {
                if let Some(existing) = self.snapshot.fetch_volume(&volume.root)? {
                    *volume = existing;
                }
            }
            let children = node.children(source);
            source.check()?;
            let Ok(children) = children else {
                continue;
            };
            self.storage.cache_volumes(&job, &volumes)?;
            for child in children {
                db.execute(
                    "INSERT OR IGNORE INTO states(state,root,header) VALUES(?1,?2,?3)",
                    params![state.raw_cid, child.cid(), serde_json::to_vec(&child)?],
                )?;
            }
        }
        Ok(())
    }
}
impl Drop for ConnectionContent {
    fn drop(&mut self) {
        for job in self
            .original_jobs
            .iter()
            .chain(std::iter::once(&self.read_job))
            .chain(self.state_jobs.get_mut().unwrap().iter())
        {
            let _ = self.storage.release_cached_volumes(job);
        }
    }
}
impl NodeStorage {
    /// Adopt only the post-state of an actually admitted execution. Worker
    /// preparation never grants child facts or promotes speculative child state.
    pub(super) fn promote_connection_state(
        &self,
        state: &str,
        block_cid: &str,
        path: &[String],
    ) -> Result<(), StoreError> {
        let scopes = self.retention_scopes();
        let target = match self.retention_policy().state {
            StateRetention::All => scopes.chain.clone(),
            StateRetention::Tip => scopes.executed(path),
            StateRetention::None => scopes.state(path),
        };
        self.copy_connection_state(state, &target)?;
        self.copy_state_scope(&scopes.prepared(state, block_cid, path), &target)
    }
    /// Preserve a prepared child candidate as read content only. This does not
    /// advance execution state or grant admission authority.
    pub(crate) fn retain_prepared_state(
        &self,
        state: &str,
        block_cid: &str,
        path: &[String],
    ) -> Result<(), StoreError> {
        path_key(path)?;
        let _turn = self.turn.lock().unwrap();
        let scopes = self.retention_scopes();
        let target = if self.retention_policy().state == StateRetention::All {
            scopes.chain.clone()
        } else {
            scopes.reads(block_cid, path)
        };
        self.copy_connection_state(state, &target)?;
        if self.retention_policy().state != StateRetention::All {
            self.copy_connection_state(state, &scopes.prepared(state, block_cid, path))?;
        }
        Ok(())
    }
    pub(super) fn copy_state_scope(&self, source: &str, target: &str) -> Result<(), StoreError> {
        let mut after = String::new();
        loop {
            let roots = self.broker.retained_roots_page(source, &after, 256)?;
            let Some(last) = roots.last().cloned() else {
                break;
            };
            self.broker.merge_retained_roots(target, &roots)?;
            self.retained_again(target, &roots);
            after = last;
        }
        Ok(())
    }
    pub(super) fn release_prepared_states(
        &self,
        cid: &str,
        path: &[String],
    ) -> Result<(), StoreError> {
        for scope in self
            .broker
            .retained_scopes(&self.retention_scopes().prepared_block_prefix(cid, path))?
        {
            let mut after = String::new();
            loop {
                let roots = self.broker.retained_roots_page(&scope, &after, 256)?;
                let Some(last) = roots.last().cloned() else {
                    break;
                };
                self.release_after_fact_commit(&scope, &roots)?;
                after = last;
            }
        }
        Ok(())
    }
    fn copy_connection_state(&self, state: &str, target: &str) -> Result<(), StoreError> {
        let prefix = format!(
            "{}:fetch:connection-state:{state}:",
            self.retention_scopes().chain
        );
        let mut after = String::new();
        loop {
            let roots = self
                .broker
                .retained_prefix_roots_page(&prefix, &after, 256)?;
            let Some(last) = roots.last().cloned() else {
                break;
            };
            self.broker.merge_retained_roots(target, &roots)?;
            self.retained_again(target, &roots);
            after = last;
        }
        Ok(())
    }
}

/// Header links are canonical {rawCID: ...} wrappers. Complete Volume manifests
/// already contain their ordinary links; only external boundaries are queued.
/// Block predecessors are excluded so this does not walk chain history.
fn references(volume: &SerializedVolume) -> Result<BTreeSet<String>, StoreError> {
    fn walk(value: &CborValue, refs: &mut BTreeSet<String>) {
        match value {
            CborValue::Map(entries) => {
                let block = ["height", "transactions", "postState", "spec"]
                    .iter()
                    .all(|name| {
                        entries
                            .iter()
                            .any(|(key, _)| matches!(key,CborValue::Text(key) if key == name))
                    });
                for (key, value) in entries {
                    if matches!(key,CborValue::Text(key) if block && key == "parent") {
                        continue;
                    }
                    if matches!(key,CborValue::Text(key) if key == "rawCID" || key == "moduleCID") {
                        if let CborValue::Text(cid) = value
                            && lattice_cashew::canonical_cid(cid).is_ok()
                        {
                            refs.insert(cid.clone());
                        }
                    } else {
                        walk(value, refs);
                    }
                }
            }
            CborValue::Array(entries) => {
                for value in entries {
                    walk(value, refs);
                }
            }
            _ => {}
        }
    }
    let mut refs = BTreeSet::new();
    for bytes in volume.entries.values() {
        // Opaque policy modules have no typed DAG links. Authenticated CBOR
        // volumes are canonical; decoding does not consult untrusted I/O.
        if let Ok(value) = cbor::decode_value(bytes) {
            walk(&value, &mut refs);
        }
    }
    refs.retain(|cid| !volume.entries.contains_key(cid));
    Ok(refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_cashew::{Volume, volume::serialize_recursive};
    fn execution_block(
        lazy: &lattice_primitives::LatticeStateHeader,
        storage: &NodeStorage,
    ) -> Block {
        let rows: serde_json::Value = serde_json::from_str(include_str!(
            "../../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
        ))
        .unwrap();
        let entries: BTreeMap<String, Vec<u8>> =
            serde_json::from_value(rows[0]["entries"].clone()).unwrap();
        let verified = lattice_validation::validate_root_genesis(
            &BlockHeader::from_cid(rows[0]["cid"].as_str().unwrap()).unwrap(),
            &entries,
            i64::MAX,
        )
        .unwrap();
        let mut block = verified.header().node.as_deref().unwrap().clone();
        // Exercise the real persistence hook with an unresolved output;
        // its ordinary serialization deliberately omits every state Volume.
        block.post_state = lazy.resolve(storage, None).unwrap();
        block.transactions =
            lattice_cashew::Header::new(lattice_cashew::MerkleDictionary::default()).unwrap();
        block.children = lattice_cashew::Header::new(BTreeMap::new()).unwrap();
        block
    }
    #[test]
    fn state_members_without_boundary_manifests_are_restored_and_adopted_per_level() {
        for (policy, swept, fail, prepared, switch) in [
            StateRetention::All,
            StateRetention::Tip,
            StateRetention::None,
        ]
        .into_iter()
        .flat_map(|p| {
            [
                (p, false, false, false, false),
                (p, true, false, false, false),
                (p, true, true, false, false),
                (p, true, false, true, false),
                (p, true, false, true, true),
            ]
        }) {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(
                NodeStorage::open_with_retention(
                    dir.path(),
                    "genesis",
                    "chain",
                    RetentionPolicy {
                        state: policy,
                        bodies: BodyRetention::None,
                    },
                )
                .unwrap(),
            );
            let mut state = lattice_primitives::LatticeState::empty().unwrap();
            let mut accounts = lattice_primitives::AccountState::default();
            let mut deposits = lattice_primitives::DepositState::default();
            let mut general = lattice_primitives::GeneralState::default();
            let mut receipts = lattice_primitives::ReceiptState::default();
            for i in 0..100 {
                let key = format!("entry-{i:03}");
                accounts = accounts.inserting(&key, i).unwrap();
                deposits = deposits.inserting(&key, i + 1000).unwrap();
                general = general.inserting(&key, format!("value-{i}")).unwrap();
                receipts = receipts.inserting(&key, format!("receipt-{i}")).unwrap();
            }
            state.account_state = Volume::new(accounts).unwrap();
            state.deposit_state = Volume::new(deposits).unwrap();
            state.general_state = Volume::new(general).unwrap();
            state.receipt_state = Volume::new(receipts).unwrap();
            let state = Volume::new(state).unwrap();
            let volumes = serialize_recursive(&state, &()).unwrap();
            let roots: Vec<_> = volumes.iter().map(|v| v.root.clone()).collect();
            assert!(
                roots.len() > 256,
                "fixture must cross promotion page boundary"
            );
            // These bytes are readable as members of one complete containing
            // Volume, but the child state boundary manifests do not exist.
            let mut entries: BTreeMap<_, _> = volumes.into_iter().flat_map(|v| v.entries).collect();
            let envelope = Volume::new("containing-volume".to_owned()).unwrap();
            entries.extend(
                serialize_recursive(&envelope, &())
                    .unwrap()
                    .into_iter()
                    .flat_map(|v| v.entries),
            );
            let container = SerializedVolume::new(&envelope.raw_cid, entries);
            storage.cache_volumes("container", &[container]).unwrap();
            let snapshot = storage.preflight_snapshot().unwrap();
            let member = state.node.as_ref().unwrap().account_state.raw_cid.clone();
            assert!(snapshot.fetch_volume(&member).unwrap().is_none());
            assert!(
                snapshot
                    .fetch_data(&[member.clone()].into_iter().collect())
                    .unwrap()
                    .contains_key(&member)
            );
            let content = ConnectionContent::new(snapshot, storage.clone(), "worker", vec![]);
            if swept {
                storage.release_cached_volumes("container").unwrap();
                storage.sweep().unwrap();
                assert!(storage.fetch_volume(&state.raw_cid).is_none());
            }
            let db = Connection::open("").unwrap();
            db.execute_batch("CREATE TABLE states(state TEXT,root TEXT,header BLOB,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(state,root));").unwrap();
            let source = Sources {
                snapshot: content.snapshot(),
                failure: Mutex::new(None),
                entries: BTreeMap::new(),
            };
            let lazy = lattice_primitives::LatticeStateHeader::from_cid(&state.raw_cid).unwrap();
            content.preserve_state(&db, &source, &lazy).unwrap();
            storage.sweep().unwrap();
            for root in &roots {
                assert!(storage.fetch_volume(root).is_some());
            }
            if prepared {
                let path = vec!["Nexus".into(), "child".into()];
                let block = execution_block(&lazy, storage.as_ref());
                let cid = lattice_primitives::content_id(&block).unwrap();
                storage
                    .retain_prepared_state(&state.raw_cid, &cid, &path)
                    .unwrap();
                let scopes = storage.retention_scopes();
                assert!(
                    storage
                        .broker
                        .retained_roots(&scopes.executed(&path))
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    storage
                        .broker
                        .retained_roots(&scopes.state(&path))
                        .unwrap()
                        .is_empty()
                );
                drop(content);
                storage.release_cached_volumes("container").unwrap();
                storage.sweep().unwrap();
                for root in &roots {
                    assert!(storage.fetch_volume(root).is_some());
                }
                if switch {
                    drop(storage);
                    let reopened = NodeStorage::open_with_retention(
                        dir.path(),
                        "genesis",
                        "chain",
                        RetentionPolicy {
                            state: StateRetention::All,
                            bodies: BodyRetention::None,
                        },
                    )
                    .unwrap();
                    reopened.reconcile_read_scopes(StateRetention::All).unwrap();
                    reopened.sweep().unwrap();
                    for root in &roots {
                        assert!(
                            reopened.fetch_volume(root).is_some(),
                            "policy switch {policy:?}: {root}"
                        );
                    }
                    assert!(
                        reopened
                            .broker
                            .retained_scopes(&scopes.prepared_prefix())
                            .unwrap()
                            .is_empty()
                    );
                    assert!(
                        reopened
                            .broker
                            .retained_scopes(&scopes.reads_prefix())
                            .unwrap()
                            .is_empty()
                    );
                    continue;
                }
                let output =
                    serialize_recursive(&Volume::new(block.clone()).unwrap(), &()).unwrap();
                let level = NodeFactBatch {
                    proofs: vec![],
                    path: path.clone(),
                    facts: vec![],
                    headers: vec![],
                    volume_roots: vec![],
                    cursors: BTreeMap::new(),
                };
                storage
                    .atomic_fact_turn(|| {
                        storage.persist_executed_block(level, &output, &block, "delayed-candidate")
                    })
                    .unwrap();
                storage.release_block_reads(policy, &cid, &path).unwrap();
                storage.sweep().unwrap();
                for root in &roots {
                    assert!(
                        storage.fetch_volume(root).is_some(),
                        "delayed {policy:?}: {root}"
                    );
                }
                continue;
            }
            // A child execution adopts its own post-state, independent of root
            // post-state and temporary body/read dependency ownership.
            let block = execution_block(&lazy, storage.as_ref());
            let header = Volume::new(block.clone()).unwrap();
            let output = serialize_recursive(&header, &()).unwrap();
            let level = NodeFactBatch {
                proofs: vec![],
                path: vec!["Nexus".into(), "child".into()],
                facts: vec![],
                headers: vec![],
                volume_roots: vec![],
                cursors: BTreeMap::from([(
                    "peer".into(),
                    StreamCursor {
                        log_id: "retention-test".into(),
                        position: 1,
                    },
                )]),
            };
            let result = storage.atomic_fact_turn(|| {
                storage.persist_executed_block(level, &output, &block, "retention-test")?;
                if fail {
                    return Err(StoreError::Corrupt(
                        "injected late fact-turn failure".into(),
                    ));
                }
                Ok(())
            });
            assert_eq!(
                result.is_err(),
                fail,
                "{policy:?}, swept={swept}: {result:?}"
            );
            assert_eq!(
                storage
                    .journal()
                    .chain_cursors(&["Nexus".into(), "child".into()])
                    .unwrap()
                    .is_empty(),
                fail
            );
            drop(content);
            storage.release_cached_volumes("container").unwrap();
            storage.sweep().unwrap();
            for root in &roots {
                assert!(storage.fetch_volume(root).is_some(), "{policy:?}: {root}");
            }
        }
    }
}
