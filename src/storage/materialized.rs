use super::*;
use lattice_volume_broker::{
    DiskBroker, RetainedRootBroker, Roots, SerializedVolume, VolumeBroker,
};
/// Content-first persistence with configured state/body retention.
/// This is a storage executor, not network block admission.
pub struct NodeStorage {
    // Drop storage connections before releasing the directory lock.
    pub(super) store: NodeStore,
    pub(super) broker: DiskBroker,
    scope: String,
    retention: RetentionPolicy,
    pub(super) turn: turn::TurnGate,
    pub(super) turn_releases: Mutex<Option<Vec<(String, Roots)>>>,
    pub(super) turn_failed: std::sync::atomic::AtomicBool,
    _directory_lock: StorageDirectoryLock,
}
impl NodeStorage {
    pub fn open(
        directory: impl AsRef<Path>,
        genesis: impl Into<String>,
        retention_scope: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::open_with_retention(
            directory,
            genesis,
            retention_scope,
            RetentionPolicy::default(),
        )
    }
    pub fn open_with_retention(
        directory: impl AsRef<Path>,
        genesis: impl Into<String>,
        retention_scope: impl Into<String>,
        retention: RetentionPolicy,
    ) -> Result<Self, StoreError> {
        let scope = retention_scope.into();
        if scope.is_empty() || scope.contains('\0') {
            return Err(StoreError::InvalidConfiguration(
                "invalid retention scope".into(),
            ));
        }
        let directory = directory.as_ref();
        let lock = StorageDirectoryLock::acquire(directory)?;
        let broker = DiskBroker::open(directory.join("volumes.db"))?;
        let store = NodeStore::open(directory.join("state.db"), genesis)?;
        if retention.state != StateRetention::Tip {
            store.forget_state_counts()?;
        }
        Ok(Self {
            retention,
            store,
            broker,
            scope,
            turn: turn::TurnGate::new(),
            turn_releases: Mutex::new(None),
            turn_failed: std::sync::atomic::AtomicBool::new(false),
            _directory_lock: lock,
        })
    }
    /// Snapshot content under the same turn gate as content/journal commits.
    /// The caller owns this read lease until its bounded validation work ends.
    pub(crate) fn preflight_snapshot(
        &self,
    ) -> Result<lattice_volume_broker::DiskReadSnapshot, StoreError> {
        let _turn = self.turn.lock().unwrap();
        Ok(self.broker.read_snapshot()?)
    }
    pub fn retention_policy(&self) -> RetentionPolicy {
        self.retention
    }
    pub fn journal(&self) -> &NodeStore {
        &self.store
    }
    pub fn fetch_volume(&self, root: &str) -> Option<SerializedVolume> {
        self.broker.fetch_volume_local(root)
    }
    pub fn retained_roots(&self) -> Result<Roots, StoreError> {
        Ok(self.broker.retained_roots(&self.scope)?)
    }
    /// Input facts must already be locally authenticated. Supplied volumes commit
    /// in one CAS transaction, their roots are retained, then all levels' facts
    /// commit in one journal transaction. A failure never reports success.
    /// Content from a failed final journal transaction may remain safely retained.
    pub fn persist_materialized_batch(
        &self,
        levels: &[NodeFactBatch],
        volumes: &[SerializedVolume],
        log_id: &str,
    ) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        self.broker.store_volumes_local(volumes)?;
        let roots = volumes.iter().map(|v| v.root.clone()).collect();
        self.broker.merge_retained_roots(&self.scope, &roots)?;
        self.store.stage_node_facts(levels, log_id)
    }
    /// Sweep and content-first writes share a lock: new volumes cannot be swept
    /// between their CAS commit and root retention.
    pub fn sweep(&self) -> Result<usize, StoreError> {
        let _turn = self.turn.lock().unwrap();
        Ok(self.broker.sweep()?)
    }
}

/// Local CAS adapter for composed validators. Missing/corrupt rows remain
/// availability failures and are never translated into invalid block verdicts.
impl lattice_cashew::ContentSource for NodeStorage {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        self.broker
            .fetch_data_local(&[cid.to_owned()].into_iter().collect())
            .remove(cid)
            .ok_or_else(|| lattice_cashew::Error::MissingContent(cid.into()))
    }
}

impl NodeStorage {
    /// Content and live pins precede the atomic journal delta. A failed journal
    /// may leave safely pinned content; the same delta can be retried.
    pub fn persist_pool_delta(
        &self,
        delta: &crate::core::mining::PoolDelta,
    ) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        let scope = format!("{}:mempool", self.scope);
        let mut volumes = BTreeMap::new();
        let mut roots = Roots::new();
        for item in delta.added.iter().chain(&delta.journaled) {
            let volume = lattice_cashew::Volume::new(item.transaction.clone())?;
            if volume.0.raw_cid != item.cid {
                return Err(StoreError::InvalidConfiguration(
                    "pool content CID mismatch".into(),
                ));
            }
            // Transactions have one Volume boundary: envelope and inline body.
            let serialized = lattice_cashew::volume::serialize_boundary(&volume, &())?;
            roots.insert(item.cid.clone());
            volumes.insert(item.cid.clone(), serialized);
        }
        self.broker
            .store_volumes_local(&volumes.into_values().collect::<Vec<_>>())?;
        self.broker.merge_retained_roots(&scope, &roots)?;
        self.retained_again(&scope, &roots);
        self.store.apply_pool_journal(delta)?;
        self.release_after_fact_commit(&scope, &delta.removed.iter().cloned().collect())?;
        Ok(())
    }
    pub fn live_mempool_roots(&self) -> Result<Roots, StoreError> {
        Ok(self
            .broker
            .retained_roots(&format!("{}:mempool", self.scope))?)
    }
    pub fn restored_mempool_items(
        &self,
    ) -> Result<Vec<crate::core::mempool::MempoolItem>, StoreError> {
        use lattice_cashew::ContentSource;
        let mut result = Vec::new();
        for record in self.store.local_mempool_transactions()? {
            let bytes = self.fetch(&record.transaction_cid)?;
            lattice_cashew::verify_content_address(&record.transaction_cid, &bytes)?;
            let mut transaction: lattice_primitives::Transaction =
                lattice_cashew::cbor::decode(&bytes)?;
            transaction.body = transaction.body.resolve(self, None)?;
            let added_at = record
                .added_at
                .checked_mul(1000)
                .ok_or_else(|| StoreError::Corrupt("pool timestamp overflow".into()))?;
            result.push(crate::core::mempool::MempoolItem {
                cid: record.transaction_cid,
                transaction,
                added_at,
            });
        }
        Ok(result)
    }
}

impl NodeStorage {
    /// At startup only local journal rows survive as live pool roots. Peer and
    /// public-submitted entries were volatile. Other held-state scopes remain.
    pub fn reconcile_mempool_roots(&self) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        if self.turn_releases.lock().unwrap().is_some() {
            return Err(StoreError::InvalidConfiguration(
                "mempool reconciliation is startup-only, outside fact turns".into(),
            ));
        }
        let roots = self
            .store
            .local_mempool_transactions()?
            .into_iter()
            .map(|r| r.transaction_cid)
            .collect();
        self.broker
            .advance_retained_roots(&format!("{}:mempool", self.scope), &roots)?;
        Ok(())
    }
}

impl NodeStorage {
    /// Explicit bounded-job cache scope, separate from durable held state. The
    /// host releases it on job completion/cancellation; CID membership validates
    /// here, while protocol closure/validity is decided by the actual validator.
    pub fn cache_volumes(&self, job: &str, volumes: &[SerializedVolume]) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        self.broker.store_volumes_local(volumes)?;
        self.broker.merge_retained_roots(
            &format!("{}:fetch:{job}", self.scope),
            &volumes.iter().map(|v| v.root.clone()).collect(),
        )?;
        Ok(())
    }
    /// Startup discards incomplete job leases. Durable journal and local-pool
    /// scopes own their content independently, so crash debris grants no facts.
    pub fn reconcile_cached_volumes(&self) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        for scope in self
            .broker
            .retained_scopes(&format!("{}:fetch:", self.scope))?
        {
            self.release_cached_scope(&scope)?;
        }
        Ok(())
    }
    pub fn release_cached_volumes(&self, job: &str) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        let scope = format!("{}:fetch:{job}", self.scope);
        self.release_cached_scope(&scope)
    }
    fn release_cached_scope(&self, scope: &str) -> Result<(), StoreError> {
        let mut after = String::new();
        loop {
            let roots = self.broker.retained_roots_page(scope, &after, 256)?;
            let Some(last) = roots.last().cloned() else {
                break;
            };
            self.broker.release_retained_roots(scope, &roots)?;
            after = last;
        }
        Ok(())
    }
    pub fn serving_volume(&self, root: &str) -> Result<Option<SerializedVolume>, StoreError> {
        if let Some(volume) = self.fetch_volume(root) {
            return Ok(Some(volume));
        }
        Ok(self
            .store
            .child_index_bytes(root)?
            .filter(|bytes| lattice_cashew::validate_entry(root, bytes).is_ok())
            .map(|bytes| SerializedVolume::new(root, BTreeMap::from([(root.into(), bytes)]))))
    }
    pub fn serving_bundle(&self, root: &str) -> Result<Vec<String>, StoreError> {
        for import in self.store.staged_imports(None)?.into_iter().rev() {
            if import.volume_roots.iter().any(|r| r == root) {
                let mut roots = vec![root.into()];
                roots.extend(import.volume_roots.into_iter().filter(|r| r != root));
                return Ok(roots);
            }
        }
        Ok(vec![root.into()])
    }
}

impl NodeStorage {
    pub fn operator_pins(&self) -> Result<Roots, StoreError> {
        Ok(self
            .broker
            .retained_roots(&format!("{}:operator", self.scope))?)
    }
    pub fn pin_volume(
        &self,
        root: &str,
        entries: Option<BTreeMap<String, Vec<u8>>>,
    ) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        if self.broker.fetch_volume_local(root).is_none() {
            let entries =
                entries.ok_or_else(|| StoreError::Corrupt("missingMaterializedVolume".into()))?;
            self.broker
                .store_volumes_local(&[SerializedVolume::new(root, entries)])?;
        }
        self.broker.merge_retained_roots(
            &format!("{}:operator", self.scope),
            &[root.into()].into_iter().collect(),
        )?;
        Ok(())
    }
    pub fn unpin_volume(&self, root: &str) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        let scope = format!("{}:operator", self.scope);
        if self.broker.retained_roots(&scope)?.contains(root) {
            self.broker
                .release_retained_roots(&scope, &[root.into()].into_iter().collect())?;
        }
        Ok(())
    }
}

impl NodeStorage {
    /// Replace a job/configuration lease after all supplied content validates.
    /// Storage and scope replacement share the sweep lock; a failed replacement
    /// preserves the old retained set and may leave only unpinned CAS debris.
    pub fn replace_cached_volumes(
        &self,
        job: &str,
        volumes: &[SerializedVolume],
    ) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        self.broker.store_volumes_local(volumes)?;
        self.broker.advance_retained_roots(
            &format!("{}:fetch:{job}", self.scope),
            &volumes.iter().map(|v| v.root.clone()).collect(),
        )?;
        Ok(())
    }
    pub(super) fn retention_scopes(&self) -> RetentionScopes {
        RetentionScopes {
            chain: self.scope.clone(),
        }
    }
    fn retain_held_locked(&self, scope: &str, roots: &Roots) -> Result<(), StoreError> {
        let held = roots
            .iter()
            .filter(|root| self.broker.fetch_volume_local(root).is_some())
            .cloned()
            .collect();
        self.broker.merge_retained_roots(scope, &held)?;
        self.retained_again(scope, &held);
        Ok(())
    }
    /// Body roots must all exist; state/spec read roots that are locally absent
    /// are omitted, as in Swift retainHeld. Caller has already stored content.
    pub fn retain_stored_content(
        &self,
        policy: StateRetention,
        roots: &Roots,
        body: &Roots,
        block: &str,
        path: &[String],
    ) -> Result<(), StoreError> {
        path_key(path)?;
        let _turn = self.turn.lock().unwrap();
        let scopes = self.retention_scopes();
        self.broker
            .merge_retained_roots(&scopes.body(block, path), body)?;
        let reads = roots.difference(body).cloned().collect();
        let scope = if policy == StateRetention::All {
            scopes.chain.clone()
        } else {
            scopes.reads(block, path)
        };
        self.retain_held_locked(&scope, &reads)
    }
    /// Pin execution outputs before their journal transaction. `None` retains
    /// only the most recent execution's locally held output set; `Tip` keeps a
    /// temporary executed scope until keep_tip_state has committed its counts.
    /// Specs/modules/constants always belong to the chain scope separately.
    pub fn retain_execution_content(
        &self,
        policy: StateRetention,
        path: &[String],
        states: &Roots,
        constants: &Roots,
    ) -> Result<(), StoreError> {
        path_key(path)?;
        let _turn = self.turn.lock().unwrap();
        let scopes = self.retention_scopes();
        self.retain_held_locked(&scopes.chain, constants)?;
        match policy {
            StateRetention::All => self.retain_held_locked(&scopes.chain, states)?,
            StateRetention::Tip => self.retain_held_locked(&scopes.executed(path), states)?,
            StateRetention::None if !states.is_empty() => {
                let scope = scopes.state(path);
                let previous = self.broker.retained_roots(&scope)?;
                self.retain_held_locked(&scope, states)?;
                self.release_after_fact_commit(
                    &scope,
                    &previous.difference(states).cloned().collect(),
                )?;
            }
            StateRetention::None => {}
        }
        Ok(())
    }
    /// Pins new nodes, commits durable counts/tip atomically, then releases old
    /// nodes and temporary execution pins. A crash can leave extra pins, never
    /// committed counts whose newly entering content was not first retained.
    pub fn keep_tip_state(&self, tip: &str, path: &[String]) -> Result<(), StoreError> {
        path_key(path)?;
        let _turn = self.turn.lock().unwrap();
        let last = self.store.state_tip(path)?;
        if last.as_deref() == Some(tip) {
            return Ok(());
        }
        let Some(fact) = self.store.block_fact(tip, path)? else {
            return Ok(());
        };
        let root = lattice_primitives::LatticeStateHeader::from_cid(fact.post_state_cid)?;
        let old = last
            .as_deref()
            .map(|cid| self.store.block_fact(cid, path))
            .transpose()?
            .flatten()
            .map(|fact| lattice_primitives::LatticeStateHeader::from_cid(fact.post_state_cid))
            .transpose()?;
        let plan = plan_tip_state(&root, old.as_ref(), self, |cid| {
            self.store.state_count(cid, path)
        })?;
        let scopes = self.retention_scopes();
        let scope = scopes.state(path);
        self.retain_held_locked(&scope, &plan.entering.iter().cloned().collect())?;
        let mut gone: Roots = self
            .store
            .move_state_counts(path, tip, &plan.changes)?
            .left
            .into_iter()
            .collect();
        if last.is_none() {
            let counted: Roots = plan
                .changes
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(cid, _)| cid.clone())
                .collect();
            gone.extend(
                self.broker
                    .retained_roots(&scope)?
                    .difference(&counted)
                    .cloned(),
            );
        }
        self.broker.release_retained_roots(&scope, &gone)?;
        let executed_scope = scopes.executed(path);
        let executed = self.broker.retained_roots(&executed_scope)?;
        self.broker
            .release_retained_roots(&executed_scope, &executed)?;
        Ok(())
    }
    pub fn release_block_reads(
        &self,
        policy: StateRetention,
        block: &str,
        path: &[String],
    ) -> Result<(), StoreError> {
        if policy == StateRetention::All {
            return Ok(());
        }
        path_key(path)?;
        let _turn = self.turn.lock().unwrap();
        let scope = self.retention_scopes().reads(block, path);
        let roots = self.broker.retained_roots(&scope)?;
        self.broker.release_retained_roots(&scope, &roots)?;
        self.release_prepared_states(block, path)?;
        Ok(())
    }
    /// Drop executed non-genesis bodies except each level's live tip. Pending
    /// bodies retain their independent scope until execution or explicit refusal.
    pub fn release_old_bodies(
        &self,
        policy: BodyRetention,
        tips: &BTreeMap<Vec<String>, String>,
    ) -> Result<(), StoreError> {
        if policy == BodyRetention::All {
            return Ok(());
        }
        let _turn = self.turn.lock().unwrap();
        let scopes = self.retention_scopes();
        for scope in self.broker.retained_scopes(&scopes.body_prefix())? {
            let Some((path, cid)) = scopes.block(&scope, false) else {
                continue;
            };
            // An absent path is dormant/unhosted, not a level with no live tip.
            // Startup restores root before applying hosted configuration; dropping
            // child bodies here would erase its latest body before rehosting.
            let Some(tip) = tips.get(&path) else {
                continue;
            };
            if tip == &cid || self.store.is_genesis(&cid, &path)? {
                continue;
            }
            if matches!(
                self.store.block_status(&cid, &path)?,
                Some(BlockStatus::Executed | BlockStatus::ExecutedAndPinned)
            ) {
                let roots = self.broker.retained_roots(&scope)?;
                self.broker.release_retained_roots(&scope, &roots)?;
            }
        }
        Ok(())
    }
    pub fn reconcile_read_scopes(&self, policy: StateRetention) -> Result<(), StoreError> {
        let _turn = self.turn.lock().unwrap();
        let scopes = self.retention_scopes();
        for scope in self.broker.retained_scopes(&scopes.prepared_prefix())? {
            let Some((path, cid)) = scopes.prepared_block(&scope) else {
                continue;
            };
            if policy == StateRetention::All
                || matches!(
                    self.store.block_status(&cid, &path)?,
                    Some(BlockStatus::Executed | BlockStatus::ExecutedAndPinned)
                )
            {
                if policy == StateRetention::All {
                    self.copy_state_scope(&scope, &scopes.chain)?;
                }
                self.release_prepared_states(&cid, &path)?;
            }
        }
        for scope in self.broker.retained_scopes(&scopes.reads_prefix())? {
            let Some((path, cid)) = scopes.block(&scope, true) else {
                continue;
            };
            if policy == StateRetention::All
                || matches!(
                    self.store.block_status(&cid, &path)?,
                    Some(BlockStatus::Executed | BlockStatus::ExecutedAndPinned)
                )
            {
                if policy == StateRetention::All {
                    self.copy_state_scope(&scope, &scopes.chain)?;
                }
                let roots = self.broker.retained_roots(&scope)?;
                self.broker.release_retained_roots(&scope, &roots)?;
            }
        }
        Ok(())
    }
}

impl NodeStorage {
    /// Admission supplies a fully validated execution header. Persist all bytes
    /// and required scopes under one sweep lock, then journal; only afterwards
    /// release transient reads/previous execution pins. Nested candidate bodies
    /// get their own path scopes even before those child executions complete.
    pub(super) fn persist_executed_block(
        &self,
        level: NodeFactBatch,
        volumes: &[SerializedVolume],
        block: &lattice_primitives::Block,
        log_id: &str,
    ) -> Result<(), StoreError> {
        use lattice_primitives::content_id;
        let _turn = self.turn.lock().unwrap();
        self.promote_connection_state(
            &block.post_state.raw_cid,
            &lattice_primitives::content_id(block)?,
            &level.path,
        )?;
        if self.retention == RetentionPolicy::default()
            || (level.path.len() == 1 && block.height == 0)
        {
            return self.persist_materialized_batch(&[level], volumes, log_id);
        }
        let _turn = self.turn.lock().unwrap();
        self.broker.store_volumes_local(volumes)?;
        let scopes = self.retention_scopes();
        let mut frontier = vec![(block, level.path.clone())];
        let mut all_body_roots = Roots::new();
        let mut plans = Vec::new();
        while let Some((candidate, path)) = frontier.pop() {
            let cid = content_id(candidate)?;
            let dictionary = candidate.transactions.resolve(self, None)?;
            let dictionary = dictionary
                .node
                .as_deref()
                .ok_or_else(|| {
                    StoreError::Corrupt("executed transaction index unavailable".into())
                })?
                .resolve_all(self)?;
            let mut body = Roots::from([cid.clone()]);
            body.extend(
                dictionary
                    .entries()?
                    .into_values()
                    .map(|tx| tx.raw_cid.clone()),
            );
            all_body_roots.extend(body.iter().cloned());
            let header = lattice_cashew::Volume::new(candidate.clone())?;
            let roots: Roots = lattice_cashew::volume::serialize_recursive(&header, &())?
                .into_iter()
                .map(|v| v.root)
                .collect();
            plans.push((path.clone(), cid, body, roots));
            if let Some(children) = candidate.children.node.as_deref() {
                for (directory, child) in children {
                    if let Some(child) = child.node.as_deref() {
                        let mut path = path.clone();
                        path.push(directory.clone());
                        frontier.push((child, path));
                    }
                }
            }
        }
        let state_roots: Roots =
            lattice_cashew::volume::serialize_recursive(&block.post_state, &())?
                .into_iter()
                .map(|v| v.root)
                .collect();
        let constants: Roots = lattice_cashew::volume::serialize_recursive(&block.spec, &())?
            .into_iter()
            .map(|v| v.root)
            .collect();
        let mut constants = constants;
        if let Some(spec) = block.spec.node.as_deref() {
            constants.extend(spec.wasm_policies.iter().map(|p| p.module_cid.clone()));
        }
        self.retain_held_locked(&scopes.chain, &constants)?;
        let mut execution_previous = Roots::new();
        for (path, cid, body, roots) in &plans {
            self.broker
                .merge_retained_roots(&scopes.body(cid, path), body)?;
            let reads: Roots = roots.difference(&all_body_roots).cloned().collect();
            if self.retention.state == StateRetention::All {
                self.retain_held_locked(&scopes.chain, &reads)?;
            } else if path != &level.path {
                self.retain_held_locked(&scopes.reads(cid, path), &reads)?;
            }
        }
        match self.retention.state {
            StateRetention::All => self.retain_held_locked(&scopes.chain, &state_roots)?,
            StateRetention::Tip => {
                self.retain_held_locked(&scopes.executed(&level.path), &state_roots)?
            }
            StateRetention::None => {
                let scope = scopes.state(&level.path);
                execution_previous = self.broker.retained_roots(&scope)?;
                self.retain_held_locked(&scope, &state_roots)?;
            }
        }
        // Every newly stored root must have a conservative owner before the
        // journal moves. Unclassified read content is a temporary block lease.
        let root_cid = content_id(block)?;
        let root_reads = scopes.reads(&root_cid, &level.path);
        let supplied: Roots = volumes.iter().map(|v| v.root.clone()).collect();
        self.retain_held_locked(&root_reads, &supplied)?;
        self.store
            .stage_node_facts(std::slice::from_ref(&level), log_id)?;
        // Cleanup is operational. Once the journal commits, a maintenance error
        // must not turn a valid execution into an invalid-block response.
        if self.retention.state == StateRetention::None && !state_roots.is_empty() {
            let gone = execution_previous
                .difference(&state_roots)
                .cloned()
                .collect();
            let _ = self.release_after_fact_commit(&scopes.state(&level.path), &gone);
        }
        let _ = self.release_after_fact_commit(&root_reads, &supplied);
        self.promote_connection_state(
            &block.post_state.raw_cid,
            &lattice_primitives::content_id(block)?,
            &level.path,
        )?;
        Ok(())
    }
}
