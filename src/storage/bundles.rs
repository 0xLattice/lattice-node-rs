//! Non-authoritative block bundle cache and local `storeBlock` content walk.
//! Cache failure costs recomputation, never chain admission or recovery.
use super::*;
use lattice_cashew::{ContentSource, Header, Volume, VolumeMerkleDictionary};
use lattice_primitives::*;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

pub(super) struct BundleCache(Option<Mutex<Connection>>);
impl BundleCache {
    pub fn open(directory: &Path) -> Self {
        let open = || -> rusqlite::Result<Connection> {
            let db = Connection::open(directory.join("volume-bundles.db"))?;
            db.busy_timeout(Duration::from_millis(100))?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE IF NOT EXISTS volume_bundles(root TEXT PRIMARY KEY,roots BLOB NOT NULL) WITHOUT ROWID;")?;
            Ok(db)
        };
        Self(open().ok().map(Mutex::new))
    }
    pub fn get(&self, root: &str) -> Option<Vec<String>> {
        let db = self.0.as_ref()?.lock().ok()?;
        let data: Vec<u8> = db
            .query_row(
                "SELECT roots FROM volume_bundles WHERE root=?1",
                [root],
                |r| r.get(0),
            )
            .ok()?;
        let roots: Vec<String> = serde_json::from_slice(&data).ok()?;
        // Cache content is not an admission authority. Malformed rows are misses.
        (roots.first().is_some_and(|r| r == root)
            && roots
                .iter()
                .all(|r| lattice_cashew::canonical_cid(r).is_ok())
            && roots.iter().collect::<BTreeSet<_>>().len() == roots.len())
        .then_some(roots)
    }
    pub fn put(&self, root: &str, roots: &[String]) {
        if let Some(db) = &self.0
            && let Ok(db) = db.lock()
            && let Ok(data) = serde_json::to_vec(roots)
        {
            let _ = db.execute(
                "INSERT OR REPLACE INTO volume_bundles(root,roots) VALUES(?1,?2)",
                params![root, data],
            );
        }
    }
}
fn add<V: lattice_cashew::DagNode + Clone + Serialize>(
    value: &Volume<V>,
    roots: &mut BTreeSet<String>,
) -> Result<(), StoreError> {
    roots.extend(
        lattice_cashew::volume::serialize_recursive(value, &())?
            .into_iter()
            .map(|v| v.root),
    );
    Ok(())
}
fn targeted<V: Clone + Serialize + DeserializeOwned + lattice_cashew::DagNode>(
    header: &Volume<VolumeMerkleDictionary<V>>,
    keys: &BTreeSet<String>,
    source: &impl ContentSource,
    roots: &mut BTreeSet<String>,
) -> Result<(), StoreError> {
    if keys.is_empty() {
        return Ok(());
    }
    let mut resolved = header.0.resolve(source, None)?;
    let mut tree = resolved.node.as_deref().unwrap().clone();
    for key in keys {
        tree = tree.resolve_key(key, source)?;
    }
    resolved.node = Some(Arc::new(tree));
    add(&Volume(resolved), roots)
}
/// Exact root set selected by Swift Block.storeBlock. No execution/verdict and
/// no fetching of independent parent, post-state or child-block Volumes.
pub(super) fn walk(root: &str, source: &impl ContentSource) -> Result<Vec<String>, StoreError> {
    let header = Header::<Block>::from_cid(root)?.resolve(source, None)?;
    let block = header.node.as_deref().unwrap();
    let spec = block.spec.0.resolve(source, None)?;
    let spec_node = spec.node.as_deref().unwrap();
    let content =
        lattice_validation::resolve_block_content(block, source, spec_node.max_block_size)
            .map_err(|e| StoreError::Corrupt(format!("bundle content unavailable: {e}")))?;
    let mut roots = BTreeSet::from([root.to_owned(), spec.raw_cid.clone()]);
    // Only transaction Volumes are recursively selected here; all other nested
    // block links stay independent. The block boundary itself was read above.
    for (_, transaction) in content
        .block
        .transactions
        .node
        .as_deref()
        .unwrap()
        .entries()?
    {
        add(&transaction, &mut roots)?;
    }
    let prev = block.prev_state.0.resolve(source, None)?;
    roots.insert(prev.raw_cid.clone());
    let prev = prev.node.as_deref().unwrap();
    let (mut accounts, mut general, mut deposits, mut receipts, mut parent_receipts) = (
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    if let Some(recipient) = &block.reward_recipient {
        accounts.insert(recipient.clone());
    }
    for transaction in &content.transactions {
        let body = transaction.body.node.as_deref().unwrap();
        accounts.extend(body.account_actions.iter().map(|a| a.owner.clone()));
        if let Some(signer) = body.signers.first() {
            accounts.insert(format!("{}{signer}", lattice_validation::NONCE_PREFIX));
        }
        general.extend(body.actions.iter().map(|a| a.key.clone()));
        for a in &body.deposit_actions {
            deposits.insert(
                DepositKey {
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce,
                }
                .to_string(),
            );
        }
        for a in &body.withdrawal_actions {
            deposits.insert(
                DepositKey {
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce,
                }
                .to_string(),
            );
            if let Some(directory) = body.chain_path.last() {
                parent_receipts.insert(
                    ReceiptKey {
                        directory: directory.clone(),
                        demander: a.demander.clone(),
                        amount_demanded: a.amount_demanded,
                        nonce: a.nonce,
                    }
                    .storage_key(),
                );
            }
        }
        for a in &body.receipt_actions {
            receipts.insert(
                ReceiptKey {
                    directory: a.directory.clone(),
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce,
                }
                .storage_key(),
            );
            accounts.insert(a.withdrawer.clone());
            accounts.insert(a.demander.clone());
        }
    }
    targeted(&prev.account_state, &accounts, source, &mut roots)?;
    targeted(&prev.general_state, &general, source, &mut roots)?;
    targeted(&prev.deposit_state, &deposits, source, &mut roots)?;
    targeted(&prev.receipt_state, &receipts, source, &mut roots)?;
    if !parent_receipts.is_empty() {
        let parent = block.parent_state.0.resolve(source, None)?;
        roots.insert(parent.raw_cid.clone());
        targeted(
            &parent.node.as_deref().unwrap().receipt_state,
            &parent_receipts,
            source,
            &mut roots,
        )?;
    }
    for policy in &spec_node.wasm_policies {
        let module =
            Header::<WasmPolicyModule>::from_cid(&policy.module_cid)?.resolve(source, None)?;
        roots.insert(module.raw_cid.clone());
    }
    if block.height == 0 {
        add(&Volume::new(LatticeState::empty()?)?, &mut roots)?;
    }
    roots.remove(root);
    Ok(std::iter::once(root.to_owned()).chain(roots).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn worker_records_bundle_before_state_pruning_and_cache_remains_optional() {
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../tests/serving-bundle-swift.json")).unwrap();
        let row = rows.iter().find(|r| r["name"] == "combined").unwrap();
        let root = row["root"].as_str().unwrap();
        let volumes: Vec<lattice_cashew::SerializedVolume> =
            serde_json::from_value(row["volumes"].clone()).unwrap();
        let source: BTreeMap<String, Vec<u8>> =
            volumes.iter().flat_map(|v| v.entries.clone()).collect();
        let expected: Vec<String> = serde_json::from_value(row["swift"].clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "fixture", "test").unwrap());
        storage.cache_volumes("fixture", &volumes).unwrap();
        let block: Block = lattice_cashew::cbor::decode(&source[root]).unwrap();
        let block = lattice_validation::resolve_block_content(&block, &source, 1_000_000)
            .unwrap()
            .block;
        let header = Volume::new(block).unwrap();
        let lease = ConnectionContent::new(
            storage.preflight_snapshot().unwrap(),
            storage.clone(),
            root,
            vec![],
        );
        lease.preserve(&header).unwrap();
        drop(lease);
        storage.release_cached_volumes("fixture").unwrap();
        storage.sweep().unwrap();
        assert!(storage.fetch_volume(root).is_none());
        assert_eq!(
            storage.serving_bundle(root).unwrap(),
            expected,
            "worker recorded before first serving call"
        );
        drop(storage);
        let reopened = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
        assert_eq!(reopened.serving_bundle(root).unwrap(), expected);
        drop(reopened);
        std::fs::remove_file(dir.path().join("volume-bundles.db")).unwrap();
        let reopened = NodeStorage::open(dir.path(), "fixture", "test").unwrap();
        assert_eq!(
            reopened.serving_bundle(root).unwrap(),
            vec![root],
            "lost cache falls back when historical content is pruned"
        );
    }
}
