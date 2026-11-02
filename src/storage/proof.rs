use super::*;
use lattice_block_tree::ChildBlockProof;
#[derive(Debug, Clone)]
pub struct StoredProof {
    pub child_cid: String,
    pub proof: ChildBlockProof,
}
fn decode_proof(child: &str, root: &str, bytes: &[u8], path: &[String]) -> Option<ChildBlockProof> {
    if lattice_primitives::canonical_cid(child).as_deref() != Some(child)
        || lattice_primitives::canonical_cid(root).as_deref() != Some(root)
    {
        return None;
    }
    let proof = ChildBlockProof::deserialize(bytes)?;
    if proof.root_cid != root || proof.directory_path != path[1..] {
        return None;
    }
    Some(proof)
}
impl NodeStore {
    /// Typed proof journal reads. Bad rows are omitted and removed while holding
    /// the database lock, so a repaired copy cannot race the cleanup.
    pub fn saved_proofs(
        &self,
        path: &[String],
        children: &[String],
    ) -> Result<BTreeMap<String, Vec<ChildBlockProof>>, StoreError> {
        if path.len() < 2 || path_key(path).is_err() {
            return Ok(BTreeMap::new());
        }
        let chain = path_key(path)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut result = BTreeMap::<String, Vec<ChildBlockProof>>::new();
        let mut bad = vec![];
        for chunk in children.chunks(500) {
            let marks = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT rowid,child,root,bytes FROM child_proofs WHERE chain=? AND child IN ({marks}) ORDER BY child,root"
            );
            let mut args = vec![chain.clone()];
            args.extend_from_slice(chunk);
            let mut stmt = tx.prepare(&sql)?;
            for row in stmt.query_map(rusqlite::params_from_iter(args), |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                ))
            })? {
                let (id, child, root, bytes) = row?;
                if let Some(proof) = decode_proof(&child, &root, &bytes, path) {
                    result.entry(child).or_default().push(proof)
                } else {
                    bad.push(id);
                }
            }
        }
        for id in bad {
            tx.execute(
                "DELETE FROM child_proofs WHERE rowid=?1 AND chain=?2",
                params![id, chain],
            )?;
        }
        tx.commit()?;
        Ok(result)
    }
}
