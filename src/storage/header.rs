use super::*;
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader};
impl StoredHeader {
    /// Serialize a content-bound header and child index. This authenticates content
    /// identity only; callers still need consensus admission before staging facts.
    pub fn from_nodes(
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
    ) -> Result<Self, StoreError> {
        let block_bytes = lattice_cashew::cbor::encode(block)?;
        let children_bytes = lattice_cashew::cbor::encode(children)?;
        let block_cid = lattice_primitives::content_id(block)?;
        let children_cid = block.children.raw_cid.clone();
        lattice_cashew::validate_entry(&children_cid, &children_bytes)?;
        Ok(Self {
            block_cid,
            block: block_bytes,
            children_cid,
            children: children_bytes,
        })
    }
}
impl NodeStore {
    /// Corrupt or unavailable evidence is not served as an authenticated header.
    pub fn header(
        &self,
        cid: &str,
    ) -> Result<Option<(Block, FlatDictionary<BlockHeader>)>, StoreError> {
        let db = self.db.lock().unwrap();
        let raw:Option<(Vec<u8>,String,Vec<u8>)>=db.query_row("SELECT h.block,h.children_cid,c.bytes FROM headers h JOIN child_indexes c ON c.cid=h.children_cid WHERE h.cid=?1",[cid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((block_bytes, children_cid, children_bytes)) = raw else {
            return Ok(None);
        };
        if lattice_cashew::validate_entry(cid, &block_bytes).is_err()
            || lattice_cashew::validate_entry(&children_cid, &children_bytes).is_err()
        {
            return Ok(None);
        }
        let Ok(block) = lattice_cashew::cbor::decode::<Block>(&block_bytes) else {
            return Ok(None);
        };
        if block.children.raw_cid != children_cid {
            return Ok(None);
        }
        let Ok(children) =
            lattice_cashew::cbor::decode::<FlatDictionary<BlockHeader>>(&children_bytes)
        else {
            return Ok(None);
        };
        Ok(Some((block, children)))
    }
    pub fn has_accepted_block(&self, cid: &str, path: &[String]) -> Result<bool, StoreError> {
        Ok(self.block_status(cid, path)?.is_some())
    }
    pub fn has_executed_block(&self, cid: &str, path: &[String]) -> Result<bool, StoreError> {
        Ok(self
            .block_status(cid, path)?
            .is_some_and(|status| status != BlockStatus::Header))
    }
}

impl NodeStore {
    /// Grow-only child directory index reconstructed from every executed block,
    /// including side branches. Header-only commitments are excluded.
    pub fn executed_child_directories(
        &self,
        path: &[String],
    ) -> Result<BTreeSet<String>, StoreError> {
        let db = self.db.lock().unwrap();
        let mut result = BTreeSet::new();
        let mut query=db.prepare("SELECT DISTINCT h.children_cid,c.bytes FROM accepted_blocks a JOIN headers h ON h.cid=a.block_cid JOIN child_indexes c ON c.cid=h.children_cid WHERE a.chain_path=?1 AND a.validated=1")?;
        for row in query.query_map([path_key(path)?], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
        })? {
            let (cid, bytes) = row?;
            lattice_cashew::validate_entry(&cid, &bytes)?;
            let children: lattice_cashew::FlatDictionary<BlockHeader> =
                lattice_cashew::cbor::decode(&bytes)?;
            result.extend(children.into_keys());
        }
        Ok(result)
    }
}
