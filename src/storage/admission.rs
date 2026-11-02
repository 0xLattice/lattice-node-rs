use super::*;
use lattice_block_tree::{ChainTree, ChainUpdate, ChildBlockProof, HeaderPreparation};
use lattice_cashew::FlatDictionary;
use lattice_primitives::{Block, BlockHeader};
#[derive(Debug)]
pub enum HeaderImport {
    Applied(ChainUpdate),
    Duplicate,
}
impl NodeStorage {
    fn require_root_store(&self, tree: &ChainTree) -> Result<(), StoreError> {
        if !tree.context().is_some_and(|c| {
            c.is_root() && c.genesis_cid() == Some(self.journal().nexus_genesis_cid.as_str())
        }) {
            return Err(StoreError::InvalidConfiguration(
                "root tree differs from storage pin".into(),
            ));
        }
        Ok(())
    }
    /// Persist the complete authenticated root genesis before making its
    /// execution and weight visible. No untrusted facts enter this path.
    pub fn import_root_genesis(
        &self,
        tree: &mut ChainTree,
        verified: &lattice_validation::VerifiedRootGenesis,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        if self.journal().nexus_genesis_cid != verified.block_cid() {
            return Err(StoreError::InvalidConfiguration(
                "root genesis differs from storage pin".into(),
            ));
        }
        let block =
            verified.header().0.node.as_deref().ok_or_else(|| {
                StoreError::Corrupt("validated genesis is not materialized".into())
            })?;
        let children = block.children.node.as_deref().ok_or_else(|| {
            StoreError::Corrupt("validated genesis index is not materialized".into())
        })?;
        let HeaderPreparation::Prepared(prepared) =
            tree.prepare_root_genesis(verified, children)?
        else {
            return Ok(HeaderImport::Duplicate);
        };
        let volumes = lattice_cashew::volume::serialize_recursive(verified.header(), &())?;
        let level = NodeFactBatch {
            path: vec!["Nexus".into()],
            facts: prepared.batches().to_vec(),
            headers: vec![StoredHeader::from_nodes(block, children)?],
            proofs: vec![],
            volume_roots: volumes.iter().map(|v| v.root.clone()).collect(),
            cursors: BTreeMap::new(),
        };
        self.persist_executed_block(level, &volumes, block, log_id)?;
        Ok(HeaderImport::Applied(tree.apply_prepared(*prepared)?))
    }
    pub fn import_root_transition(
        &self,
        tree: &mut ChainTree,
        verified: &lattice_validation::VerifiedRootTransition,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        self.require_root_store(tree)?;
        let HeaderPreparation::Prepared(prepared) = tree.prepare_root_transition(verified)? else {
            return Ok(HeaderImport::Duplicate);
        };
        let block = verified
            .header()
            .0
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("validated block not materialized".into()))?;
        let children =
            block.children.node.as_deref().ok_or_else(|| {
                StoreError::Corrupt("validated child index not materialized".into())
            })?;
        let volumes = lattice_cashew::volume::serialize_recursive(verified.header(), &())?;
        let level = NodeFactBatch {
            path: vec!["Nexus".into()],
            facts: prepared.batches().to_vec(),
            headers: vec![StoredHeader::from_nodes(block, children)?],
            proofs: vec![],
            volume_roots: volumes.iter().map(|v| v.root.clone()).collect(),
            cursors: BTreeMap::new(),
        };
        self.persist_executed_block(level, &volumes, block, log_id)?;
        Ok(HeaderImport::Applied(tree.apply_prepared(*prepared)?))
    }
    fn child_anchor(
        &self,
        tree: &ChainTree,
        parent: &ChainTree,
        block: &Block,
    ) -> Result<lattice_block_tree::VerifiedChildParentAnchor, StoreError> {
        if parent.context().is_some_and(|c| c.is_root()) {
            self.require_root_store(parent)?;
        }
        match tree.verify_child_parent_anchor(block, None) {
            Ok(anchor) => Ok(anchor),
            Err(lattice_block_tree::ParentAnchorFailure::Required(link)) => {
                let evidence = parent.prove_parent_continuity(&link).ok_or_else(|| {
                    lattice_block_tree::ParentAnchorFailure::Required(link.clone())
                })?;
                Ok(tree.verify_child_parent_anchor(block, Some(&evidence))?)
            }
            Err(error) => Err(error.into()),
        }
    }
    pub fn import_child_genesis_header(
        &self,
        tree: &mut ChainTree,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        spec: &lattice_primitives::ChainSpec,
        proof: &ChildBlockProof,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let path = tree
            .context()
            .ok_or_else(|| StoreError::InvalidConfiguration("child tree lacks context".into()))?
            .path()
            .to_vec();
        let evidence = proof.verify_securing_work(block, &path)?;
        let HeaderPreparation::Prepared(prepared) =
            tree.prepare_child_genesis_header(block, children, spec, &evidence)?
        else {
            self.persist_duplicate_proofs(
                path,
                vec![StoredProof {
                    child_cid: evidence.child_cid().into(),
                    proof: proof.clone(),
                }],
                log_id,
            )?;
            return Ok(HeaderImport::Duplicate);
        };
        let spec_header = lattice_cashew::Volume::new(spec.clone())?;
        let volumes = lattice_cashew::volume::serialize_recursive(&spec_header, &())?;
        let level = NodeFactBatch {
            path,
            facts: prepared.batches().to_vec(),
            headers: vec![StoredHeader::from_nodes(block, children)?],
            proofs: vec![StoredProof {
                child_cid: evidence.child_cid().into(),
                proof: proof.clone(),
            }],
            volume_roots: volumes.iter().map(|v| v.root.clone()).collect(),
            cursors: BTreeMap::new(),
        };
        self.persist_materialized_batch(&[level], &volumes, log_id)?;
        Ok(HeaderImport::Applied(tree.apply_prepared(*prepared)?))
    }
    pub fn import_child_genesis(
        &self,
        tree: &mut ChainTree,
        parent: &ChainTree,
        verified: &lattice_validation::VerifiedChildGenesis,
        proof: &ChildBlockProof,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let block = verified
            .header()
            .0
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("child genesis not materialized".into()))?;
        let evidence = proof.verify_securing_work(block, verified.chain_path())?;
        let anchor = self.child_anchor(tree, parent, block)?;
        let prepared = tree.prepare_child_genesis(verified, &evidence, &anchor)?;
        self.persist_child_execution(tree, verified.header(), proof, prepared, log_id)
    }
    pub fn import_child_transition(
        &self,
        tree: &mut ChainTree,
        parent: &ChainTree,
        verified: &lattice_validation::VerifiedChildTransition,
        proof: &ChildBlockProof,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let block = verified
            .header()
            .0
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("child block not materialized".into()))?;
        let evidence = proof.verify_securing_work(block, verified.chain_path())?;
        let anchor = self.child_anchor(tree, parent, block)?;
        let prepared = tree.prepare_child_transition(verified, &evidence, &anchor)?;
        self.persist_child_execution(tree, verified.header(), proof, prepared, log_id)
    }
    fn persist_child_execution(
        &self,
        tree: &mut ChainTree,
        header: &BlockHeader,
        proof: &ChildBlockProof,
        preparation: HeaderPreparation,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let HeaderPreparation::Prepared(prepared) = preparation else {
            return Ok(HeaderImport::Duplicate);
        };
        let block = header
            .0
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("child block not materialized".into()))?;
        let children = block
            .children
            .node
            .as_deref()
            .ok_or_else(|| StoreError::Corrupt("child index not materialized".into()))?;
        let volumes = lattice_cashew::volume::serialize_recursive(header, &())?;
        let level = NodeFactBatch {
            path: tree.context().unwrap().path().to_vec(),
            facts: prepared.batches().to_vec(),
            headers: vec![StoredHeader::from_nodes(block, children)?],
            proofs: vec![StoredProof {
                child_cid: header.0.raw_cid.clone(),
                proof: proof.clone(),
            }],
            volume_roots: volumes.iter().map(|v| v.root.clone()).collect(),
            cursors: BTreeMap::new(),
        };
        self.persist_executed_block(level, &volumes, block, log_id)?;
        Ok(HeaderImport::Applied(tree.apply_prepared(*prepared)?))
    }
    /// Real root header admission, with durable facts and header evidence before
    /// visible graph mutation. The exclusive tree borrow spans staging and apply.
    pub fn import_root_header(
        &self,
        tree: &mut ChainTree,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        now: i64,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        self.require_root_store(tree)?;
        let prepared = tree.prepare_root_header(block, children, now)?;
        self.persist_header_preparation(tree, block, children, prepared, vec![], log_id)
    }
    pub fn import_child_header(
        &self,
        tree: &mut ChainTree,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        proof: &ChildBlockProof,
        now: i64,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let path = tree
            .context()
            .ok_or_else(|| StoreError::InvalidConfiguration("header tree has no context".into()))?
            .path();
        let evidence = proof.verify_securing_work(block, path)?;
        let prepared = tree.prepare_child_header(block, children, &evidence, now)?;
        let stored = StoredProof {
            child_cid: evidence.child_cid().into(),
            proof: proof.clone(),
        };
        self.persist_header_preparation(tree, block, children, prepared, vec![stored], log_id)
    }
    fn persist_duplicate_proofs(
        &self,
        path: Vec<String>,
        proofs: Vec<StoredProof>,
        log_id: &str,
    ) -> Result<(), StoreError> {
        if !proofs.is_empty() {
            self.persist_materialized_batch(
                &[NodeFactBatch {
                    path,
                    facts: vec![],
                    headers: vec![],
                    proofs,
                    volume_roots: vec![],
                    cursors: BTreeMap::new(),
                }],
                &[],
                log_id,
            )?;
        }
        Ok(())
    }
    fn persist_header_preparation(
        &self,
        tree: &mut ChainTree,
        block: &Block,
        children: &FlatDictionary<BlockHeader>,
        preparation: HeaderPreparation,
        proofs: Vec<StoredProof>,
        log_id: &str,
    ) -> Result<HeaderImport, StoreError> {
        let HeaderPreparation::Prepared(prepared) = preparation else {
            self.persist_duplicate_proofs(
                tree.context()
                    .ok_or_else(|| {
                        StoreError::InvalidConfiguration("header tree has no context".into())
                    })?
                    .path()
                    .to_vec(),
                proofs,
                log_id,
            )?;
            return Ok(HeaderImport::Duplicate);
        };
        let path = tree
            .context()
            .ok_or_else(|| StoreError::InvalidConfiguration("header tree has no context".into()))?
            .path()
            .to_vec();
        let stored = StoredHeader::from_nodes(block, children)?;
        let level = NodeFactBatch {
            proofs,
            path,
            facts: prepared.batches().to_vec(),
            headers: vec![stored],
            volume_roots: vec![],
            cursors: BTreeMap::new(),
        };
        self.persist_materialized_batch(&[level], &[], log_id)?;
        Ok(HeaderImport::Applied(tree.apply_prepared(*prepared)?))
    }
}

impl NodeStorage {
    pub fn import_root_rejection(
        &self,
        tree: &mut ChainTree,
        rejection: lattice_block_tree::VerifiedRootRejection,
        log_id: &str,
    ) -> Result<ChainUpdate, StoreError> {
        self.require_root_store(tree)?;
        let prepared = rejection.into_prepared();
        let facts = prepared.batches().to_vec();
        let mut next = tree.clone();
        // A result may arrive after another graph turn: reject stale bases before
        // writing their facts, rather than discovering staleness after commit.
        let update = next.apply_prepared(prepared)?;
        self.persist_materialized_batch(
            &[NodeFactBatch {
                path: vec!["Nexus".into()],
                facts,
                headers: vec![],
                proofs: vec![],
                volume_roots: vec![],
                cursors: BTreeMap::new(),
            }],
            &[],
            log_id,
        )?;
        *tree = next;
        Ok(update)
    }
}

impl NodeStorage {
    pub fn import_child_rejection(
        &self,
        tree: &mut ChainTree,
        rejection: lattice_block_tree::VerifiedChildRejection,
        log_id: &str,
    ) -> Result<ChainUpdate, StoreError> {
        let path = tree
            .context()
            .filter(|c| !c.is_root())
            .ok_or_else(|| {
                StoreError::InvalidConfiguration("child rejection requires a child tree".into())
            })?
            .path()
            .to_vec();
        let prepared = rejection.into_prepared();
        let facts = prepared.batches().to_vec();
        let mut next = tree.clone();
        let update = next.apply_prepared(prepared)?;
        self.persist_materialized_batch(
            &[NodeFactBatch {
                path,
                facts,
                headers: vec![],
                proofs: vec![],
                volume_roots: vec![],
                cursors: BTreeMap::new(),
            }],
            &[],
            log_id,
        )?;
        *tree = next;
        Ok(update)
    }
}
