//! Root-only runtime template assembly. Building work is not block admission.
use crate::core::{
    mining::{TemplateBuild, TemplateJob},
    work::TemplateChain,
};
use lattice_cashew::{ContentSource, Header};
use lattice_primitives::*;
use lattice_validation::*;
use num_bigint::BigUint;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
#[derive(Debug, thiserror::Error)]
pub enum TemplateBuildError {
    #[error("root template cannot carry a parent or descendant recipient plan")]
    UnsupportedContext,
    #[error("invalid minimum work")]
    InvalidMinimumWork,
    #[error("timestamp overflow")]
    TimestampOverflow,
    #[error("adding children changed the selected template context")]
    ContextChanged,
    #[error("no template fits the block byte budget")]
    TooLarge,
    #[error(transparent)]
    Content(#[from] lattice_cashew::Error),
    #[error(transparent)]
    Builder(#[from] BlockBuilderError),
    #[error(transparent)]
    BlockContent(#[from] BlockContentError),
}
pub struct TemplateBuildOutput {
    pub build: TemplateBuild,
    pub refused: Vec<String>,
}
/// Easiest nonzero target representing at least this much work; inputs outside
/// 1..=2^255 are rejected, never clamped.
pub fn minimum_work_target(work: UInt256) -> Option<UInt256> {
    let w = BigUint::from_bytes_be(&work.0);
    if w == BigUint::default() || w > (BigUint::from(1u8) << 255usize) {
        return None;
    }
    let n = ((BigUint::from(1u8) << 256usize) / w) - 1u8;
    let bytes = n.to_bytes_be();
    let mut out = [0; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Some(UInt256(out))
}
pub fn template_digest(tip: &str, transactions: &[Transaction]) -> String {
    let mut cids: Vec<_> = transactions
        .iter()
        .filter_map(|tx| content_id(tx).ok())
        .collect();
    cids.sort();
    hex::encode(Sha256::digest(
        format!("tip:{tip}\nmempool:{}", cids.join(",")).as_bytes(),
    ))
}
pub(super) fn work_id(cid: String, plan: &BTreeMap<Vec<String>, UInt256>) -> String {
    if plan.is_empty() {
        return cid;
    }
    let mut lines: Vec<_> = plan
        .iter()
        .map(|(path, w)| (path.join("/"), hex::encode(w.0)))
        .collect();
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    let text = lines
        .into_iter()
        .map(|(p, w)| format!("{p}={w}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{cid}-{}",
        hex::encode(&Sha256::digest(text.as_bytes())[..16])
    )
}
fn skippable(error: &BlockBuilderError) -> bool {
    matches!(
        error,
        BlockBuilderError::ChildCommitment(_)
            | BlockBuilderError::StateGrowth
            | BlockBuilderError::Transition(_)
            | BlockBuilderError::Execution(ExecutionError::State(_) | ExecutionError::Diff(_))
    )
}
pub(super) struct Assembly<'a, S> {
    pub previous: &'a Block,
    pub source: &'a S,
    pub timestamp: i64,
    pub anchor: Option<DifficultyAnchor>,
    pub recipient: Option<&'a str>,
    pub children: &'a BTreeMap<String, Block>,
    pub parent_carrier: Option<&'a Block>,
    pub path: &'a [String],
}
impl<S: ContentSource> Assembly<'_, S> {
    fn candidate(&self, txs: &[Transaction]) -> Result<Block, BlockBuilderError> {
        // Swift invokes the withdrawal gate if any input body is unresolved,
        // even when resolving it later reveals no withdrawals. That gate also
        // validates every transaction's signatures and action shape.
        let check_withdrawals = txs.iter().any(|tx| {
            tx.body
                .node
                .as_deref()
                .is_none_or(|body| !body.withdrawal_actions.is_empty())
        });
        let candidate = BlockTemplate {
            previous: self.previous,
            transactions: txs,
            children: self.children,
            parent_chain_block: self.parent_carrier,
            timestamp: self.timestamp,
            target: None,
            next_target: None,
            nonce: 0,
            difficulty_anchor: self.anchor,
            reward_recipient: self.recipient,
        }
        .build(self.source)?
        .block;
        if check_withdrawals {
            if self.path.first().map(String::as_str) != Some("Nexus") {
                return Err(BlockBuilderError::Transition(
                    TransitionError::ParentReceipt,
                ));
            }
            let mut withdrawals = Vec::new();
            for tx in txs {
                let Some(body) = validate_transaction(tx, self.source, false)? else {
                    return Err(BlockBuilderError::Transition(
                        TransitionError::ParentReceipt,
                    ));
                };
                withdrawals.extend(body.withdrawal_actions);
            }
            if !withdrawals.is_empty() {
                if self.path.len() < 2 {
                    return Err(BlockBuilderError::Transition(
                        TransitionError::ParentReceipt,
                    ));
                }
                validate_authenticated_withdrawals(
                    &self.previous.post_state,
                    &candidate.parent_state,
                    self.path.last().expect("child directory"),
                    &withdrawals,
                    self.source,
                )?;
            }
        }
        Ok(candidate)
    }
    pub(super) fn assemble(
        &self,
        txs: &[Transaction],
        limit: usize,
    ) -> Result<Block, BlockBuilderError> {
        let mut selected = Vec::new();
        let mut candidate = self.candidate(&[])?;
        let mut pending = vec![txs];
        while selected.len() < limit {
            let Some(chunk) = pending.pop() else {
                break;
            };
            if chunk.is_empty() {
                continue;
            }
            let remaining = limit - selected.len();
            if chunk.len() > remaining {
                pending.push(&chunk[remaining..]);
                pending.push(&chunk[..remaining]);
                continue;
            }
            let mut next = selected.clone();
            next.extend_from_slice(chunk);
            match self.candidate(&next) {
                Ok(block) => {
                    selected = next;
                    candidate = block;
                }
                Err(e) if skippable(&e) => {
                    if chunk.len() > 1 {
                        let mid = chunk.len() / 2;
                        pending.push(&chunk[mid..]);
                        pending.push(&chunk[..mid]);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(candidate)
    }
}
/// Caller supplies an executed tip snapshot and epoch-fences completion. This
/// function neither marks a tip executed nor installs resulting work.
pub fn build_root_template(
    job: &TemplateJob,
    source: &impl ContentSource,
    now: i64,
    anchor: Option<DifficultyAnchor>,
) -> Result<TemplateBuildOutput, TemplateBuildError> {
    if job.request.parent_carrier.is_some() || !job.request.child_recipients.is_empty() {
        return Err(TemplateBuildError::UnsupportedContext);
    }
    for (path, w) in &job.request.minimum_work {
        if path.first().map(String::as_str) != Some("Nexus") || minimum_work_target(*w).is_none() {
            return Err(TemplateBuildError::InvalidMinimumWork);
        }
    }
    let previous = Header::<Block>::from_cid(&job.tip_cid)?.resolve(source, None)?;
    let previous = previous
        .node
        .as_deref()
        .ok_or_else(|| lattice_cashew::Error::MissingContent(job.tip_cid.clone()))?;
    let spec = previous.spec.0.resolve(source, None)?;
    let spec = spec
        .node
        .as_deref()
        .ok_or_else(|| lattice_cashew::Error::MissingContent(previous.spec.0.raw_cid.clone()))?;
    let timestamp = now.max(
        previous
            .timestamp
            .checked_add(1)
            .ok_or(TemplateBuildError::TimestampOverflow)?,
    );
    let height = previous
        .height
        .checked_add(1)
        .ok_or(BlockBuilderError::HeightOverflow)?;
    let path = vec!["Nexus".to_owned()];
    let mut accepted = Vec::new();
    let mut refused = Vec::new();
    for tx in &job.transactions {
        if spec.wasm_policies.is_empty() {
            accepted.push(tx.clone());
            continue;
        }
        let Ok(body) = tx.body.resolve(source, None) else {
            continue;
        };
        let Some(body) = body.node.as_deref() else {
            continue;
        };
        let verdict = PolicyBatch {
            bodies: std::slice::from_ref(body),
            spec,
            chain_path: &path,
            height,
            timestamp,
            scopes: None,
        }
        .verify(source);
        match verdict {
            Ok(true) => accepted.push(tx.clone()),
            Ok(false) => {
                if let Ok(cid) = content_id(tx) {
                    refused.push(cid);
                }
            }
            Err(e) if e.verdict() == PolicyFailureVerdict::Invalid => {
                if let Ok(cid) = content_id(tx) {
                    refused.push(cid);
                }
            }
            Err(_) => {}
        }
    }
    let assembly = Assembly {
        previous,
        source,
        timestamp,
        anchor,
        recipient: job.request.reward_recipient.as_deref(),
        children: &BTreeMap::new(),
        parent_carrier: None,
        path: &path,
    };
    // With no supplied children, fit searches the largest transaction allowance.
    // Failed execution chunks are bisected independently inside each probe.
    let mut low: Option<usize> = None;
    let mut high = usize::try_from(spec.max_number_of_transactions_per_block)
        .unwrap_or(usize::MAX)
        .min(accepted.len());
    let mut probe = high;
    let mut best = None;
    loop {
        let block = assembly.assemble(&accepted, probe)?;
        let fits = match resolve_block_content(&block, source, spec.max_block_size) {
            Ok(_) => true,
            Err(BlockContentError::Size(ContentSizeError::ExceedsLimit)) => false,
            Err(e) => return Err(e.into()),
        };
        if fits {
            low = Some(probe);
            best = Some(block);
            if probe == high {
                break;
            }
            probe += (high - probe).div_ceil(2);
        } else {
            if probe == 0 {
                break;
            }
            high = probe - 1;
            let start = low.map(|v| v + 1).unwrap_or(0);
            if high < start {
                break;
            }
            probe = start + (high - start) / 2;
        }
    }
    let block = best.ok_or(TemplateBuildError::TooLarge)?;
    let search_target = job
        .request
        .minimum_work
        .get(&path)
        .and_then(|w| minimum_work_target(*w))
        .map(|cap| cap.min(block.target))
        .unwrap_or(block.target);
    let chains = vec![TemplateChain {
        chain_path: path,
        target: block.target,
        height: block.height,
        reward_recipient: block.reward_recipient.clone(),
    }];
    Ok(TemplateBuildOutput {
        build: TemplateBuild {
            work_id: work_id(content_id(&block)?, &job.request.minimum_work),
            block,
            search_target,
            targets: vec![search_target],
            digest: template_digest(&job.tip_cid, &job.transactions),
            chains,
        },
        refused,
    })
}
