//! Local hosted-child template construction. These inputs and outputs confer no
//! execution or securing-work authority; the host must fence every level's epoch.
use super::template::{Assembly, TemplateBuildError};
use lattice_cashew::ContentSource;
use lattice_primitives::*;
use lattice_validation::*;
use std::collections::{BTreeMap, BTreeSet};

/// The actor selects an executed ancestor whose parent anchor can advance to
/// the carrier's entering state. A weighed but unexecuted root must have neither
/// `previous` nor `genesis_spec`; it waits instead of minting another genesis.
#[derive(Clone, Debug)]
pub struct ChildTemplateContext {
    /// Original act-on tip, before selecting an older anchor-compatible tip.
    pub tip_cid: Option<String>,
    pub best_header_tip: String,
    pub previous: Option<BlockHeader>,
    pub anchor: Option<DifficultyAnchor>,
    pub transactions: Vec<Transaction>,
    pub genesis_spec: Option<ChainSpec>,
    pub genesis_premine_recipient: Option<String>,
    pub genesis_transactions: Vec<Transaction>,
    pub genesis_target: UInt256,
    pub below_act_on_tip: bool,
}

pub struct LevelTemplateInput<'a> {
    pub path: &'a [String],
    pub previous: &'a Block,
    pub transactions: &'a [Transaction],
    pub children: &'a BTreeMap<String, Block>,
    pub hosted: &'a BTreeSet<String>,
    pub parent_carrier: Option<&'a Block>,
    pub timestamp: i64,
    pub recipient: Option<&'a str>,
    pub anchor: Option<DifficultyAnchor>,
    pub spec: &'a ChainSpec,
}

fn fits(
    block: &Block,
    source: &impl ContentSource,
    limit: i64,
) -> Result<bool, TemplateBuildError> {
    match resolve_block_content(block, source, limit) {
        Ok(_) => Ok(true),
        Err(BlockContentError::Size(ContentSizeError::ExceedsLimit)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Swift's transaction-count bisection and height-rotated child fitting. Pool
/// commitments into hosted directories are omitted even if their own child
/// cannot be carried this round. Content-only children are never fetched here.
pub fn fit_level(
    input: &LevelTemplateInput<'_>,
    source: &impl ContentSource,
) -> Result<Block, TemplateBuildError> {
    let pooled: Vec<_> = input
        .transactions
        .iter()
        .filter(|tx| {
            !tx.body.node.as_deref().is_some_and(|body| {
                body.actions.iter().any(|a| {
                    a.new_value.is_some()
                        && a.key
                            .strip_prefix("child/")
                            .is_some_and(|d| input.hosted.contains(d))
                })
            })
        })
        .cloned()
        .collect();
    let empty = BTreeMap::new();
    let assemble = |children: &BTreeMap<String, Block>, txs: &[Transaction], limit| {
        Assembly {
            previous: input.previous,
            source,
            timestamp: input.timestamp,
            anchor: input.anchor,
            recipient: input.recipient,
            children,
            parent_carrier: input.parent_carrier,
            path: input.path,
        }
        .assemble(txs, limit)
    };
    let mut optional: Vec<_> = input.children.iter().collect();
    if !optional.is_empty() {
        let offset = (input.previous.height % optional.len() as u64) as usize;
        optional.rotate_left(offset);
    }
    let mut low = None;
    let mut high = usize::try_from(input.spec.max_number_of_transactions_per_block)
        .unwrap_or(usize::MAX)
        .min(pooled.len());
    let mut probe = high;
    let mut best = None;
    loop {
        let provisional = assemble(&empty, &pooled, probe)?;
        let mut candidate = None;
        if fits(&provisional, source, input.spec.max_block_size)? {
            let resolved = resolve_block_content(&provisional, source, input.spec.max_block_size)?;
            let dictionary = resolved.block.transactions.node.as_deref().ok_or_else(|| {
                lattice_cashew::Error::MissingContent(provisional.transactions.raw_cid.clone())
            })?;
            let entries: BTreeMap<_, _> = dictionary.entries()?.into_iter().collect();
            let mut selected = Vec::with_capacity(entries.len());
            // Merkle entries are lexicographic (0,1,10,2); Swift rebuilds in
            // numeric index order. Never silently reorder the selected pool.
            for index in 0..entries.len() {
                let header = entries
                    .get(&index.to_string())
                    .ok_or(BlockBuilderError::InvalidTransactionContent)?;
                let tx = header.0.resolve(source, None)?;
                selected.push(
                    tx.node
                        .as_deref()
                        .ok_or_else(|| {
                            lattice_cashew::Error::MissingContent(header.0.raw_cid.clone())
                        })?
                        .clone(),
                );
            }
            let build = |count: usize| -> Result<Block, TemplateBuildError> {
                let children = optional
                    .iter()
                    .take(count)
                    .map(|(k, v)| ((*k).clone(), (*v).clone()))
                    .collect();
                let block = assemble(&children, &selected, usize::MAX)?;
                let mut same_context = block.clone();
                same_context.children = provisional.children.clone();
                if content_id(&same_context)? != content_id(&provisional)? {
                    return Err(TemplateBuildError::ContextChanged);
                }
                Ok(block)
            };
            let all = build(optional.len())?;
            if fits(&all, source, input.spec.max_block_size)? {
                candidate = Some(all);
            } else if !optional.is_empty() {
                let mut fitting = usize::from(probe != 0);
                let minimum = build(fitting)?;
                if fits(&minimum, source, input.spec.max_block_size)? {
                    candidate = Some(minimum);
                    let mut failing = optional.len();
                    while fitting + 1 < failing {
                        let count = fitting + (failing - fitting) / 2;
                        let block = build(count)?;
                        if fits(&block, source, input.spec.max_block_size)? {
                            fitting = count;
                            candidate = Some(block);
                        } else {
                            failing = count;
                        }
                    }
                }
            }
        }
        if let Some(block) = candidate {
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
            let start = low.map_or(0, |v| v + 1);
            if high < start {
                break;
            }
            probe = start + (high - start) / 2;
        }
    }
    best.ok_or(TemplateBuildError::TooLarge)
}

/// Search thresholds for locally assembled direct children. Swift local child
/// candidates carry no scheduling witness; unresolved deeper plan entries cap
/// work conservatively even when the descendant is absent. Remote witness-based
/// candidate scheduling is deliberately a separate operation.
pub fn local_search_targets(
    block: &Block,
    path: &[String],
    supplied_children: &BTreeMap<String, Block>,
    minimum_work: &BTreeMap<Vec<String>, UInt256>,
) -> Result<Vec<UInt256>, TemplateBuildError> {
    use super::template::minimum_work_target;
    let threshold =
        |committed: UInt256, path: &[String]| -> Result<(UInt256, bool), TemplateBuildError> {
            let cap = minimum_work
                .get(path)
                .map(|w| minimum_work_target(*w).ok_or(TemplateBuildError::InvalidMinimumWork))
                .transpose()?;
            Ok(cap
                .filter(|cap| *cap < committed)
                .map_or((committed, false), |cap| (cap, true)))
        };
    // Validate every entry, including filters not represented by this candidate.
    for (entry, work) in minimum_work {
        if !entry.starts_with(path) || minimum_work_target(*work).is_none() {
            return Err(TemplateBuildError::InvalidMinimumWork);
        }
    }
    let (own, binding) = threshold(block.target, path)?;
    let mut easiest = own;
    let mut bound = binding.then_some(own);
    let mut thresholds = vec![own];
    let empty_index = lattice_cashew::Header::new(BTreeMap::<String, BlockHeader>::new())?.raw_cid;
    let mut leaves_only = true;
    let Some(index) = block.children.node.as_deref() else {
        return Err(lattice_cashew::Error::MissingContent(block.children.raw_cid.clone()).into());
    };
    for (directory, child) in supplied_children {
        let Some(carried) = index.get(directory) else {
            continue;
        };
        if carried.0.raw_cid != content_id(child)? {
            return Err(BlockBuilderError::ChildCommitment(directory.clone()).into());
        }
        let mut child_path = path.to_vec();
        child_path.push(directory.clone());
        let (target, binding) = threshold(child.target, &child_path)?;
        thresholds.push(target);
        easiest = easiest.max(target);
        if binding {
            bound = Some(bound.map_or(target, |b| b.min(target)));
        }
        for (entry, work) in minimum_work {
            if entry.len() > child_path.len() && entry.starts_with(&child_path) {
                let cap =
                    minimum_work_target(*work).ok_or(TemplateBuildError::InvalidMinimumWork)?;
                bound = Some(bound.map_or(cap, |b| b.min(cap)));
            }
        }
        leaves_only &= child.children.raw_cid == empty_index;
    }
    let search = bound.map_or(easiest, |b| easiest.min(b));
    if !leaves_only {
        return Ok(vec![search]);
    }
    thresholds.push(search);
    thresholds.retain(|t| *t <= search);
    thresholds.sort_unstable_by(|a, b| b.cmp(a));
    thresholds.dedup();
    Ok(thresholds)
}

pub struct MergedTemplateBuildOutput {
    pub build: crate::core::mining::TemplateBuild,
    /// Host applies each refusal only if that level's tip/pool epoch still matches.
    pub refused: BTreeMap<Vec<String>, Vec<String>>,
}

fn policy_transactions(
    transactions: &[Transaction],
    spec: &ChainSpec,
    path: &[String],
    height: u64,
    timestamp: i64,
    source: &impl ContentSource,
) -> (Vec<Transaction>, Vec<String>) {
    if spec.wasm_policies.is_empty() {
        return (transactions.to_vec(), vec![]);
    }
    let mut accepted = vec![];
    let mut refused = vec![];
    for tx in transactions {
        let Ok(body) = tx.body.resolve(source, None) else {
            continue;
        };
        let Some(body) = body.node.as_deref() else {
            continue;
        };
        let result = PolicyBatch {
            bodies: std::slice::from_ref(body),
            spec,
            chain_path: path,
            height,
            timestamp,
            scopes: None,
        }
        .verify(source);
        match result {
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
    (accepted, refused)
}

struct LocalAssembly<'a, S> {
    contexts: &'a BTreeMap<Vec<String>, ChildTemplateContext>,
    request: &'a crate::core::mining::TemplateRequest,
    source: &'a S,
    refused: BTreeMap<Vec<String>, Vec<String>>,
}
impl<S: ContentSource> LocalAssembly<'_, S> {
    fn hosted(&self, path: &[String]) -> BTreeSet<String> {
        self.contexts
            .keys()
            .filter(|p| p.len() == path.len() + 1 && p.starts_with(path))
            .filter_map(|p| p.last().cloned())
            .collect()
    }
    fn children(
        &mut self,
        path: &[String],
        entering: &LatticeStateHeader,
        timestamp: i64,
    ) -> BTreeMap<String, Block> {
        let paths: Vec<_> = self
            .contexts
            .keys()
            .filter(|p| p.len() == path.len() + 1 && p.starts_with(path))
            .cloned()
            .collect();
        let mut children = BTreeMap::new();
        for path in paths {
            // One unavailable/invalid hosted candidate must not suppress root work.
            if let Ok(Some(block)) = self.child(&path, entering, timestamp) {
                children.insert(path.last().expect("direct child").clone(), block);
            }
        }
        children
    }
    fn child(
        &mut self,
        path: &[String],
        entering: &LatticeStateHeader,
        timestamp: i64,
    ) -> Result<Option<Block>, TemplateBuildError> {
        let context = &self.contexts[path];
        if let Some(header) = &context.previous {
            let resolved = header.0.resolve(self.source, None)?;
            let previous = resolved
                .node
                .as_deref()
                .ok_or_else(|| lattice_cashew::Error::MissingContent(header.0.raw_cid.clone()))?;
            let spec = previous.spec.0.resolve(self.source, None)?;
            let spec = spec.node.as_deref().ok_or_else(|| {
                lattice_cashew::Error::MissingContent(previous.spec.0.raw_cid.clone())
            })?;
            let time = timestamp.max(
                previous
                    .timestamp
                    .checked_add(1)
                    .ok_or(TemplateBuildError::TimestampOverflow)?,
            );
            let children = if context.below_act_on_tip {
                BTreeMap::new()
            } else {
                self.children(path, &previous.post_state, time)
            };
            let (pooled, refused) = if context.below_act_on_tip {
                (vec![], vec![])
            } else {
                policy_transactions(
                    &context.transactions,
                    spec,
                    path,
                    previous
                        .height
                        .checked_add(1)
                        .ok_or(BlockBuilderError::HeightOverflow)?,
                    time,
                    self.source,
                )
            };
            if !refused.is_empty() {
                self.refused.insert(path.to_vec(), refused);
            }
            // Builder reads only carrier.prev_state. This local surrogate is
            // never published, persisted, or offered as an authenticated block.
            let mut carrier = previous.clone();
            carrier.prev_state = entering.clone();
            carrier.timestamp = timestamp;
            let hosted = self.hosted(path);
            fit_level(
                &LevelTemplateInput {
                    path,
                    previous,
                    transactions: &pooled,
                    children: &children,
                    hosted: &hosted,
                    parent_carrier: Some(&carrier),
                    timestamp: time,
                    recipient: self.request.child_recipients.get(path).map(String::as_str),
                    anchor: context.anchor,
                    spec,
                },
                self.source,
            )
            .map(Some)
        } else if let Some(spec) = &context.genesis_spec {
            let mut transactions = Vec::new();
            if let Some(recipient) = &context.genesis_premine_recipient {
                let amount = spec.premine_amount();
                if amount != 0 {
                    let delta = i64::try_from(amount)
                        .map_err(|_| BlockBuilderError::InvalidTransactionContent)?;
                    transactions.push(Transaction::new(
                        BTreeMap::new(),
                        lattice_cashew::Header::new(TransactionBody {
                            account_actions: vec![AccountAction {
                                owner: recipient.clone(),
                                delta,
                            }],
                            actions: vec![],
                            deposit_actions: vec![],
                            receipt_actions: vec![],
                            withdrawal_actions: vec![],
                            signers: vec![],
                            nonce: 0,
                            chain_path: path.to_vec(),
                        })?,
                    ));
                }
            }
            transactions.extend(context.genesis_transactions.clone());
            let block = GenesisTemplate {
                spec,
                transactions: &transactions,
                children: &BTreeMap::new(),
                parent_state: Some(entering),
                timestamp,
                target: context.genesis_target,
                nonce: 0,
                version: Block::CURRENT_VERSION,
            }
            .build(self.source)?
            .block;
            let header = lattice_cashew::Volume::new(block.clone())?;
            // Genesis is offered only after the same production validation used
            // by child admission; the result itself is not installed here.
            if validate_child_genesis(&header, self.source, timestamp, path).is_err() {
                return Ok(None);
            }
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }
}

/// Assemble a root and its locally hosted descendants from a coherent host
/// snapshot. `previous` child headers must already be selected for anchor
/// continuity against each parent's entering state. The host must reject this
/// result if ANY referenced tip, pool, configuration, or hosted level changes.
/// Mined blocks still require real child proofs and private admission validation.
pub fn build_merged_template(
    job: &crate::core::mining::TemplateJob,
    source: &impl ContentSource,
    now: i64,
    anchor: Option<DifficultyAnchor>,
    contexts: &BTreeMap<Vec<String>, ChildTemplateContext>,
) -> Result<MergedTemplateBuildOutput, TemplateBuildError> {
    use super::template::{minimum_work_target, work_id};
    use crate::core::{mining::TemplateBuild, work::TemplateChain};
    if job.request.parent_carrier.is_some() {
        return Err(TemplateBuildError::UnsupportedContext);
    }
    let path = vec!["Nexus".to_owned()];
    for (entry, work) in &job.request.minimum_work {
        if !entry.starts_with(&path) || minimum_work_target(*work).is_none() {
            return Err(TemplateBuildError::InvalidMinimumWork);
        }
    }
    if contexts.keys().any(|p| {
        p.len() < 2 || !p.starts_with(&path) || p.iter().skip(1).any(|d| !child_directory(d))
    }) {
        return Err(BlockBuilderError::ChildCommitment("hosted path".into()).into());
    }
    let previous =
        lattice_cashew::Header::<Block>::from_cid(&job.tip_cid)?.resolve(source, None)?;
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
    let mut assembly = LocalAssembly {
        contexts,
        request: &job.request,
        source,
        refused: BTreeMap::new(),
    };
    let children = assembly.children(&path, &previous.post_state, timestamp);
    let (pooled, refused) = policy_transactions(
        &job.transactions,
        spec,
        &path,
        previous
            .height
            .checked_add(1)
            .ok_or(BlockBuilderError::HeightOverflow)?,
        timestamp,
        source,
    );
    if !refused.is_empty() {
        assembly.refused.insert(path.clone(), refused);
    }
    let hosted = assembly.hosted(&path);
    let block = fit_level(
        &LevelTemplateInput {
            path: &path,
            previous,
            transactions: &pooled,
            children: &children,
            hosted: &hosted,
            parent_carrier: None,
            timestamp,
            recipient: job.request.reward_recipient.as_deref(),
            anchor,
            spec,
        },
        source,
    )?;
    let targets = local_search_targets(&block, &path, &children, &job.request.minimum_work)?;
    let mut chains = vec![];
    let mut frontier = vec![(&block, path)];
    while let Some((block, path)) = frontier.pop() {
        chains.push(TemplateChain {
            chain_path: path.clone(),
            target: block.target,
            height: block.height,
            reward_recipient: block.reward_recipient.clone(),
        });
        if let Some(children) = block.children.node.as_deref() {
            for (directory, header) in children.iter().rev() {
                if let Some(child) = header.0.node.as_deref() {
                    let mut path = path.clone();
                    path.push(directory.clone());
                    frontier.push((child, path));
                }
            }
        }
    }
    Ok(MergedTemplateBuildOutput {
        build: TemplateBuild {
            work_id: work_id(content_id(&block)?, &job.request.minimum_work),
            block,
            search_target: targets[0],
            targets,
            digest: merged_template_digest(&job.tip_cid, &job.transactions, contexts),
            chains,
        },
        refused: assembly.refused,
    })
}

/// Snapshot digest shared by status and completed work. Includes every hosted
/// level, including waiting or omitted candidates, in Swift parent-before-child
/// order (depth, then slash-joined path).
pub fn merged_template_digest(
    tip: &str,
    transactions: &[Transaction],
    contexts: &BTreeMap<Vec<String>, ChildTemplateContext>,
) -> String {
    use super::template::template_digest;
    use sha2::{Digest, Sha256};
    let own = template_digest(tip, transactions);
    if contexts.is_empty() {
        return own;
    }
    let mut lines = vec![own];
    let mut levels: Vec<_> = contexts.iter().collect();
    levels.sort_by(|(a, _), (b, _)| {
        a.len()
            .cmp(&b.len())
            .then_with(|| a.join("/").cmp(&b.join("/")))
    });
    for (_, context) in levels {
        let tip = context.tip_cid.as_deref().unwrap_or("");
        lines.push(format!(
            "{tip}/{}/{}",
            context.best_header_tip,
            template_digest(tip, &context.transactions)
        ));
    }
    hex::encode(Sha256::digest(lines.join("\n").as_bytes()))
}
