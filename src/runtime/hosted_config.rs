//! Pure hosted-genesis configuration precheck. This is not genesis admission:
//! state transitions, action validity, module configuration and securing proofs
//! remain separate gates. `now` is the caller's UTC timestamp in milliseconds.
use lattice_cashew::Header;
use lattice_primitives::*;
use lattice_validation::WasmPolicyContext;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, thiserror::Error)]
pub enum PremineError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Content(#[from] lattice_cashew::Error),
    #[error(transparent)]
    Context(#[from] lattice_validation::PolicyContextError),
    #[error(transparent)]
    Policy(#[from] lattice_wasm::PolicyError),
}

/// The optional unsigned protocol premine comes before creator transactions.
/// No recipient or a zero amount produces no transaction, as in Swift.
pub fn premine_transactions(
    spec: &ChainSpec,
    recipient: Option<&str>,
    path: &[String],
) -> Result<Vec<Transaction>, PremineError> {
    let amount = spec.premine_amount();
    let Some(recipient) = recipient.filter(|_| amount > 0) else {
        return Ok(Vec::new());
    };
    if !standard_cid(recipient) {
        return Err(PremineError::Invalid(format!(
            "{recipient} is not an address"
        )));
    }
    let delta = i64::try_from(amount).map_err(|_| {
        PremineError::Invalid(format!(
            "a premine of {amount} is more than one account action carries"
        ))
    })?;
    Ok(vec![Transaction::new(
        BTreeMap::new(),
        Header::new(TransactionBody {
            account_actions: vec![AccountAction {
                owner: recipient.into(),
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
    )])
}

/// Check explicit creator signatures/path/no coin movement, prepend the
/// protocol premine, enforce transaction count, and evaluate transaction-scope
/// policies using supplied raw WASM bytes authenticated by their typed module
/// CIDs. Action policies are deliberately outside Swift's precheck.
pub fn check_premine(
    spec: &ChainSpec,
    recipient: Option<&str>,
    transactions: &[Transaction],
    path: &[String],
    modules: &[Vec<u8>],
    now: i64,
) -> Result<(), PremineError> {
    for (index, transaction) in transactions.iter().enumerate() {
        let body = transaction.body.node.as_deref().ok_or_else(|| {
            PremineError::Invalid(format!("genesis transaction {index} carries no body"))
        })?;
        if body.chain_path != path {
            return Err(PremineError::Invalid(format!(
                "genesis transaction {index} is for {}, not {}",
                body.chain_path.join("/"),
                path.join("/")
            )));
        }
        let signed = transaction
            .normalized_signatures()
            .filter(|s| !s.is_empty());
        let valid = signed.as_ref().is_some_and(|signatures| {
            let preimage =
                signing_preimage(&transaction.body.raw_cid, &body.chain_path, body.nonce);
            signatures
                .iter()
                .all(|(key, signature)| crypto::verify(&preimage, signature, key))
                && signatures
                    .keys()
                    .map(|key| crypto::create_address(key))
                    .collect::<Result<BTreeSet<_>, _>>()
                    .is_ok_and(|addresses| addresses == body.signers.iter().cloned().collect())
        });
        if !valid {
            return Err(PremineError::Invalid(format!(
                "genesis transaction {index} is not signed by exactly its signers"
            )));
        }
        if !body.account_actions.is_empty()
            || !body.deposit_actions.is_empty()
            || !body.receipt_actions.is_empty()
            || !body.withdrawal_actions.is_empty()
        {
            return Err(PremineError::Invalid(format!(
                "genesis transaction {index} moves coins; a genesis pays only its premine"
            )));
        }
    }
    let mut all = premine_transactions(spec, recipient, path)?;
    all.extend_from_slice(transactions);
    if all.len() as u64 > spec.max_number_of_transactions_per_block {
        return Err(PremineError::Invalid(format!(
            "a genesis of {} transactions is more than the spec's {} per block",
            all.len(),
            spec.max_number_of_transactions_per_block
        )));
    }
    let mut by_cid = BTreeMap::new();
    for bytes in modules {
        by_cid
            .entry(content_id(&WasmPolicyModule {
                bytes: bytes.clone(),
            })?)
            .or_insert(bytes);
    }
    for policy in spec
        .wasm_policies
        .iter()
        .filter(|p| p.scope == PolicyScope::Transaction)
    {
        let bytes = by_cid.get(&policy.module_cid).ok_or_else(|| {
            PremineError::Invalid(format!("no module for policy {}", policy.module_cid))
        })?;
        // Swift requires the module even with zero bodies, but only evaluates
        // (and validates its ABI/exports) when an actual body is present.
        let mut executable = None;
        for transaction in &all {
            let body = transaction
                .body
                .node
                .as_deref()
                .expect("checked materialized body");
            let context = WasmPolicyContext {
                abi_version: policy.abi_version,
                scope: PolicyScope::Transaction,
                height: 0,
                timestamp: now,
                chain_spec: spec,
                chain_path: path,
                transaction: Some(body),
                action: None,
                action_index: None,
            }
            .canonical_bytes()?;
            if executable.is_none() {
                executable = Some(lattice_wasm::WasmPolicy::new(
                    bytes,
                    policy.abi_version,
                    &policy.entrypoint,
                )?);
            }
            if !executable
                .as_ref()
                .expect("initialized")
                .evaluate(&context)?
            {
                return Err(PremineError::Invalid(if body.signers.is_empty() {
                    format!(
                        "policy {} refuses the genesis paying the premine to {}",
                        policy.module_cid,
                        recipient.unwrap_or("")
                    )
                } else {
                    format!(
                        "policy {} refuses the genesis transaction signed by {}",
                        policy.module_cid,
                        body.signers.join(", ")
                    )
                }));
            }
        }
    }
    Ok(())
}
