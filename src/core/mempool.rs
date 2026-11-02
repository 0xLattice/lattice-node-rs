//! Deterministic relay policy over transactions already classified ready by
//! consensus preflight. Pool acceptance does not prove consensus validity.
use lattice_cashew::{ContentSource, cbor};
use lattice_pow::WorkSum;
use lattice_primitives::*;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MempoolError {
    #[error("unresolved transaction")]
    Unresolved,
    #[error("transaction too large")]
    TooLarge,
    #[error("pool full")]
    Full,
    #[error("invalid state")]
    InvalidState,
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("replacement fee too low")]
    FeeTooLow,
    #[error("below relay fee")]
    BelowMinRelayFee,
    #[error("nonce ahead")]
    NonceAhead,
    #[error("content unavailable")]
    Unavailable,
    #[error("context changed")]
    ContextChanged,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MempoolDisposition {
    Ready,
    Future,
    Unavailable,
    Invalid,
}
#[derive(Debug, Clone)]
pub struct MempoolItem {
    pub cid: String,
    pub transaction: Transaction,
    pub added_at: i64,
}
#[derive(Debug, Clone, Default)]
pub struct MempoolMutation {
    pub transaction_cid: Option<String>,
    pub inserted: Option<MempoolItem>,
    pub replaced: Vec<MempoolItem>,
    pub evicted: Vec<MempoolItem>,
    pub removed: Vec<MempoolItem>,
}
impl MempoolMutation {
    pub fn departed(&self) -> impl Iterator<Item = &MempoolItem> {
        self.replaced
            .iter()
            .chain(&self.evicted)
            .chain(&self.removed)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolLimits {
    pub max_count: usize,
    pub max_bytes: usize,
    pub max_signatures: usize,
    pub min_relay_fee: u64,
}
impl Default for MempoolLimits {
    fn default() -> Self {
        Self {
            max_count: 10_000,
            max_bytes: 64 * 1024 * 1024,
            max_signatures: 64,
            min_relay_fee: 0,
        }
    }
}
impl MempoolLimits {
    pub fn replacement_increment(&self) -> u64 {
        self.min_relay_fee.max(1)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedBlock {
    pub transaction_count: usize,
    pub bytes: usize,
    pub lowest_fee: WorkSum,
    pub lowest_fee_bytes: usize,
    pub lowest_fee_weight: usize,
    pub full: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeWeight {
    pub block_room: i64,
    pub growth_room: i64,
    pub max_transactions: u64,
    pub transaction_allowance: i64,
}
impl FeeWeight {
    pub fn new(spec: &ChainSpec) -> Self {
        let room = spec.max_block_size - Mempool::BLOCK_ALLOWANCE;
        Self {
            block_room: room,
            growth_room: Mempool::growth_room(spec),
            max_transactions: spec
                .max_number_of_transactions_per_block
                .min(room.max(0) as u64),
            transaction_allowance: Mempool::TRANSACTION_ALLOWANCE,
        }
    }
    pub fn of(&self, size: usize, growth: usize) -> usize {
        let mut weight = (size as u128 + self.transaction_allowance as u128).max(1);
        if self.block_room <= 0 {
            return weight as usize;
        }
        if growth > 0 {
            let share = if self.growth_room <= 0 || growth as u128 >= self.growth_room as u128 {
                self.block_room as u128
            } else {
                (growth as u128 * self.block_room as u128).div_ceil(self.growth_room as u128)
            };
            weight = weight.max(share);
        }
        if self.max_transactions > 0 {
            weight = weight.max(self.block_room as u128 / self.max_transactions as u128)
        }
        usize::try_from(weight).expect("transaction size fits platform")
    }
}
#[derive(Debug, Clone)]
struct Entry {
    item: MempoolItem,
    size: usize,
    growth: usize,
    weight: usize,
    claims: BTreeSet<String>,
    debits: BTreeMap<String, u64>,
    fee: WorkSum,
}
#[derive(Debug, Clone)]
pub struct Mempool {
    limits: MempoolLimits,
    entries: BTreeMap<String, Entry>,
    claimants: BTreeMap<String, String>,
    committed: BTreeMap<String, u64>,
    byte_count: usize,
    version: u64,
}
fn fee(n: u64) -> WorkSum {
    WorkSum::from_hex(&format!("{n:x}")).unwrap()
}
fn scaled(value: &WorkSum, mut factor: usize) -> WorkSum {
    let mut result = WorkSum::default();
    let mut addend = value.clone();
    while factor > 0 {
        if factor & 1 == 1 {
            result = &result + &addend
        }
        factor >>= 1;
        if factor > 0 {
            addend = &addend + &addend
        }
    }
    result
}
fn rate(a: &Entry, b: &Entry) -> Ordering {
    scaled(&a.fee, b.weight).cmp(&scaled(&b.fee, a.weight))
}
fn ordered(a: &Entry, b: &Entry) -> Ordering {
    rate(b, a).then_with(|| a.item.cid.cmp(&b.item.cid))
}
fn retention(a: &Entry, b: &Entry) -> Ordering {
    rate(a, b)
        .then_with(|| b.item.added_at.cmp(&a.item.added_at))
        .then_with(|| b.item.cid.cmp(&a.item.cid))
}
fn fees(entries: &[Entry]) -> WorkSum {
    entries
        .iter()
        .fold(WorkSum::default(), |sum, e| &sum + &e.fee)
}
impl Default for Mempool {
    fn default() -> Self {
        Self::new(MempoolLimits::default())
    }
}
impl Mempool {
    pub const BLOCK_ALLOWANCE: i64 = 1792;
    pub const TRANSACTION_ALLOWANCE: i64 = 256;
    pub const REWARD_ALLOWANCE: i64 = 128;
    pub fn new(limits: MempoolLimits) -> Self {
        assert!(limits.max_count > 0 && limits.max_bytes > 0 && limits.max_signatures > 0);
        Self {
            limits,
            entries: BTreeMap::new(),
            claimants: BTreeMap::new(),
            committed: BTreeMap::new(),
            byte_count: 0,
            version: 0,
        }
    }
    pub fn limits(&self) -> &MempoolLimits {
        &self.limits
    }
    pub fn count(&self) -> usize {
        self.entries.len()
    }
    pub fn byte_count(&self) -> usize {
        self.byte_count
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn contains(&self, cid: &str) -> bool {
        self.entries.contains_key(cid)
    }
    pub fn item(&self, cid: &str) -> Option<MempoolItem> {
        self.entries.get(cid).map(|e| e.item.clone())
    }
    pub fn items(&self) -> Vec<MempoolItem> {
        self.entries.values().map(|e| e.item.clone()).collect()
    }
    pub fn growth_room(spec: &ChainSpec) -> i64 {
        spec.max_state_growth - Self::REWARD_ALLOWANCE
    }
    pub fn transaction_room(spec: &ChainSpec) -> i64 {
        spec.max_block_size - Self::BLOCK_ALLOWANCE - Self::TRANSACTION_ALLOWANCE
    }
    pub fn debits(body: &TransactionBody) -> Option<BTreeMap<String, u64>> {
        Some(
            body.net_balance_deltas()
                .ok()?
                .into_iter()
                .filter_map(|(k, v)| match v {
                    AccountBalanceDelta::Debit(n) => Some((k, n)),
                    _ => None,
                })
                .collect(),
        )
    }
    pub fn claims(body: &TransactionBody) -> BTreeSet<String> {
        let mut claims = BTreeSet::new();
        if let Some(s) = body.signers.first() {
            claims.insert(format!("nonce:{s}:{}", body.nonce));
        }
        for a in &body.actions {
            claims.insert(format!("action:{}", a.key));
        }
        for a in &body.deposit_actions {
            claims.insert(format!(
                "deposit:{}",
                DepositKey {
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce
                }
            ));
        }
        for a in &body.withdrawal_actions {
            claims.insert(format!(
                "deposit:{}",
                DepositKey {
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce
                }
            ));
        }
        for a in &body.receipt_actions {
            claims.insert(format!(
                "receipt:{}",
                ReceiptKey {
                    directory: a.directory.clone(),
                    demander: a.demander.clone(),
                    amount_demanded: a.amount_demanded,
                    nonce: a.nonce
                }
            ));
        }
        claims
    }
    pub fn balances(
        body: &TransactionBody,
        state: &LatticeState,
        source: &impl ContentSource,
    ) -> Option<BTreeMap<String, u64>> {
        let debits = Self::debits(body)?;
        let mut balances = BTreeMap::new();
        if debits.is_empty() {
            return Some(balances);
        }
        let header = &state.account_state.0;
        if header.encryption_info.is_some() {
            return None;
        }
        let mut accounts: AccountState = if let Some(node) = header.node.as_deref() {
            lattice_cashew::verify_content_address(&header.raw_cid, &cbor::encode(node).ok()?)
                .ok()?;
            node.clone()
        } else {
            let bytes = source.fetch(&header.raw_cid).ok()?;
            lattice_cashew::verify_content_address(&header.raw_cid, &bytes).ok()?;
            cbor::decode(&bytes).ok()?
        };
        for account in debits.keys() {
            accounts = accounts.resolve_key(account, source).ok()?;
            balances.insert(
                account.clone(),
                accounts.get(account).ok()?.copied().unwrap_or(0),
            );
        }
        Some(balances)
    }
    pub fn check(
        &self,
        transaction: &Transaction,
        spec: &ChainSpec,
    ) -> Result<String, MempoolError> {
        Ok(self.measure(transaction, spec, 0)?.item.cid)
    }
    fn measure(
        &self,
        transaction: &Transaction,
        spec: &ChainSpec,
        added_at: i64,
    ) -> Result<Entry, MempoolError> {
        use MempoolError::*;
        if transaction.signatures.len() > self.limits.max_signatures
            || transaction
                .signatures
                .iter()
                .any(|(k, v)| k.len() > u16::MAX as usize || v.len() > u16::MAX as usize)
        {
            return Err(TooLarge);
        }
        let body = transaction.body.node.as_deref().ok_or(Unresolved)?;
        let body_data = cbor::encode(body).map_err(|_| Unresolved)?;
        let envelope = cbor::encode(transaction).map_err(|_| Unresolved)?;
        if body.signers.len() > self.limits.max_signatures {
            return Err(TooLarge);
        }
        let surplus = body.miner_surplus().ok_or(InvalidState)?;
        let debits = Self::debits(body).ok_or(InvalidState)?;
        if surplus < fee(self.limits.min_relay_fee) {
            return Err(BelowMinRelayFee);
        }
        let size = envelope
            .len()
            .checked_add(body_data.len())
            .ok_or(TooLarge)?;
        if size as i128 > Self::transaction_room(spec) as i128 {
            return Err(TooLarge);
        }
        let growth = lattice_validation::state_growth_ceiling(std::slice::from_ref(body), None)
            .map_err(|_| InvalidState)?
            .max(0) as usize;
        if growth as i128 > Self::growth_room(spec) as i128 {
            return Err(TooLarge);
        }
        Ok(Entry {
            item: MempoolItem {
                cid: content_id(transaction).map_err(|_| Unresolved)?,
                transaction: transaction.clone(),
                added_at,
            },
            size,
            growth,
            weight: FeeWeight::new(spec).of(size, growth),
            claims: Self::claims(body),
            debits,
            fee: surplus,
        })
    }
    fn ordered(&self) -> Vec<Entry> {
        let mut entries: Vec<_> = self.entries.values().cloned().collect();
        entries.sort_by(ordered);
        entries
    }
    fn fill(spec: &ChainSpec, entries: &[Entry]) -> (Vec<Entry>, Vec<Entry>) {
        let (mut taken, mut rest) = (Vec::new(), Vec::new());
        let mut room = spec.max_block_size - Self::BLOCK_ALLOWANCE;
        let mut growth = Self::growth_room(spec);
        let mut misses = 0;
        for (i, e) in entries.iter().enumerate() {
            if taken.len() as u64 >= spec.max_number_of_transactions_per_block || misses >= 1000 {
                rest.extend_from_slice(&entries[i..]);
                break;
            }
            let charged = e.size as i128 + Self::TRANSACTION_ALLOWANCE as i128;
            if charged <= room as i128 && e.growth as i128 <= growth as i128 {
                taken.push(e.clone());
                room -= charged as i64;
                growth -= e.growth as i64;
                misses = 0
            } else {
                rest.push(e.clone());
                misses += 1
            }
        }
        (taken, rest)
    }
    pub fn next_block(&self, spec: &ChainSpec) -> Vec<Transaction> {
        Self::fill(spec, &self.ordered())
            .0
            .into_iter()
            .map(|e| e.item.transaction)
            .collect()
    }
    pub fn projected_blocks(&self, count: usize, spec: &ChainSpec) -> Vec<ProjectedBlock> {
        let mut blocks = Vec::new();
        let mut rest = self.ordered();
        while blocks.len() < count {
            let (taken, left) = Self::fill(spec, &rest);
            let Some(lowest) = taken.last() else { break };
            blocks.push(ProjectedBlock {
                transaction_count: taken.len(),
                bytes: taken.iter().map(|e| e.size).sum(),
                lowest_fee: lowest.fee.clone(),
                lowest_fee_bytes: lowest.size,
                lowest_fee_weight: lowest.weight,
                full: !left.is_empty(),
            });
            rest = left;
        }
        blocks
    }
    fn shed(
        &self,
        account: &str,
        debit: u64,
        balance: u64,
        gone: &BTreeMap<String, Entry>,
    ) -> Option<Vec<Entry>> {
        let mut held = self.committed.get(account).copied().unwrap_or(0)
            - gone
                .values()
                .map(|e| e.debits.get(account).copied().unwrap_or(0))
                .sum::<u64>();
        let fits = |held| held <= balance && debit <= balance - held;
        if fits(held) {
            return Some(Vec::new());
        }
        let mut entries: Vec<_> = self
            .entries
            .values()
            .filter(|e| e.debits.contains_key(account) && !gone.contains_key(&e.item.cid))
            .cloned()
            .collect();
        entries.sort_by(retention);
        let mut shed = Vec::new();
        for e in entries {
            held -= e.debits[account];
            shed.push(e);
            if fits(held) {
                return Some(shed);
            }
        }
        None
    }
    /// Caller must obtain consensus preflight Ready for the same executed tip and
    /// provide its authenticated account balances. This API enforces relay policy.
    pub fn submit(
        &mut self,
        transaction: &Transaction,
        spec: &ChainSpec,
        added_at: i64,
        balances: &BTreeMap<String, u64>,
    ) -> Result<MempoolMutation, MempoolError> {
        use MempoolError::*;
        let entry = self.measure(transaction, spec, added_at)?;
        let cid = entry.item.cid.clone();
        if self.contains(&cid) {
            return Ok(MempoolMutation {
                transaction_cid: Some(cid),
                ..Default::default()
            });
        }
        if entry.size > self.limits.max_bytes {
            return Err(TooLarge);
        }
        let mut displaced = BTreeMap::new();
        for claim in &entry.claims {
            if let Some(holder) = self.claimants.get(claim)
                && let Some(existing) = self.entries.get(holder)
            {
                displaced.insert(holder.clone(), existing.clone());
            }
        }
        let claims_conflict = !displaced.is_empty();
        for (account, debit) in &entry.debits {
            for e in self
                .shed(
                    account,
                    *debit,
                    balances.get(account).copied().unwrap_or(0),
                    &displaced,
                )
                .ok_or(InsufficientFunds)?
            {
                displaced.insert(e.item.cid.clone(), e);
            }
        }
        if !displaced.is_empty() {
            let before = self.ordered();
            let mut after: Vec<_> = before
                .iter()
                .filter(|e| !displaced.contains_key(&e.item.cid))
                .cloned()
                .collect();
            after.push(entry.clone());
            after.sort_by(ordered);
            let next = Self::fill(spec, &after).0;
            let required =
                &fees(&Self::fill(spec, &before).0) + &fee(self.limits.replacement_increment());
            if !next.iter().any(|e| e.item.cid == cid) || fees(&next) < required {
                return Err(if claims_conflict {
                    FeeTooLow
                } else {
                    InsufficientFunds
                });
            }
        }
        let mut count = self.count() - displaced.len();
        let mut bytes = self.byte_count - displaced.values().map(|e| e.size).sum::<usize>();
        let mut evictions = Vec::new();
        if count >= self.limits.max_count || entry.size > self.limits.max_bytes - bytes {
            let mut candidates: Vec<_> = self
                .entries
                .values()
                .filter(|e| !displaced.contains_key(&e.item.cid))
                .cloned()
                .collect();
            candidates.sort_by(retention);
            for e in candidates {
                if count < self.limits.max_count && entry.size <= self.limits.max_bytes - bytes {
                    break;
                }
                if retention(&entry, &e) != Ordering::Greater {
                    return Err(Full);
                }
                count -= 1;
                bytes -= e.size;
                evictions.push(e.item.cid);
            }
        }
        if count >= self.limits.max_count || entry.size > self.limits.max_bytes - bytes {
            return Err(Full);
        }
        let mut mutation = MempoolMutation {
            transaction_cid: Some(cid.clone()),
            ..Default::default()
        };
        for key in displaced.keys() {
            if let Some(item) = self.remove_entry(key) {
                mutation.replaced.push(item)
            }
        }
        for key in evictions {
            if let Some(item) = self.remove_entry(&key) {
                mutation.evicted.push(item)
            }
        }
        mutation.inserted = Some(entry.item.clone());
        self.insert(entry);
        Ok(mutation)
    }
    fn insert(&mut self, entry: Entry) {
        for claim in &entry.claims {
            self.claimants.insert(claim.clone(), entry.item.cid.clone());
        }
        for (account, debit) in &entry.debits {
            *self.committed.entry(account.clone()).or_default() += debit;
        }
        self.byte_count += entry.size;
        self.entries.insert(entry.item.cid.clone(), entry);
        self.version = self.version.wrapping_add(1);
    }
    fn remove_entry(&mut self, cid: &str) -> Option<MempoolItem> {
        let e = self.entries.remove(cid)?;
        for c in e.claims {
            if self.claimants.get(&c).is_some_and(|held| held == cid) {
                self.claimants.remove(&c);
            }
        }
        for (a, d) in e.debits {
            let left = self.committed[&a] - d;
            if left == 0 {
                self.committed.remove(&a);
            } else {
                self.committed.insert(a, left);
            }
        }
        self.byte_count -= e.size;
        self.version = self.version.wrapping_add(1);
        Some(e.item)
    }
    pub fn remove(&mut self, cids: impl IntoIterator<Item = String>) -> MempoolMutation {
        MempoolMutation {
            removed: cids
                .into_iter()
                .filter_map(|cid| self.remove_entry(&cid))
                .collect(),
            ..Default::default()
        }
    }
    pub fn clear(&mut self) -> MempoolMutation {
        self.remove(self.entries.keys().cloned().collect::<Vec<_>>())
    }
    pub fn rebalance(&mut self, balances: &BTreeMap<String, u64>) -> MempoolMutation {
        let mut mutation = MempoolMutation::default();
        for (a, b) in balances {
            for e in self.shed(a, 0, *b, &BTreeMap::new()).unwrap_or_default() {
                if let Some(item) = self.remove_entry(&e.item.cid) {
                    mutation.evicted.push(item)
                }
            }
        }
        mutation
    }
}
