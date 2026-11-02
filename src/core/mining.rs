//! Per-level mining reducer. Job epochs fence stale work, including A→B→A.
//! A shell must persist the leading PoolChanged delta before releasing effects.
use super::{mempool::*, sync::PeerId, work::*};
use lattice_primitives::*;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionOrigin {
    Local(u64),
    Submitted(u64),
    Peer(PeerId),
    Returned,
    Restored(i64),
}
impl TransactionOrigin {
    fn answered(&self) -> bool {
        matches!(self, Self::Local(_) | Self::Submitted(_))
    }
}
#[derive(Debug, Clone)]
pub struct TipMove {
    pub tip_cid: String,
    pub confirmed: BTreeSet<String>,
    pub returned: Vec<Transaction>,
    pub left: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct PreflightJob {
    pub cid: String,
    pub transaction: Transaction,
    pub tip_cid: String,
    pub tip_epoch: u64,
    pub pool_version: u64,
    pub checks_policies: bool,
}
#[derive(Debug, Clone)]
pub struct ParentCarrier {
    pub cid: String,
    pub block: Block,
}
impl PartialEq for ParentCarrier {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
}
impl Eq for ParentCarrier {}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TemplateRequest {
    pub reward_recipient: Option<String>,
    pub minimum_work: BTreeMap<Vec<String>, UInt256>,
    pub parent_carrier: Option<ParentCarrier>,
    pub child_recipients: BTreeMap<Vec<String>, String>,
}
#[derive(Debug, Clone)]
pub struct TemplateJob {
    pub id: u64,
    pub tip_cid: String,
    pub tip_epoch: u64,
    pub pool_version: u64,
    pub transactions: Vec<Transaction>,
    pub request: TemplateRequest,
}
#[derive(Debug, Clone)]
pub struct TemplateBuild {
    pub work_id: String,
    pub block: Block,
    pub search_target: UInt256,
    pub targets: Vec<UInt256>,
    pub digest: String,
    pub chains: Vec<TemplateChain>,
}
#[derive(Debug, Clone)]
pub enum MiningEvent {
    TransactionReceived(Transaction, TransactionOrigin),
    Preflighted(PreflightJob, MempoolDisposition, BTreeMap<String, u64>),
    TipMoved(TipMove),
    TemplateRequested(u64, Box<TemplateRequest>),
    TemplateBuilt(Box<TemplateJob>, Option<Box<TemplateBuild>>),
    PolicyRefused {
        tip_cid: String,
        cids: Vec<String>,
    },
    SubmitWork {
        reply_id: u64,
        work_id: String,
        nonce: u64,
    },
}
#[derive(Debug, Clone, Default)]
pub struct PoolDelta {
    pub added: Vec<MempoolItem>,
    pub journaled: Vec<MempoolItem>,
    pub removed: Vec<String>,
}
impl PoolDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.journaled.is_empty() && self.removed.is_empty()
    }
}
#[derive(Debug, Clone)]
pub enum MiningEffect {
    PoolChanged(PoolDelta),
    TransactionAdmitted {
        reply_id: u64,
        cid: String,
        count: usize,
        bytes: usize,
    },
    TransactionRefused(u64, MempoolError),
    AnnounceTransaction(String),
    TemplateIssued(u64, WorkTemplate),
    TemplateRefused(u64, TemplateError),
    Mined(u64, Block),
    WorkRefused(u64, TemplateError),
    Preflight(PreflightJob),
    BuildTemplate(TemplateJob),
    ReturnTransactions {
        left: Vec<String>,
        carried: BTreeSet<String>,
    },
}
#[derive(Debug, Clone)]
pub struct MiningConfig {
    pub max_pending_peer_admissions: usize,
    pub max_pending_per_peer: usize,
    pub max_pending_returned: usize,
    pub max_pending_submitted: usize,
    pub max_reissues: usize,
    pub max_waiting_template_requests: usize,
    pub mempool: MempoolLimits,
    pub template_lifetime: i64,
    pub template_capacity: usize,
}
impl Default for MiningConfig {
    fn default() -> Self {
        Self {
            max_pending_peer_admissions: 1024,
            max_pending_per_peer: 64,
            max_pending_returned: 10000,
            max_pending_submitted: 256,
            max_reissues: 3,
            max_waiting_template_requests: 64,
            mempool: MempoolLimits::default(),
            template_lifetime: 30000,
            template_capacity: 16,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    Local,
    Submitted,
    Peer(PeerId),
    Returned,
}
impl Slot {
    fn of(origin: &TransactionOrigin) -> Self {
        match origin {
            TransactionOrigin::Local(_) | TransactionOrigin::Restored(_) => Self::Local,
            TransactionOrigin::Submitted(_) => Self::Submitted,
            TransactionOrigin::Peer(p) => Self::Peer(p.clone()),
            TransactionOrigin::Returned => Self::Returned,
        }
    }
}
#[derive(Debug, Clone)]
struct Admission {
    transaction: Transaction,
    origins: Vec<TransactionOrigin>,
    slot: Slot,
    reissues: usize,
}
#[derive(Debug, Clone)]
struct Waiting {
    reply_id: u64,
    reissues: usize,
}
#[derive(Debug, Default)]
struct Turn {
    delta: PoolDelta,
    replies: Vec<MiningEffect>,
    jobs: Vec<MiningEffect>,
}
impl Turn {
    fn effects(self) -> Vec<MiningEffect> {
        let mut out = Vec::new();
        if !self.delta.is_empty() {
            out.push(MiningEffect::PoolChanged(self.delta))
        }
        out.extend(self.replies);
        out.extend(self.jobs);
        out
    }
}
#[derive(Debug, Clone)]
pub struct MiningState {
    mempool: Mempool,
    templates: WorkTemplateBook,
    tip_cid: String,
    tip_epoch: u64,
    spec: Option<ChainSpec>,
    config: MiningConfig,
    admissions: BTreeMap<String, Admission>,
    preflighting: BTreeSet<String>,
    journaled: BTreeSet<String>,
    builds: BTreeMap<u64, (TemplateJob, Vec<Waiting>)>,
    next_job_id: u64,
}
impl MiningState {
    pub fn new(tip_cid: String, spec: Option<ChainSpec>, config: MiningConfig) -> Self {
        Self {
            mempool: Mempool::new(config.mempool.clone()),
            templates: WorkTemplateBook::new(config.template_lifetime, config.template_capacity),
            tip_cid,
            tip_epoch: 0,
            spec,
            config,
            admissions: BTreeMap::new(),
            preflighting: BTreeSet::new(),
            journaled: BTreeSet::new(),
            builds: BTreeMap::new(),
            next_job_id: 1,
        }
    }
    pub fn mempool(&self) -> &Mempool {
        &self.mempool
    }
    pub fn templates(&self) -> &WorkTemplateBook {
        &self.templates
    }
    pub fn tip_cid(&self) -> &str {
        &self.tip_cid
    }
    pub fn tip_epoch(&self) -> u64 {
        self.tip_epoch
    }
    pub fn set_spec(&mut self, spec: Option<ChainSpec>) {
        self.spec = spec
    }
    pub fn journaled(&self) -> &BTreeSet<String> {
        &self.journaled
    }
    pub fn next_block(&self) -> Vec<Transaction> {
        self.spec
            .as_ref()
            .map(|s| self.mempool.next_block(s))
            .unwrap_or_default()
    }
    pub fn pending_admissions(&self) -> usize {
        self.admissions.len()
    }
    pub fn pending_origins(&self) -> usize {
        self.admissions.values().map(|a| a.origins.len()).sum()
    }
    pub fn pending_peer_admissions(&self) -> usize {
        self.admissions
            .values()
            .filter(|a| matches!(a.slot, Slot::Peer(_)))
            .count()
    }
    pub fn pending_from(&self, peer: &PeerId) -> usize {
        self.admissions
            .values()
            .filter(|a| a.slot == Slot::Peer(peer.clone()))
            .count()
    }
    pub fn pending_returned(&self) -> usize {
        self.admissions
            .values()
            .filter(|a| a.slot == Slot::Returned)
            .count()
    }
    pub fn pending_submitted(&self) -> usize {
        self.admissions
            .values()
            .filter(|a| a.slot == Slot::Submitted)
            .count()
    }
    pub fn is_pending(&self, cid: &str) -> bool {
        self.admissions.contains_key(cid)
    }
    pub fn waiting_template_requests(&self) -> usize {
        self.builds.values().map(|(_, r)| r.len()).sum()
    }
    pub fn outstanding_preflights(&self) -> usize {
        self.preflighting.len()
    }
    fn room(&self, slot: &Slot) -> bool {
        match slot {
            Slot::Local => true,
            Slot::Submitted => self.pending_submitted() < self.config.max_pending_submitted,
            Slot::Returned => self.pending_returned() < self.config.max_pending_returned,
            Slot::Peer(p) => {
                self.pending_peer_admissions() < self.config.max_pending_peer_admissions
                    && self.pending_from(p) < self.config.max_pending_per_peer
            }
        }
    }
    pub fn step(&mut self, event: MiningEvent, now: i64) -> Vec<MiningEffect> {
        let mut turn = Turn::default();
        match event {
            MiningEvent::TransactionReceived(tx, origin) => self.receive(tx, origin, &mut turn),
            MiningEvent::Preflighted(job, disposition, balances) => {
                self.preflighted(job, disposition, &balances, now, &mut turn)
            }
            MiningEvent::TipMoved(movement) => self.tip_moved(movement, &mut turn),
            MiningEvent::TemplateRequested(id, request) => self.request_template(
                *request,
                vec![Waiting {
                    reply_id: id,
                    reissues: 0,
                }],
                &mut turn,
            ),
            MiningEvent::TemplateBuilt(job, build) => {
                self.built(*job, build.map(|b| *b), now, &mut turn)
            }
            MiningEvent::PolicyRefused { tip_cid, cids } => {
                if tip_cid == self.tip_cid {
                    let m = self.mempool.remove(cids);
                    self.departed(m, &mut turn)
                }
            }
            MiningEvent::SubmitWork {
                reply_id,
                work_id,
                nonce,
            } => match self.templates.submission(&work_id, nonce, now) {
                Ok(block) => {
                    if block.has_valid_pow() {
                        self.templates.discard(&work_id)
                    }
                    turn.replies.push(MiningEffect::Mined(reply_id, block));
                }
                Err(e) => turn.replies.push(MiningEffect::WorkRefused(reply_id, e)),
            },
        }
        turn.effects()
    }
    fn receive(&mut self, tx: Transaction, origin: TransactionOrigin, turn: &mut Turn) {
        if self.spec.is_none() && matches!(origin, TransactionOrigin::Restored(_)) {
            if let Ok(cid) = content_id(&tx) {
                self.admissions.entry(cid).or_insert(Admission {
                    transaction: tx,
                    origins: vec![origin],
                    slot: Slot::Local,
                    reissues: 0,
                });
            }
            return;
        }
        let cid = match self
            .spec
            .as_ref()
            .ok_or(MempoolError::Unavailable)
            .and_then(|s| self.mempool.check(&tx, s))
        {
            Ok(cid) => cid,
            Err(e) => {
                self.refuse(&[origin], content_id(&tx).ok().as_deref(), e, turn);
                return;
            }
        };
        if let Some(item) = self.mempool.item(&cid) {
            self.admitted(&item, &[origin], false, turn);
            return;
        }
        let slot = Slot::of(&origin);
        if let Some(admission) = self.admissions.get_mut(&cid) {
            if matches!(origin, TransactionOrigin::Peer(_)) {
                return;
            }
            admission.origins.push(origin);
            if slot == Slot::Local {
                admission.slot = Slot::Local
            }
            return;
        }
        if !self.room(&slot) {
            if let TransactionOrigin::Submitted(id) = origin {
                turn.replies
                    .push(MiningEffect::TransactionRefused(id, MempoolError::Full))
            }
            return;
        }
        self.admissions.insert(
            cid.clone(),
            Admission {
                transaction: tx.clone(),
                origins: vec![origin],
                slot,
                reissues: 0,
            },
        );
        self.preflight(cid, tx, true, turn);
    }
    fn preflight(
        &mut self,
        cid: String,
        transaction: Transaction,
        checks_policies: bool,
        turn: &mut Turn,
    ) {
        if !self.preflighting.insert(cid.clone()) {
            return;
        }
        turn.jobs.push(MiningEffect::Preflight(PreflightJob {
            cid,
            transaction,
            tip_cid: self.tip_cid.clone(),
            tip_epoch: self.tip_epoch,
            pool_version: self.mempool.version(),
            checks_policies,
        }));
    }
    fn preflighted(
        &mut self,
        job: PreflightJob,
        disposition: MempoolDisposition,
        balances: &BTreeMap<String, u64>,
        now: i64,
        turn: &mut Turn,
    ) {
        if job.tip_epoch != self.tip_epoch {
            return;
        }
        self.preflighting.remove(&job.cid);
        if let Some(admission) = self.admissions.get(&job.cid).cloned() {
            if !job.checks_policies {
                self.preflight(job.cid, admission.transaction, true, turn);
                return;
            }
            self.admissions.remove(&job.cid);
            self.admit(admission, &job.cid, disposition, balances, now, turn);
            return;
        }
        let mutation = if disposition == MempoolDisposition::Ready {
            self.mempool.rebalance(balances)
        } else {
            self.mempool.remove([job.cid])
        };
        self.departed(mutation, turn);
    }
    fn admit(
        &mut self,
        admission: Admission,
        cid: &str,
        disposition: MempoolDisposition,
        balances: &BTreeMap<String, u64>,
        now: i64,
        turn: &mut Turn,
    ) {
        let refusal = match disposition {
            MempoolDisposition::Ready => None,
            MempoolDisposition::Future => Some(MempoolError::NonceAhead),
            MempoolDisposition::Unavailable => Some(MempoolError::Unavailable),
            MempoolDisposition::Invalid => Some(MempoolError::InvalidState),
        };
        if let Some(error) = refusal {
            self.refuse(&admission.origins, Some(cid), error, turn);
            return;
        }
        let at = admission
            .origins
            .iter()
            .find_map(|o| {
                if let TransactionOrigin::Restored(at) = o {
                    Some(*at)
                } else {
                    None
                }
            })
            .unwrap_or(now);
        match self
            .spec
            .as_ref()
            .ok_or(MempoolError::Unavailable)
            .and_then(|s| self.mempool.submit(&admission.transaction, s, at, balances))
        {
            Ok(mutation) => {
                let inserted = mutation.inserted.clone();
                self.departed(mutation, turn);
                if let Some(ref item) = inserted {
                    turn.delta.added.push(item.clone())
                }
                if let Some(item) = self.mempool.item(cid) {
                    self.admitted(&item, &admission.origins, inserted.is_some(), turn)
                }
            }
            Err(e) => self.refuse(&admission.origins, Some(cid), e, turn),
        }
    }
    fn admitted(
        &mut self,
        item: &MempoolItem,
        origins: &[TransactionOrigin],
        inserted: bool,
        turn: &mut Turn,
    ) {
        let mut announce = false;
        for origin in origins {
            match origin {
                TransactionOrigin::Local(id) => {
                    if self.journaled.insert(item.cid.clone()) {
                        turn.delta.journaled.push(item.clone())
                    }
                    announce = true;
                    turn.replies.push(self.answer(*id, &item.cid));
                }
                TransactionOrigin::Submitted(id) => {
                    announce = true;
                    turn.replies.push(self.answer(*id, &item.cid));
                }
                TransactionOrigin::Restored(_) => {
                    self.journaled.insert(item.cid.clone());
                }
                TransactionOrigin::Returned => announce = true,
                TransactionOrigin::Peer(_) => announce |= inserted,
            }
        }
        if announce {
            turn.replies
                .push(MiningEffect::AnnounceTransaction(item.cid.clone()))
        }
    }
    fn answer(&self, id: u64, cid: &str) -> MiningEffect {
        MiningEffect::TransactionAdmitted {
            reply_id: id,
            cid: cid.into(),
            count: self.mempool.count(),
            bytes: self.mempool.byte_count(),
        }
    }
    fn refuse(
        &mut self,
        origins: &[TransactionOrigin],
        cid: Option<&str>,
        error: MempoolError,
        turn: &mut Turn,
    ) {
        for origin in origins {
            match origin {
                TransactionOrigin::Local(id) | TransactionOrigin::Submitted(id) => turn
                    .replies
                    .push(MiningEffect::TransactionRefused(*id, error)),
                TransactionOrigin::Restored(_) => {
                    if let Some(cid) = cid {
                        turn.delta.removed.push(cid.into())
                    }
                }
                _ => (),
            }
        }
    }
    fn departed(&mut self, mutation: MempoolMutation, turn: &mut Turn) {
        for item in mutation.departed() {
            self.journaled.remove(&item.cid);
            turn.delta.added.retain(|i| i.cid != item.cid);
            turn.delta.journaled.retain(|i| i.cid != item.cid);
            turn.delta.removed.push(item.cid.clone());
        }
    }
    fn confirm(&mut self, cids: &BTreeSet<String>, turn: &mut Turn) {
        let mutation = self.mempool.remove(cids.iter().cloned());
        self.departed(mutation, turn);
        for cid in cids {
            if let Some(admission) = self.admissions.remove(cid) {
                for origin in admission.origins {
                    match origin {
                        TransactionOrigin::Local(id) | TransactionOrigin::Submitted(id) => {
                            turn.replies.push(self.answer(id, cid))
                        }
                        TransactionOrigin::Restored(_) => turn.delta.removed.push(cid.clone()),
                        _ => (),
                    }
                }
            }
        }
    }
    fn tip_moved(&mut self, movement: TipMove, turn: &mut Turn) {
        self.tip_cid = movement.tip_cid;
        self.tip_epoch = self.tip_epoch.wrapping_add(1);
        self.confirm(&movement.confirmed, turn);
        let cids: Vec<_> = self.admissions.keys().cloned().collect();
        for cid in cids {
            let admission = self.admissions.get_mut(&cid).unwrap();
            admission.reissues += 1;
            if admission.reissues <= self.config.max_reissues
                || !admission.origins.iter().any(TransactionOrigin::answered)
            {
                continue;
            }
            let old = self.admissions.remove(&cid).unwrap();
            let mut keep = Vec::new();
            for origin in old.origins {
                match origin {
                    TransactionOrigin::Local(id) | TransactionOrigin::Submitted(id) => {
                        turn.replies.push(MiningEffect::TransactionRefused(
                            id,
                            MempoolError::ContextChanged,
                        ))
                    }
                    o => keep.push(o),
                }
            }
            if let Some(first) = keep.first() {
                let slot = if keep
                    .iter()
                    .any(|o| matches!(o, TransactionOrigin::Restored(_)))
                {
                    Slot::Local
                } else {
                    Slot::of(first)
                };
                if self.room(&slot) {
                    self.admissions.insert(
                        cid,
                        Admission {
                            origins: keep,
                            slot,
                            ..old
                        },
                    );
                }
            }
        }
        for tx in movement.returned {
            if !self.room(&Slot::Returned) {
                continue;
            }
            if let Ok(cid) = content_id(&tx)
                && !movement.confirmed.contains(&cid)
                && !self.mempool.contains(&cid)
                && !self.admissions.contains_key(&cid)
            {
                self.admissions.insert(
                    cid,
                    Admission {
                        transaction: tx,
                        origins: vec![TransactionOrigin::Returned],
                        slot: Slot::Returned,
                        reissues: 0,
                    },
                );
            }
        }
        self.preflighting.clear();
        for item in self.mempool.items() {
            self.preflight(item.cid, item.transaction, false, turn)
        }
        for (cid, admission) in self.admissions.clone() {
            self.preflight(cid, admission.transaction, true, turn)
        }
        if !movement.left.is_empty() {
            turn.jobs.push(MiningEffect::ReturnTransactions {
                left: movement.left,
                carried: movement.confirmed,
            });
        }
        let stale = std::mem::take(&mut self.builds);
        for (_, (job, replies)) in stale {
            let mut again = Vec::new();
            for mut waiting in replies {
                waiting.reissues += 1;
                if waiting.reissues > self.config.max_reissues {
                    turn.replies.push(MiningEffect::TemplateRefused(
                        waiting.reply_id,
                        TemplateError::ContextChanged,
                    ))
                } else {
                    again.push(waiting)
                }
            }
            if !again.is_empty() {
                self.request_template(job.request, again, turn)
            }
        }
    }
    fn request_template(
        &mut self,
        request: TemplateRequest,
        waiting: Vec<Waiting>,
        turn: &mut Turn,
    ) {
        if let Some((_, replies)) = self.builds.values_mut().find(|(j, _)| {
            j.tip_epoch == self.tip_epoch
                && j.pool_version == self.mempool.version()
                && j.request == request
        }) {
            replies.extend(waiting);
            return;
        }
        if self.waiting_template_requests() + waiting.len()
            > self.config.max_waiting_template_requests
        {
            for entry in waiting {
                turn.replies.push(MiningEffect::TemplateRefused(
                    entry.reply_id,
                    TemplateError::Busy,
                ))
            }
            return;
        }
        let job = TemplateJob {
            id: self.next_job_id,
            tip_cid: self.tip_cid.clone(),
            tip_epoch: self.tip_epoch,
            pool_version: self.mempool.version(),
            transactions: self.next_block(),
            request,
        };
        self.next_job_id = self
            .next_job_id
            .checked_add(1)
            .expect("template job identifier exhausted");
        self.builds.insert(job.id, (job.clone(), waiting));
        turn.jobs.push(MiningEffect::BuildTemplate(job));
    }
    fn built(&mut self, job: TemplateJob, build: Option<TemplateBuild>, now: i64, turn: &mut Turn) {
        if job.tip_epoch != self.tip_epoch {
            return;
        }
        let Some((_, waiting)) = self.builds.remove(&job.id) else {
            return;
        };
        let Some(build) = build else {
            for entry in waiting {
                turn.replies.push(MiningEffect::TemplateRefused(
                    entry.reply_id,
                    TemplateError::BuildFailed,
                ))
            }
            return;
        };
        let issued = self.templates.issue(
            WorkTemplate {
                work_id: build.work_id,
                block: build.block,
                search_target: build.search_target,
                targets: build.targets,
                tip_cid: job.tip_cid,
                pool_version: job.pool_version,
                expires_at: now
                    .checked_add(self.templates.lifetime())
                    .expect("template expiry overflow"),
                digest: build.digest,
                chains: build.chains,
            },
            now,
        );
        for entry in waiting {
            turn.replies
                .push(MiningEffect::TemplateIssued(entry.reply_id, issued.clone()))
        }
    }
}

/// Durable event boundary for the pure mining reducer. A transaction failure
/// preserves the original state and withholds replies/jobs until retry succeeds.
pub struct DurableMiningCore {
    state: MiningState,
    storage: std::sync::Arc<crate::storage::NodeStorage>,
}
impl DurableMiningCore {
    pub fn new(state: MiningState, storage: std::sync::Arc<crate::storage::NodeStorage>) -> Self {
        Self { state, storage }
    }
    pub fn state(&self) -> &MiningState {
        &self.state
    }
    pub fn step(
        &mut self,
        event: MiningEvent,
        now: i64,
    ) -> Result<Vec<MiningEffect>, crate::storage::StoreError> {
        let mut next = self.state.clone();
        let effects = next.step(event, now);
        for effect in &effects {
            if let MiningEffect::PoolChanged(delta) = effect {
                self.storage.persist_pool_delta(delta)?;
            }
        }
        self.state = next;
        Ok(effects
            .into_iter()
            .filter(|effect| !matches!(effect, MiningEffect::PoolChanged(_)))
            .collect())
    }
}

/// Produced only by the root executor after graph execution and content checks.
/// The result's authenticated balances cannot be replaced by a callback.
#[derive(Debug)]
pub struct RootPreflightCompletion {
    job: PreflightJob,
    result: lattice_validation::TransactionPreflightResult,
}
/// Immutable, private-input executor with a WAL content lease. Field order
/// closes the snapshot before releasing the node directory ownership.
pub struct RootPreflightTask {
    job: PreflightJob,
    path: Vec<String>,
    snapshot: lattice_volume_broker::DiskReadSnapshot,
    _storage: std::sync::Arc<crate::storage::NodeStorage>,
}
struct SnapshotSource<'a>(&'a lattice_volume_broker::DiskReadSnapshot);
impl lattice_cashew::ContentSource for SnapshotSource<'_> {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        self.0
            .fetch_data(&[cid.to_owned()].into_iter().collect())
            .ok()
            .and_then(|mut rows| rows.remove(cid))
            .ok_or_else(|| lattice_cashew::Error::MissingContent(cid.into()))
    }
}
impl RootPreflightTask {
    pub fn run(self, now: i64) -> Result<RootPreflightCompletion, crate::storage::StoreError> {
        let tip = lattice_cashew::Volume(lattice_cashew::Header::from_cid(&self.job.tip_cid)?);
        let result = lattice_validation::TransactionPreflight {
            transaction: &self.job.transaction,
            tip: &tip,
            chain_path: &self.path,
            parent_state: None,
            now_milliseconds: now,
            checks_policies: self.job.checks_policies,
        }
        .classify(&SnapshotSource(&self.snapshot))?;
        Ok(RootPreflightCompletion {
            job: self.job,
            result,
        })
    }
}
impl DurableMiningCore {
    /// `None` means stale work; the tip move issues replacement jobs. Missing
    /// content is classified Unavailable by the composed validator.
    pub fn execute_root_preflight(
        &self,
        job: &PreflightJob,
        tree: &lattice_block_tree::ChainTree,
        now: i64,
    ) -> Result<Option<RootPreflightCompletion>, crate::storage::StoreError> {
        self.prepare_root_preflight(job, tree)?
            .map(|task| task.run(now))
            .transpose()
    }
    /// Captures graph authority on the actor. Only this constructor can prepare
    /// a task; the worker cannot supply a different tip or admission result.
    pub fn prepare_root_preflight(
        &self,
        job: &PreflightJob,
        tree: &lattice_block_tree::ChainTree,
    ) -> Result<Option<RootPreflightTask>, crate::storage::StoreError> {
        let Some(context) = tree.context().filter(|c| c.path().len() == 1) else {
            return Ok(None);
        };
        if job.tip_epoch != self.state.tip_epoch
            || job.tip_cid != self.state.tip_cid
            || tree.executed_prefix() != Some(job.tip_cid.as_str())
            || content_id(&job.transaction)? != job.cid
        {
            return Ok(None);
        }
        Ok(Some(RootPreflightTask {
            job: job.clone(),
            path: context.path().to_vec(),
            snapshot: self.storage.preflight_snapshot()?,
            _storage: self.storage.clone(),
        }))
    }
    pub fn complete_root_preflight(
        &mut self,
        completion: RootPreflightCompletion,
        now: i64,
    ) -> Result<Vec<MiningEffect>, crate::storage::StoreError> {
        let RootPreflightCompletion { job, result } = completion;
        if job.tip_epoch != self.state.tip_epoch
            || job.tip_cid != self.state.tip_cid
            || result.transaction_cid() != job.cid
            || result.tip_cid() != job.tip_cid
            || result.checks_policies() != job.checks_policies
            || result.parent_state_cid().is_some()
            || result.chain_path() != ["Nexus"]
        {
            return Ok(Vec::new());
        }
        let disposition = match result.disposition() {
            lattice_validation::PreflightDisposition::Ready => MempoolDisposition::Ready,
            lattice_validation::PreflightDisposition::Future => MempoolDisposition::Future,
            lattice_validation::PreflightDisposition::Invalid => MempoolDisposition::Invalid,
            lattice_validation::PreflightDisposition::Unavailable => {
                MempoolDisposition::Unavailable
            }
        };
        self.step(
            MiningEvent::Preflighted(job, disposition, result.debit_balances().clone()),
            now,
        )
    }
}

impl DurableMiningCore {
    /// Hydrate durable rows, reconcile volatile pins, and re-preflight this
    /// level's transactions. No saved row is presumed valid on the current tip.
    pub fn restore(
        state: MiningState,
        storage: std::sync::Arc<crate::storage::NodeStorage>,
        chain_path: &[String],
        now: i64,
    ) -> Result<(Self, Vec<MiningEffect>), crate::storage::StoreError> {
        let items = storage.restored_mempool_items()?;
        storage.reconcile_mempool_roots()?;
        Self::restore_items(state, storage, chain_path, now, items)
    }
    /// Activate one hosted level without replacing other live levels' pins.
    /// The process startup boundary alone reconciles the global pool scope.
    pub(crate) fn restore_level(
        state: MiningState,
        storage: std::sync::Arc<crate::storage::NodeStorage>,
        chain_path: &[String],
        now: i64,
    ) -> Result<(Self, Vec<MiningEffect>), crate::storage::StoreError> {
        let items = storage.restored_mempool_items()?;
        Self::restore_items(state, storage, chain_path, now, items)
    }
    fn restore_items(
        state: MiningState,
        storage: std::sync::Arc<crate::storage::NodeStorage>,
        chain_path: &[String],
        now: i64,
        items: Vec<crate::core::mempool::MempoolItem>,
    ) -> Result<(Self, Vec<MiningEffect>), crate::storage::StoreError> {
        let mut core = Self::new(state, storage);
        let mut effects = Vec::new();
        for item in items {
            if item
                .transaction
                .body
                .node
                .as_deref()
                .is_some_and(|b| b.chain_path == chain_path)
            {
                effects.extend(core.step(
                    MiningEvent::TransactionReceived(
                        item.transaction,
                        TransactionOrigin::Restored(item.added_at),
                    ),
                    now,
                )?);
            }
        }
        Ok((core, effects))
    }
}

#[path = "mining_child.rs"]
mod child;
pub use child::{ChildPreflightCompletion, ChildPreflightTask};
