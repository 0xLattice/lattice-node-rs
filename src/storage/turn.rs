//! Synchronous actor fact turns. The caller owns mutable cores exclusively and
//! must fail-stop on an error: provisional trees must never become observable.
use super::*;
use lattice_volume_broker::{RetainedRootBroker, Roots};
use std::{marker::PhantomData, rc::Rc};

/// Owner-thread recursion lets existing private admission helpers compose;
/// outsiders cannot observe or append provisional facts on the shared handle.
pub(super) struct TurnGate {
    state: Mutex<(Option<std::thread::ThreadId>, usize)>,
    ready: std::sync::Condvar,
}
impl TurnGate {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new((None, 0)),
            ready: std::sync::Condvar::new(),
        }
    }
    pub(super) fn lock(&self) -> Result<GateGuard<'_>, std::convert::Infallible> {
        let owner = std::thread::current().id();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while state.0.is_some_and(|held| held != owner) {
            state = self.ready.wait(state).unwrap_or_else(|p| p.into_inner());
        }
        state.0 = Some(owner);
        state.1 += 1;
        Ok(GateGuard {
            gate: self,
            _synchronous: PhantomData,
        })
    }
}
pub(super) struct GateGuard<'a> {
    gate: &'a TurnGate,
    _synchronous: PhantomData<Rc<()>>,
}
impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(|p| p.into_inner());
        state.1 -= 1;
        if state.1 == 0 {
            state.0 = None;
            self.gate.ready.notify_all();
        }
    }
}
pub(super) struct GatedConnection {
    pub(super) gate: TurnGate,
    connection: Mutex<Connection>,
}
impl GatedConnection {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            gate: TurnGate::new(),
            connection: Mutex::new(connection),
        }
    }
    pub(super) fn lock(&self) -> std::sync::LockResult<DatabaseGuard<'_>> {
        let gate = self.gate.lock().unwrap();
        match self.connection.lock() {
            Ok(connection) => Ok(DatabaseGuard {
                connection,
                _gate: gate,
            }),
            Err(error) => Err(std::sync::PoisonError::new(DatabaseGuard {
                connection: error.into_inner(),
                _gate: gate,
            })),
        }
    }
}
pub(super) struct DatabaseGuard<'a> {
    // Connection must unlock before ownership is released.
    connection: std::sync::MutexGuard<'a, Connection>,
    _gate: GateGuard<'a>,
}
impl std::ops::Deref for DatabaseGuard<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.connection
    }
}
impl std::ops::DerefMut for DatabaseGuard<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
}

/// Preserve independent immediate transactions, but compose admitted writes
/// into the enclosing node turn when one is active.
pub(super) enum JournalTransaction<'a> {
    Transaction(rusqlite::Transaction<'a>),
    Savepoint(rusqlite::Savepoint<'a>),
}
impl std::ops::Deref for JournalTransaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self {
            Self::Transaction(tx) => tx,
            Self::Savepoint(tx) => tx,
        }
    }
}
impl JournalTransaction<'_> {
    pub(super) fn commit(self) -> rusqlite::Result<()> {
        match self {
            Self::Transaction(tx) => tx.commit(),
            Self::Savepoint(tx) => tx.commit(),
        }
    }
}
pub(super) fn transaction(
    db: &mut Connection,
    behavior: rusqlite::TransactionBehavior,
) -> rusqlite::Result<JournalTransaction<'_>> {
    if db.is_autocommit() {
        db.transaction_with_behavior(behavior)
            .map(JournalTransaction::Transaction)
    } else {
        db.savepoint().map(JournalTransaction::Savepoint)
    }
}

struct FactTurn<'a> {
    storage: &'a NodeStorage,
    finished: bool,
    // Cannot cross a runtime await or migrate between worker threads.
    _synchronous: PhantomData<Rc<()>>,
}
impl NodeStorage {
    /// One synchronous reducer event, or a constituent of the owner's mined /
    /// connected event. A failed constituent poisons the enclosing barrier even
    /// if an internal caller mistakenly catches its error.
    pub(crate) fn fact_turn_or_join<T, E: From<StoreError>>(
        &self,
        action: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let _serial = self.turn.lock().unwrap();
        let _journal = self.store.db.gate.lock().unwrap();
        let joined = self.turn_releases.lock().unwrap().is_some();
        if !joined {
            return self.atomic_fact_turn(action);
        }
        let result = action();
        if result.is_err() {
            self.turn_failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
    /// Internal actor boundary only. All effects/replies must remain queued
    /// until this returns success. The closure must not await or publish state.
    pub(crate) fn atomic_fact_turn<T, E: From<StoreError>>(
        &self,
        action: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let _serial = self.turn.lock().unwrap();
        let _journal = self.store.db.gate.lock().unwrap();
        let mut releases = self.turn_releases.lock().unwrap();
        if releases.is_some() {
            return Err(StoreError::InvalidConfiguration("nested node fact turn".into()).into());
        }
        self.store
            .db
            .lock()
            .unwrap()
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(StoreError::from)?;
        *releases = Some(vec![]);
        self.turn_failed
            .store(false, std::sync::atomic::Ordering::Relaxed);
        drop(releases);
        let mut turn = FactTurn {
            storage: self,
            finished: false,
            _synchronous: PhantomData,
        };
        let value = action()?;
        if self.turn_failed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StoreError::Corrupt("constituent fact turn failed".into()).into());
        }
        self.store
            .db
            .lock()
            .unwrap()
            .execute_batch("COMMIT")
            .map_err(StoreError::from)?;
        turn.finished = true;
        let releases = self.turn_releases.lock().unwrap().take().unwrap();
        // Retention cleanup never changes a successful consensus verdict.
        for (scope, roots) in releases {
            let _ = self.broker.release_retained_roots(&scope, &roots);
        }
        Ok(value)
    }
    pub(super) fn retained_again(&self, scope: &str, roots: &Roots) {
        if let Some(pending) = self.turn_releases.lock().unwrap().as_mut() {
            for (old_scope, old_roots) in pending {
                if old_scope == scope {
                    old_roots.retain(|root| !roots.contains(root));
                }
            }
        }
    }
    pub(super) fn release_after_fact_commit(
        &self,
        scope: &str,
        roots: &Roots,
    ) -> Result<(), StoreError> {
        let mut pending = self.turn_releases.lock().unwrap();
        if let Some(pending) = pending.as_mut() {
            pending.push((scope.into(), roots.clone()));
            Ok(())
        } else {
            self.broker.release_retained_roots(scope, roots)?;
            Ok(())
        }
    }
}
impl Drop for FactTurn<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let db = self
                .storage
                .store
                .db
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let _ = db.execute_batch("ROLLBACK");
            self.storage
                .turn_releases
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
        }
    }
}
