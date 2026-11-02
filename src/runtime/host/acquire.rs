//! Lazy dependency reads: bounded blocking workers ask the asynchronous host.
use super::*;
use crate::runtime::acquisition::{Reservation, SESSION_BYTES};
use lattice_cashew::ContentSource;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(super) struct Request {
    pub root: String,
    pub reserve_only: bool,
    pub cancelled: Arc<AtomicBool>,
    pub reserved: Option<Arc<Reservation>>,
    pub reply: oneshot::Sender<Answer>,
}
pub(super) struct Answer {
    pub volumes: Vec<SerializedVolume>,
    pub reservation: Option<Arc<Reservation>>,
}
#[derive(Default)]
struct State {
    attempted: BTreeSet<String>,
    entries: BTreeMap<String, Vec<u8>>,
    volumes: BTreeMap<String, Vec<String>>,
    bytes: usize,
    reservation: Option<Arc<Reservation>>,
}
pub(super) struct Session {
    pub(super) cancelled: Arc<AtomicBool>,
    requests: mpsc::Sender<Request>,
    state: Mutex<State>,
    failure: Mutex<Option<crate::storage::StoreError>>,
    cache: Arc<Mutex<super::super::content_cache::ContentCache>>,
}
impl Session {
    pub(super) fn new(
        requests: mpsc::Sender<Request>,
        cache: Arc<Mutex<super::super::content_cache::ContentCache>>,
    ) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            requests,
            cache,
            failure: Mutex::new(None),
            state: Mutex::new(State::default()),
        }
    }
    pub(super) fn check(&self) -> Result<(), crate::storage::StoreError> {
        match self.failure.lock().unwrap().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    pub(super) fn entries(&self) -> BTreeMap<String, Vec<u8>> {
        self.state.lock().unwrap().entries.clone()
    }
    pub(super) fn volumes(&self) -> Vec<SerializedVolume> {
        let state = self.state.lock().unwrap();
        state
            .volumes
            .iter()
            .map(|(root, cids)| {
                SerializedVolume::new(
                    root.clone(),
                    cids.iter()
                        .filter_map(|cid| {
                            state
                                .entries
                                .get(cid)
                                .map(|bytes| (cid.clone(), bytes.clone()))
                        })
                        .collect(),
                )
            })
            .collect()
    }
}
impl ContentSource for Session {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        let missing = || lattice_cashew::Error::MissingContent(cid.into());
        if self.cancelled.load(Ordering::Acquire) {
            return Err(missing());
        }
        let mut state = self.state.lock().unwrap();
        if let Some(bytes) = state.entries.get(cid) {
            return Ok(bytes.clone());
        }
        let cached = self.cache.lock().unwrap().fetch(cid).filter(|bytes| {
            // A block header's ordinary children belong to its whole Volume.
            // An entry-only cache hit cannot promise that boundary: warm it
            // from peers as Swift reading(root) does before resolving a block.
            lattice_cashew::cbor::decode::<Block>(bytes).is_err()
        });
        if let Some(bytes) = cached {
            // Retained per-job copies share the same global budget as remote
            // answers. Snapshot-only reads never reserve acquisition capacity.
            if state.reservation.is_none() {
                let (reply, response) = oneshot::channel();
                self.requests
                    .blocking_send(Request {
                        cancelled: self.cancelled.clone(),
                        root: cid.into(),
                        reserve_only: true,
                        reserved: None,
                        reply,
                    })
                    .map_err(|_| missing())?;
                state.reservation = response.blocking_recv().map_err(|_| missing())?.reservation;
                if state.reservation.is_none() {
                    return Err(missing());
                }
            }
            let charge = cid.len() + bytes.len() + 512;
            if charge > SESSION_BYTES.saturating_sub(state.bytes) {
                return Err(missing());
            }
            state.bytes += charge;
            state.entries.insert(cid.into(), bytes.clone());
            return Ok(bytes);
        }
        if state.attempted.contains(cid) {
            return Err(missing());
        }
        let attempt_charge = cid.len() + 512;
        if attempt_charge > SESSION_BYTES.saturating_sub(state.bytes) {
            return Err(missing());
        }
        state.bytes += attempt_charge;
        state.attempted.insert(cid.into());
        let (reply, response) = oneshot::channel();
        self.requests
            .blocking_send(Request {
                cancelled: self.cancelled.clone(),
                root: cid.into(),
                reserve_only: false,
                reserved: state.reservation.clone(),
                reply,
            })
            .map_err(|_| missing())?;
        let answer = response.blocking_recv().map_err(|_| missing())?;
        if let Some(reservation) = answer.reservation {
            state.reservation = Some(reservation);
        }
        for volume in answer.volumes {
            if volume.validate().is_err() {
                continue;
            }
            let charge = volume
                .entries
                .iter()
                .filter(|(cid, _)| !state.entries.contains_key(*cid))
                .try_fold(0usize, |n, (cid, bytes)| {
                    n.checked_add(cid.len() + bytes.len() + 512)
                });
            let Some(charge) = charge else {
                return Err(missing());
            };
            if charge > SESSION_BYTES.saturating_sub(state.bytes) {
                return Err(missing());
            }
            state.bytes += charge;
            state.volumes.insert(
                volume.root.clone(),
                volume.entries.keys().cloned().collect(),
            );
            state.entries.extend(volume.entries);
        }
        let bytes = state.entries.get(cid).cloned().ok_or_else(missing)?;
        self.cache
            .lock()
            .unwrap()
            .remember(cid.into(), bytes.clone());
        Ok(bytes)
    }
}
pub(super) struct Source<'a> {
    pub snapshot: &'a lattice_volume_broker::DiskReadSnapshot,
    pub session: &'a Session,
}
impl ContentSource for Source<'_> {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        // The immutable snapshot is never refreshed underneath a validation.
        match self
            .snapshot
            .fetch_data(&[cid.to_owned()].into_iter().collect())
        {
            Ok(mut entries) => match entries.remove(cid) {
                Some(bytes) => Ok(bytes),
                None => self.session.fetch(cid),
            },
            Err(error) => {
                *self.session.failure.lock().unwrap() = Some(error.into());
                Err(lattice_cashew::Error::MissingContent(cid.into()))
            }
        }
    }
}
impl Host {
    pub(super) async fn acquire_dependency(&mut self, request: Request) -> anyhow::Result<()> {
        if request.reply.is_closed() {
            return Ok(());
        }
        let id = self.id()?;
        self.fetches
            .insert(id, Fetch::Worker(request.reply, request.cancelled));
        if request.reserve_only {
            self.network.reserve_worker(id);
        } else if let Some(reservation) = request.reserved {
            self.network
                .fetch_reserved(id, request.root, reservation)
                .await?;
        } else {
            self.network.fetch(id, request.root, false, None).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::acquisition::{Priority, Queue};
    #[tokio::test(flavor = "current_thread")]
    async fn lazy_session_reuses_one_global_reservation_and_releases_after_consumption() {
        let (tx, mut rx) = mpsc::channel::<Request>(1);
        let session = Session::new(
            tx,
            Arc::new(Mutex::new(
                super::super::super::content_cache::ContentCache::new(0),
            )),
        );
        let mut queue = Queue::new(SESSION_BYTES);
        queue.push(SESSION_BYTES, Priority::OwnWork, ());
        let (_, reservation) = queue.pop().unwrap();
        let first = SerializedVolume::new(
            lattice_cashew::cid_for_bytes(0x55, b"first"),
            BTreeMap::from([(
                lattice_cashew::cid_for_bytes(0x55, b"first"),
                b"first".to_vec(),
            )]),
        );
        let second = SerializedVolume::new(
            lattice_cashew::cid_for_bytes(0x55, b"second"),
            BTreeMap::from([(
                lattice_cashew::cid_for_bytes(0x55, b"second"),
                b"second".to_vec(),
            )]),
        );
        let a = first.root.clone();
        let b = second.root.clone();
        let worker = tokio::task::spawn_blocking(move || {
            assert_eq!(session.fetch(&a).unwrap(), b"first");
            assert_eq!(session.fetch(&b).unwrap(), b"second");
            assert_eq!(session.fetch(&a).unwrap(), b"first");
            session
        });
        let request = rx.recv().await.unwrap();
        assert!(request.reserved.is_none());
        request
            .reply
            .send(Answer {
                volumes: vec![first],
                reservation: Some(Arc::new(reservation)),
            })
            .ok()
            .unwrap();
        let request = rx.recv().await.unwrap();
        assert!(request.reserved.is_some());
        queue.push(SESSION_BYTES, Priority::OwnWork, ());
        assert!(queue.pop().is_none());
        request
            .reply
            .send(Answer {
                volumes: vec![second],
                reservation: request.reserved,
            })
            .ok()
            .unwrap();
        let session = worker.await.unwrap();
        assert_eq!(queue.used(), SESSION_BYTES);
        assert_eq!(session.volumes().len(), 2);
        assert!(rx.try_recv().is_err());
        drop(session);
        assert_eq!(queue.used(), 0);
        assert!(queue.pop().is_some());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_actor_reply_unblocks_worker_without_invalid_verdict() {
        let (tx, mut rx) = mpsc::channel::<Request>(1);
        let session = Session::new(
            tx,
            Arc::new(Mutex::new(
                super::super::super::content_cache::ContentCache::new(0),
            )),
        );
        let worker = tokio::task::spawn_blocking(move || session.fetch("missing"));
        let request = rx.recv().await.unwrap();
        drop(request);
        assert!(matches!(
            worker.await.unwrap(),
            Err(lattice_cashew::Error::MissingContent(_))
        ));
    }
}
