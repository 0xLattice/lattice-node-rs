//! Swift PublicVolumeFetches: one remote read and one shared archive per CID.
//! Completed responses are not cached or pinned by this coordinator.
use super::api::VolumeFuture;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, watch};

pub const MAXIMUM_ARCHIVE_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchAnswer {
    Archive(Arc<Vec<u8>>),
    NotServed,
    Busy,
}
/// The transport must authenticate the requested CID before returning bytes.
pub trait ArchiveFetcher: Send + Sync + 'static {
    fn fetch(&self, cid: String) -> VolumeFuture;
}
type Receiver = watch::Receiver<Option<FetchAnswer>>;
#[derive(Clone)]
pub struct PublicVolumeFetches {
    limit: usize,
    fetcher: Arc<dyn ArchiveFetcher>,
    active: Arc<Mutex<BTreeMap<String, Receiver>>>,
}
impl PublicVolumeFetches {
    pub fn effective_limit(requested: usize, in_flight_volume_bytes: usize) -> usize {
        if requested == 0 {
            return 0;
        }
        requested
            .min(in_flight_volume_bytes / 2 / MAXIMUM_ARCHIVE_BYTES)
            .max(1)
    }
    pub fn new(
        requested: usize,
        in_flight_volume_bytes: usize,
        fetcher: Arc<dyn ArchiveFetcher>,
    ) -> Self {
        Self {
            limit: Self::effective_limit(requested, in_flight_volume_bytes),
            fetcher,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub fn enabled(&self) -> bool {
        self.limit > 0
    }
    pub async fn archive(&self, cid: &str) -> FetchAnswer {
        let mut active = self.active.lock().await;
        let mut receiver = if let Some(shared) = active.get(cid) {
            shared.clone()
        } else {
            if active.len() >= self.limit {
                return FetchAnswer::Busy;
            }
            let (sender, receiver) = watch::channel(None);
            active.insert(cid.into(), receiver.clone());
            let fetcher = self.fetcher.clone();
            let active = self.active.clone();
            let cid = cid.to_owned();
            // Callers can disconnect without cancelling the task shared by
            // others. The deadline also releases the slot if the host stalls.
            tokio::spawn(async move {
                let result =
                    match tokio::time::timeout(Duration::from_secs(30), fetcher.fetch(cid.clone()))
                        .await
                    {
                        Ok(Ok(Some(bytes))) if bytes.len() <= MAXIMUM_ARCHIVE_BYTES => {
                            FetchAnswer::Archive(Arc::new(bytes))
                        }
                        Ok(Ok(None)) | Ok(Ok(Some(_))) => FetchAnswer::NotServed,
                        _ => FetchAnswer::Busy,
                    };
                let mut active = active.lock().await;
                active.remove(&cid);
                sender.send_replace(Some(result));
            });
            receiver
        };
        drop(active);
        loop {
            if let Some(answer) = receiver.borrow_and_update().as_ref() {
                return answer.clone();
            }
            if receiver.changed().await.is_err() {
                return FetchAnswer::Busy;
            }
        }
    }
}

/// Authenticate the operator retrieval adapter's content again at the public
/// boundary before encoding the canonical complete-volume archive.
pub fn decode_rpc_archive(cid: &str, value: &serde_json::Value) -> Option<Vec<u8>> {
    use base64::Engine;
    if value.get("cid")?.as_str()? != cid {
        return None;
    }
    let encoded = value.get("entries")?.as_object()?;
    if encoded.is_empty() || encoded.len() > u16::MAX as usize {
        return None;
    }
    let mut entries = BTreeMap::new();
    let mut size = 2usize;
    for (key, data) in encoded {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data.as_str()?)
            .ok()?;
        size = size.checked_add(6 + key.len())?.checked_add(bytes.len())?;
        if size > MAXIMUM_ARCHIVE_BYTES {
            return None;
        }
        entries.insert(key.clone(), bytes);
    }
    let volume = lattice_cashew::SerializedVolume::new(cid, entries);
    volume.validate().ok()?;
    ivy_core::Volume {
        root_cid: cid.into(),
        entries: volume
            .entries
            .into_iter()
            .map(|(cid, data)| ivy_core::ContentEntry { cid, data })
            .collect(),
    }
    .encode()
}
