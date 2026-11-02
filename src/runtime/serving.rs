//! ServingLedger.swift persistence boundary: operator credit is synced before
//! it changes the shared Tally, while ordinary serving deltas save periodically.
use lattice_tally::{PeerId, Tally};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
#[derive(Debug, thiserror::Error)]
pub enum ServingError {
    #[error("invalidPeer")]
    InvalidPeer,
    #[error("invalidBytes")]
    InvalidBytes,
    #[error("notSaved")]
    NotSaved,
    #[error("unreadable serving ledger: {0}")]
    Unreadable(PathBuf),
}
#[derive(Serialize, Deserialize)]
struct Saved {
    version: u8,
    balances: BTreeMap<String, i64>,
}
pub struct ServingLedger {
    path: PathBuf,
    tally: Tally,
    written: BTreeMap<String, i64>,
}
impl ServingLedger {
    pub fn open(directory: &Path) -> Result<Self, ServingError> {
        let path = directory.join("serving.json");
        let tally = Tally::default();
        match std::fs::read(&path) {
            Ok(bytes) => {
                let saved: Saved = serde_json::from_slice(&bytes)
                    .map_err(|_| ServingError::Unreadable(path.clone()))?;
                if saved.version != 1 {
                    return Err(ServingError::Unreadable(path));
                }
                for (key, bytes) in saved.balances {
                    let peer = peer(&key).map_err(|_| ServingError::Unreadable(path.clone()))?;
                    tally.credit_standing(&peer, bytes);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ServingError::Unreadable(path)),
        }
        let written = standings(&tally);
        Ok(Self {
            path,
            tally,
            written,
        })
    }
    pub fn tally(&self) -> Tally {
        self.tally.clone()
    }
    pub fn standing(&self, key: &str) -> Result<i64, ServingError> {
        Ok(self.tally.standing(&peer(key)?))
    }
    pub fn credit(&mut self, key: &str, bytes: i64) -> Result<i64, ServingError> {
        let peer = peer(key)?;
        if bytes <= 0 {
            return Err(ServingError::InvalidBytes);
        }
        let mut balances = standings(&self.tally);
        let sum = balances
            .get(key)
            .copied()
            .unwrap_or(0)
            .saturating_add(bytes);
        balances.insert(key.into(), sum);
        self.write(balances)?;
        self.tally.credit_standing(&peer, bytes);
        let _ = self.save();
        Ok(self.tally.standing(&peer))
    }
    pub fn save(&mut self) -> Result<(), ServingError> {
        let current = standings(&self.tally);
        if current != self.written {
            self.write(current)?;
        }
        Ok(())
    }
    fn write(&mut self, balances: BTreeMap<String, i64>) -> Result<(), ServingError> {
        let result = (|| -> std::io::Result<()> {
            let data = serde_json::to_vec(&Saved {
                version: 1,
                balances: balances.clone(),
            })?;
            let temp = self.path.with_extension(format!(
                "tmp-{:016x}",
                rand_core::RngCore::next_u64(&mut rand_core::OsRng)
            ));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp)?;
            let result = (|| {
                file.write_all(&data)?;
                file.sync_all()?;
                std::fs::rename(&temp, &self.path)?;
                File::open(self.path.parent().unwrap())?.sync_all()
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(temp);
            }
            result
        })();
        result.map_err(|_| ServingError::NotSaved)?;
        self.written = balances;
        Ok(())
    }
}
fn peer(key: &str) -> Result<PeerId, ServingError> {
    if ivy_core::PeerKey::from_hex(key).is_some_and(|p| p.hex() == key) {
        Ok(PeerId::new(key))
    } else {
        Err(ServingError::InvalidPeer)
    }
}
fn standings(tally: &Tally) -> BTreeMap<String, i64> {
    tally
        .standings()
        .into_iter()
        .map(|(p, n)| (p.public_key().to_owned(), n))
        .collect()
}
