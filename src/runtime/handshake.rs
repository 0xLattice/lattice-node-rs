use crate::core::wire::WireError;
use lattice_primitives::ChainRuntimeContext;
use serde::{Deserialize, Serialize};
pub const HELLO: &str = "lattice.overlay.hello.v1";
pub const TRANSACTION_AVAILABLE: &str = "lattice.overlay.transaction.available.v1";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainHandshake {
    #[serde(deserialize_with = "version")]
    pub version: u16,
    #[serde(rename = "nexusGenesisCID")]
    pub nexus_genesis_cid: String,
    pub chain_path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
}
impl ChainHandshake {
    pub fn root(genesis: &str) -> Self {
        Self {
            version: 6,
            nexus_genesis_cid: genesis.into(),
            chain_path: vec!["Nexus".into()],
            capabilities: Some(vec!["hello-updates".into(), "volume-bundle".into()]),
        }
    }
    fn valid(&self) -> bool {
        !self.nexus_genesis_cid.is_empty()
            && self.nexus_genesis_cid.len() <= 65535
            && self
                .nexus_genesis_cid
                .bytes()
                .all(|b| (0x21..=0x7e).contains(&b))
            && ChainRuntimeContext::new(
                self.chain_path.clone(),
                if self.chain_path.len() == 1 {
                    Some("bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia")
                } else {
                    None
                },
            )
            .is_ok()
    }
    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        if !self.valid() {
            return Err(WireError::Malformed);
        }
        let bytes =
            serde_json::to_vec(&serde_json::to_value(self).map_err(|_| WireError::Malformed)?)
                .map_err(|_| WireError::Malformed)?;
        if bytes.len() > 65536 {
            return Err(WireError::Oversized);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() > 65536 {
            return Err(WireError::Oversized);
        }
        let value = crate::core::wire::decode_json_value(bytes)?;
        let hello: Self = serde_json::from_value(value).map_err(|_| WireError::Malformed)?;
        if !hello.valid() {
            return Err(WireError::Malformed);
        }
        Ok(hello)
    }
    pub fn compatible(&self, genesis: &str) -> bool {
        self.version == 6 && self.nexus_genesis_cid == genesis && self.chain_path == ["Nexus"]
    }
}

fn version<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    u16::try_from(crate::core::wire::integer(d)?).map_err(serde::de::Error::custom)
}
