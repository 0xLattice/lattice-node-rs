#[path = "wire_json.rs"]
mod foundation_json;
// Exact canonical-JSON overlay envelopes from ChainSyncWire.swift. Embedded
// block/index/spec nodes remain canonical DAG-CBOR, proofs their sealed binary.
use super::sync::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_block_tree::ChildBlockProof;
use lattice_cashew::{canonical_cid, cbor};
use lattice_primitives::{Block, BlockHeader, ChainRuntimeContext, ChainSpec};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024 - 256;
pub const STREAM_REQUEST: &str = "lattice.overlay.stream.request.v1";
pub const STREAM_PAGE: &str = "lattice.overlay.stream.page.v1";
pub const DATA_REQUEST: &str = "lattice.overlay.data.request.v1";
pub const HEADERS_RESPONSE: &str = "lattice.overlay.headers.response.v1";
pub const ANCESTORS_REQUEST: &str = "lattice.overlay.ancestors.request.v1";
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("overlay frame too large")]
    Oversized,
    #[error("malformed overlay frame")]
    Malformed,
    #[error("noncanonical overlay JSON")]
    NonCanonical,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Bytes(Vec<u8>);
impl Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(&self.0))
    }
}
impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD
            .decode(s)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}
pub(crate) fn integer<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("unsigned integer")
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(E::custom)
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<u64, E> {
            if v.is_finite() && (0. ..18446744073709551616.).contains(&v) && v.fract() == 0. {
                Ok(v as u64)
            } else {
                Err(E::custom("integer out of range"))
            }
        }
    }
    d.deserialize_any(Visitor)
}
fn maximum_integer<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    i64::try_from(integer(d)?).map_err(serde::de::Error::custom)
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamRequest {
    chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "integer")]
    request_id: u64,
    #[serde(rename = "logID", skip_serializing_if = "Option::is_none")]
    log_id: Option<String>,
    #[serde(deserialize_with = "integer")]
    after: u64,
    own: String,
}
#[derive(Serialize, Deserialize)]
struct WireEntry {
    #[serde(deserialize_with = "integer")]
    position: u64,
    proof: bool,
    cid: String,
    block: String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "integer")]
    request_id: u64,
    #[serde(rename = "logID")]
    log_id: String,
    entries: Vec<WireEntry>,
    has_more: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataRequest {
    chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "integer")]
    request_id: u64,
    cids: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AncestorsRequest {
    chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "integer")]
    request_id: u64,
    cid: String,
    #[serde(deserialize_with = "maximum_integer")]
    maximum: i64,
}
#[derive(Serialize, Deserialize)]
struct WireHeader {
    block: Bytes,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Bytes>,
    proofs: Vec<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    spec: Option<Bytes>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Headers {
    chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "integer")]
    request_id: u64,
    entries: Vec<WireHeader>,
    has_more: bool,
}
fn path(path: &[String]) -> bool {
    let placeholder = "bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia";
    ChainRuntimeContext::new(
        path.to_vec(),
        if path.len() == 1 {
            Some(placeholder)
        } else {
            None
        },
    )
    .is_ok()
}
fn cid(s: &str) -> bool {
    s.len() <= 128 && canonical_cid(s).is_ok_and(|c| c == s)
}
fn log(s: &str) -> bool {
    s.len() <= 128 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
}
pub(crate) fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, WireError> {
    serde_json::to_value(value)
        .and_then(|v| serde_json::to_vec(&v))
        .map_err(|_| WireError::Malformed)
}
trait Envelope: Serialize + DeserializeOwned {
    fn valid(&self) -> bool;
}
impl Envelope for StreamRequest {
    fn valid(&self) -> bool {
        path(&self.chain_path) && self.log_id.as_ref().is_none_or(|s| log(s)) && log(&self.own)
    }
}
impl Envelope for Page {
    fn valid(&self) -> bool {
        path(&self.chain_path)
            && log(&self.log_id)
            && (!self.log_id.is_empty() || self.entries.is_empty())
            && self.entries.len() <= 4096
            && self
                .entries
                .iter()
                .all(|e| cid(&e.cid) && cid(&e.block) && (e.proof || e.cid == e.block))
    }
}
impl Envelope for DataRequest {
    fn valid(&self) -> bool {
        path(&self.chain_path) && self.cids.len() <= 4096 && self.cids.iter().all(|s| cid(s))
    }
}
impl Envelope for AncestorsRequest {
    fn valid(&self) -> bool {
        path(&self.chain_path) && cid(&self.cid) && (1..=2048).contains(&self.maximum)
    }
}
fn node<T: DeserializeOwned + Serialize>(b: &Bytes) -> Result<T, WireError> {
    let value: T = cbor::decode(&b.0).map_err(|_| WireError::Malformed)?;
    if cbor::encode(&value).map_err(|_| WireError::Malformed)? != b.0 {
        return Err(WireError::Malformed);
    }
    Ok(value)
}
impl WireHeader {
    fn decoded(&self) -> Result<HeaderEntry, WireError> {
        let proofs = self
            .proofs
            .iter()
            .map(|bytes| {
                let p = ChildBlockProof::deserialize(&bytes.0).ok_or(WireError::Malformed)?;
                if p.serialize().map_err(|_| WireError::Malformed)? != bytes.0 {
                    return Err(WireError::Malformed);
                }
                Ok(p)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(HeaderEntry {
            block: node::<Block>(&self.block)?,
            children: self
                .children
                .as_ref()
                .map(node::<lattice_cashew::FlatDictionary<BlockHeader>>)
                .transpose()?,
            proofs,
            spec: self.spec.as_ref().map(node::<ChainSpec>).transpose()?,
        })
    }
}
impl Envelope for Headers {
    fn valid(&self) -> bool {
        path(&self.chain_path)
            && self.entries.len() <= 4096
            && self.entries.iter().all(|e| e.decoded().is_ok())
    }
}
fn encode_envelope<T: Envelope>(value: &T) -> Result<Vec<u8>, WireError> {
    if !value.valid() {
        return Err(WireError::Malformed);
    }
    let bytes = json(value)?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(WireError::Oversized);
    }
    Ok(bytes)
}
fn decode_envelope<T: Envelope>(bytes: &[u8]) -> Result<T, WireError> {
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(WireError::Oversized);
    }
    let value: T =
        serde_json::from_value(foundation_json::parse(bytes).map_err(|_| WireError::Malformed)?)
            .map_err(|_| WireError::Malformed)?;
    if !value.valid() {
        return Err(WireError::Malformed);
    }
    if json(&value)? != bytes {
        return Err(WireError::NonCanonical);
    }
    Ok(value)
}
pub fn encode(
    message: &SyncMessage,
    chain_path: &[String],
) -> Result<(&'static str, Vec<u8>), WireError> {
    let chain_path = chain_path.to_vec();
    Ok(match message {
        SyncMessage::GetStream {
            request_id,
            log_id,
            after,
            own,
        } => (
            STREAM_REQUEST,
            encode_envelope(&StreamRequest {
                chain_path,
                request_id: *request_id,
                log_id: log_id.clone(),
                after: *after,
                own: own.clone(),
            })?,
        ),
        SyncMessage::Stream(page) => (
            STREAM_PAGE,
            encode_envelope(&Page {
                chain_path,
                request_id: page.request_id,
                log_id: page.log_id.clone(),
                entries: page
                    .entries
                    .iter()
                    .map(|e| WireEntry {
                        position: e.position,
                        proof: e.entry.kind == LogKind::Proof,
                        cid: e.entry.cid.clone(),
                        block: e.entry.block.clone(),
                    })
                    .collect(),
                has_more: page.has_more,
            })?,
        ),
        SyncMessage::GetData { request_id, cids } => (
            DATA_REQUEST,
            encode_envelope(&DataRequest {
                chain_path,
                request_id: *request_id,
                cids: cids.clone(),
            })?,
        ),
        SyncMessage::GetAncestors {
            request_id,
            cid,
            max,
        } => (
            ANCESTORS_REQUEST,
            encode_envelope(&AncestorsRequest {
                chain_path,
                request_id: *request_id,
                cid: cid.clone(),
                maximum: (*max).clamp(1, 2048) as i64,
            })?,
        ),
        SyncMessage::Headers(response) => {
            let entries = response
                .entries
                .iter()
                .map(|e| {
                    Ok(WireHeader {
                        block: Bytes(cbor::encode(&e.block).map_err(|_| WireError::Malformed)?),
                        children: e
                            .children
                            .as_ref()
                            .map(|c| cbor::encode(c).map(Bytes).map_err(|_| WireError::Malformed))
                            .transpose()?,
                        proofs: e
                            .proofs
                            .iter()
                            .map(|p| p.serialize().map(Bytes).map_err(|_| WireError::Malformed))
                            .collect::<Result<Vec<_>, _>>()?,
                        spec: e
                            .spec
                            .as_ref()
                            .map(|c| cbor::encode(c).map(Bytes).map_err(|_| WireError::Malformed))
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, WireError>>()?;
            (
                HEADERS_RESPONSE,
                encode_envelope(&Headers {
                    chain_path,
                    request_id: response.request_id,
                    entries,
                    has_more: response.has_more,
                })?,
            )
        }
    })
}
pub fn decode(
    topic: &str,
    payload: &[u8],
) -> Result<Option<(Vec<String>, SyncMessage)>, WireError> {
    Ok(Some(match topic {
        STREAM_REQUEST => {
            let m: StreamRequest = decode_envelope(payload)?;
            (
                m.chain_path,
                SyncMessage::GetStream {
                    request_id: m.request_id,
                    log_id: m.log_id,
                    after: m.after,
                    own: m.own,
                },
            )
        }
        STREAM_PAGE => {
            let m: Page = decode_envelope(payload)?;
            (
                m.chain_path,
                SyncMessage::Stream(StreamPage {
                    request_id: m.request_id,
                    log_id: m.log_id,
                    entries: m
                        .entries
                        .into_iter()
                        .map(|e| StreamEntry {
                            position: e.position,
                            entry: LogEntry {
                                kind: if e.proof {
                                    LogKind::Proof
                                } else {
                                    LogKind::Header
                                },
                                cid: e.cid,
                                block: e.block,
                            },
                        })
                        .collect(),
                    has_more: m.has_more,
                }),
            )
        }
        DATA_REQUEST => {
            let m: DataRequest = decode_envelope(payload)?;
            (
                m.chain_path,
                SyncMessage::GetData {
                    request_id: m.request_id,
                    cids: m.cids,
                },
            )
        }
        ANCESTORS_REQUEST => {
            let m: AncestorsRequest = decode_envelope(payload)?;
            (
                m.chain_path,
                SyncMessage::GetAncestors {
                    request_id: m.request_id,
                    cid: m.cid,
                    max: m.maximum as usize,
                },
            )
        }
        HEADERS_RESPONSE => {
            let m: Headers = decode_envelope(payload)?;
            (
                m.chain_path,
                SyncMessage::Headers(HeadersResponse {
                    request_id: m.request_id,
                    entries: m
                        .entries
                        .iter()
                        .map(WireHeader::decoded)
                        .collect::<Result<_, _>>()?,
                    has_more: m.has_more,
                }),
            )
        }
        _ => return Ok(None),
    }))
}

pub(crate) fn decode_json_value(bytes: &[u8]) -> Result<serde_json::Value, WireError> {
    foundation_json::parse(bytes).map_err(|_| WireError::Malformed)
}
