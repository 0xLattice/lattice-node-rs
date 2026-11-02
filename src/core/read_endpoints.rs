//! Operator-declared read URLs are unverified claims, never consensus facts.
use serde::{Deserialize, Serialize};
pub const REQUEST: &str = "lattice.overlay.read-endpoint.request.v1";
pub const RESPONSE: &str = "lattice.overlay.read-endpoint.response.v1";
pub const RESPONSE_V2: &str = "lattice.overlay.read-endpoint.response.v2";
pub fn rendezvous(genesis: &str, path: &[String]) -> String {
    format!("lattice.read-endpoint.v1:{genesis}:{}", path.join("/"))
}
pub fn valid_url(value: &str) -> bool {
    if value.len() > 512 || !value.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return false;
    }
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https")
        || rest.contains(['?', '#'])
    {
        return false;
    }
    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return false;
    }
    let (host, port) = if authority.starts_with('[') {
        let Some(end) = authority.find(']') else {
            return false;
        };
        let tail = &authority[end + 1..];
        if !tail.is_empty() && !tail.starts_with(':') {
            return false;
        }
        (&authority[..=end], tail.strip_prefix(':'))
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        (host, Some(port))
    } else {
        (authority, None)
    };
    if host.is_empty() || port.is_some_and(|p| !p.bytes().all(|b| b.is_ascii_digit())) {
        return false;
    }
    // URLComponents accepts percent-encoded host characters (including spaces)
    // as declarations. Validate their syntax/UTF-8 without applying URL-client
    // normalization or imposing an unrelated DNS policy.
    let bytes = host.as_bytes();
    let mut decoded = Vec::new();
    let mut masked = String::new();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            if at + 2 >= bytes.len() {
                return false;
            }
            let Ok(value) = u8::from_str_radix(&host[at + 1..at + 3], 16) else {
                return false;
            };
            decoded.push(value);
            masked.push('x');
            at += 3;
        } else {
            decoded.push(bytes[at]);
            masked.push(bytes[at] as char);
            at += 1;
        }
    }
    std::str::from_utf8(&decoded).is_ok() && masked.parse::<axum::http::uri::Authority>().is_ok()
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeclaredReadEndpoint {
    pub url: String,
    pub accepts_submit: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadEndpointRequest {
    pub chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "super::wire::integer")]
    pub request_id: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadEndpointResponse {
    pub chain_path: Vec<String>,
    #[serde(rename = "requestID", deserialize_with = "super::wire::integer")]
    pub request_id: u64,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepts_submit: Option<bool>,
}
pub fn valid_path(path: &[String]) -> bool {
    path == ["Nexus"] || lattice_primitives::ChainRuntimeContext::new(path.to_vec(), None).is_ok()
}
pub fn encode<T: Serialize>(message: &T) -> Result<Vec<u8>, super::wire::WireError> {
    super::wire::json(message)
}
pub fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Option<T> {
    if bytes.len() > super::wire::MAX_MESSAGE_BYTES {
        return None;
    }
    let decoded = serde_json::from_value(super::wire::decode_json_value(bytes).ok()?).ok()?;
    (encode(&decoded).ok()?.as_slice() == bytes).then_some(decoded)
}
