//! Cache policy shared by the public and authenticated read surfaces.
//! Mirrors LatticeNodeCommand.addPublicReadRoutes at the pinned Swift source.
use axum::{
    http::{HeaderValue, Method},
    response::Response,
};

pub(super) const IMMUTABLE: &str = "public, max-age=31536000, immutable";
pub(super) const STATUS: &str = "public, max-age=3";

fn policy(path: &str) -> Option<&'static str> {
    let parts: Vec<_> = path.split('/').filter(|v| !v.is_empty()).collect();
    match parts.as_slice() {
        ["transactions", _]
        | ["accounts", _]
        | ["volumes", _]
        | ["api", "chain", "genesis"]
        | ["api", "block", _, "transactions" | "children"] => Some(IMMUTABLE),
        ["api", "block", id] => Some(if *id == "latest" || id.parse::<u64>().is_ok() {
            STATUS
        } else {
            IMMUTABLE
        }),
        ["health"]
        | ["api", "transaction", _]
        | ["api", "state", "account", _]
        | ["api", "chain", "info" | "spec" | "children" | "endpoints"]
        | [
            "api",
            "blocks" | "mempool" | "fees" | "peers" | "deposits" | "deposit-state"
            | "receipt-state",
        ] => Some(STATUS),
        _ => None,
    }
}

pub(super) fn apply(response: &mut Response, method: &Method, path: &str) {
    // Missing content can appear later, so never give errors an immutable TTL.
    // Submission and operator mutations must not inherit a read's cache policy.
    if response.status().is_success()
        && (*method == Method::GET || *method == Method::HEAD)
        && let Some(policy) = policy(path)
    {
        response
            .headers_mut()
            .insert("cache-control", HeaderValue::from_static(policy));
    }
}
