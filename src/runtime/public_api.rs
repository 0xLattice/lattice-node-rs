//! Optional public reads: explicit allowlist, observed-IP budgets, bounded
//! transport, coalesced root health snapshots, and opt-in ordinary transaction submit.
use super::{
    api::{self, RpcBackend},
    public_rate::{RateLimiter, RateLimits, RouteClass},
};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
#[derive(Clone)]
struct PublicState {
    submit: bool,
    fetches: super::public_fetch::PublicVolumeFetches,
    backend: Arc<dyn RpcBackend>,
    limiter: Arc<std::sync::Mutex<RateLimiter>>,
    start: Instant,
    slots: Arc<Semaphore>,
    health: Arc<Mutex<HealthCache>>,
}
pub fn router(backend: Arc<dyn RpcBackend>, limits: RateLimits) -> anyhow::Result<Router> {
    router_with_submit(backend, limits, false)
}
pub fn router_with_submit(
    backend: Arc<dyn RpcBackend>,
    limits: RateLimits,
    submit: bool,
) -> anyhow::Result<Router> {
    router_with_fetches(backend, limits, submit, 4)
}
pub fn router_with_fetches(
    backend: Arc<dyn RpcBackend>,
    limits: RateLimits,
    submit: bool,
    fetches: usize,
) -> anyhow::Result<Router> {
    router_with_fetch_budget(
        backend,
        limits,
        submit,
        fetches,
        super::acquisition::ResourcePolicy::default().serving_max_in_flight_volume_bytes,
    )
}
/// The daemon supplies its configured Ivy serving budget here, so public
/// retrievals retain the same half-budget headroom as Swift PublicVolumeFetches.
pub fn router_with_fetch_budget(
    backend: Arc<dyn RpcBackend>,
    limits: RateLimits,
    submit: bool,
    fetches: usize,
    in_flight_volume_bytes: usize,
) -> anyhow::Result<Router> {
    let limiter = RateLimiter::new(limits, 0.).map_err(anyhow::Error::msg)?;
    Ok(Router::new()
        .fallback(any(dispatch))
        .with_state(PublicState {
            submit,
            fetches: crate::runtime::public_fetch::PublicVolumeFetches::new(
                fetches,
                in_flight_volume_bytes,
                Arc::new(BackendFetcher(backend.clone())),
            ),
            backend,
            limiter: Arc::new(std::sync::Mutex::new(limiter)),
            start: Instant::now(),
            slots: Arc::new(Semaphore::new(64)),
            health: Arc::new(Mutex::new(HealthCache::default())),
        }))
}
fn allowed(path: &str) -> bool {
    let p: Vec<_> = path.split('/').filter(|v| !v.is_empty()).collect();
    matches!(
        p.as_slice(),
        ["health"]
            | ["transactions", _]
            | ["volumes", _]
            | ["accounts", _]
            | ["api", "block", _]
            | ["api", "block", _, "transactions" | "children"]
            | ["api", "transaction", _]
            | ["api", "state", "account", _]
            | [
                "api",
                "chain",
                "info" | "spec" | "genesis" | "children" | "endpoints"
            ]
            | [
                "api",
                "blocks"
                    | "mempool"
                    | "fees"
                    | "peers"
                    | "deposits"
                    | "deposit-state"
                    | "receipt-state"
            ]
    )
}
async fn dispatch(
    State(state): State<PublicState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut request: Request,
) -> Response {
    // Normalize exactly the same empty-component aliases used by classification.
    let path = format!(
        "/{}",
        request
            .uri()
            .path()
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>()
            .join("/")
    );
    let uri = match request.uri().query() {
        Some(q) => format!("{path}?{q}"),
        None => path.clone(),
    };
    let Ok(uri) = uri.parse::<Uri>() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    *request.uri_mut() = uri;
    let method = request.method().clone();
    let mut response = if request.method() == Method::OPTIONS {
        // Preflight never spends a bucket and never permits public mutation.
        let permitted = request
            .headers()
            .get("access-control-request-method")
            .is_none_or(|m| {
                m == "GET" || m == "HEAD" || m == "OPTIONS" || (state.submit && m == "POST")
            });
        if permitted {
            StatusCode::NO_CONTENT.into_response()
        } else {
            StatusCode::FORBIDDEN.into_response()
        }
    } else {
        let class = RouteClass::classify(request.method().as_str(), &path);
        // IPv6 clients share a /64 budget. IPv4 is keyed without source port.
        let client = match peer.ip() {
            std::net::IpAddr::V4(ip) => ip.to_string(),
            std::net::IpAddr::V6(ip) => {
                ip.to_ipv4_mapped()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "{:x}:{:x}:{:x}:{:x}/64",
                            ip.segments()[0],
                            ip.segments()[1],
                            ip.segments()[2],
                            ip.segments()[3]
                        )
                    })
            }
        };
        let admitted = state.limiter.lock().unwrap().admit(
            &client,
            class,
            state.start.elapsed().as_secs_f64(),
        );
        if !admitted {
            StatusCode::TOO_MANY_REQUESTS.into_response()
        } else if !(state.submit && path == "/transactions" && request.method() == Method::POST)
            && (!allowed(&path)
                || !(request.method() == Method::GET
                    || (path == "/health" && request.method() == Method::HEAD)))
        {
            StatusCode::NOT_FOUND.into_response()
        } else if let Some(cid) = path.strip_prefix("/volumes/") {
            public_volume(&state, cid, request.uri().query()).await
        } else if path == "/health" {
            let is_head = request.method() == Method::HEAD;
            // Cache only the root snapshot. Both scoped and root health read
            // published state independently of the runtime command queue.
            let query = request.uri().query();
            let root = query.is_none_or(|q| {
                form_urlencoded::parse(q.as_bytes())
                    .find(|(k, _)| k == "chainPath")
                    .is_none_or(|(_, v)| v == "Nexus")
            });
            let route = query.map_or_else(|| "/status".into(), |q| format!("/status?{q}"));
            let mut response = if root {
                health(&state, route).await
            } else {
                api::health_response(&state.backend, route).await
            };
            if is_head {
                *response.body_mut() = Body::empty();
            }
            response
        } else {
            api::serve_with_submit(&state.backend, &state.slots, request, Some(state.submit)).await
        }
    };
    super::read_cache::apply(&mut response, &method, &path);
    response.headers_mut().insert(
        "access-control-allow-methods",
        HeaderValue::from_static(if state.submit {
            "GET, HEAD, POST, OPTIONS"
        } else {
            "GET, HEAD, OPTIONS"
        }),
    );
    response.headers_mut().insert(
        "access-control-allow-headers",
        HeaderValue::from_static("Content-Type"),
    );
    cors(response)
}
fn cors(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("access-control-allow-origin", HeaderValue::from_static("*"));
    response
}

#[derive(Default)]
struct HealthCache {
    ready: Option<(Instant, u16, Value)>,
    loading: Option<tokio::sync::watch::Receiver<Option<(u16, Value)>>>,
}
async fn health(state: &PublicState, route: String) -> Response {
    let mut cache = state.health.lock().await;
    if let Some((at, code, value)) = &cache.ready
        && at.elapsed() < Duration::from_secs(3)
    {
        return api::bounded_json(
            StatusCode::from_u16(*code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            value,
            4 << 20,
        );
    }
    let mut receiver = if let Some(receiver) = &cache.loading {
        receiver.clone()
    } else {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        cache.loading = Some(receiver.clone());
        let backend = state.backend.clone();
        let shared = state.health.clone();
        // A request disappearing must not cancel the shared snapshot and make
        // every next caller start another actor request (Swift Task semantics).
        tokio::spawn(async move {
            let result =
                match tokio::time::timeout(Duration::from_secs(30), backend.health(route)).await {
                    Ok(Ok(result)) => result,
                    _ => (
                        503,
                        serde_json::json!({"error":"health snapshot unavailable"}),
                    ),
                };
            let mut cache = shared.lock().await;
            cache.ready = Some((Instant::now(), result.0, result.1.clone()));
            cache.loading = None;
            sender.send_replace(Some(result));
        });
        receiver
    };
    drop(cache);
    loop {
        if let Some((code, value)) = receiver.borrow_and_update().as_ref() {
            return api::bounded_json(
                StatusCode::from_u16(*code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                value,
                4 << 20,
            );
        }
        if receiver.changed().await.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
}

struct BackendFetcher(Arc<dyn RpcBackend>);
impl super::public_fetch::ArchiveFetcher for BackendFetcher {
    fn fetch(&self, cid: String) -> api::VolumeFuture {
        self.0.remote_volume(cid)
    }
}
async fn public_volume(state: &PublicState, cid: &str, query: Option<&str>) -> Response {
    use super::public_fetch::{FetchAnswer, MAXIMUM_ARCHIVE_BYTES};
    if cid.len() > 8192
        || lattice_cashew::canonical_cid(cid).map_or(true, |canonical| canonical != cid)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(_permit) = state.slots.try_acquire() else {
        return busy();
    };
    if let Some(query) = query {
        if form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "chainPath")
            .is_some_and(|(_, path)| path.split('/').any(str::is_empty))
        {
            return StatusCode::NOT_FOUND.into_response();
        }
        let scope = tokio::time::timeout(
            Duration::from_secs(30),
            state
                .backend
                .request(format!("/status?{query}"), serde_json::json!({})),
        )
        .await;
        match scope {
            Ok(Ok((200, _))) => {}
            Ok(Ok((code, value))) => {
                return api::bounded_json(
                    StatusCode::from_u16(code).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                    &value,
                    4 << 20,
                );
            }
            _ => return busy(),
        }
    }
    let answer =
        match tokio::time::timeout(Duration::from_secs(30), state.backend.volume(cid.into())).await
        {
            Ok(Ok(Some(bytes))) if bytes.len() <= MAXIMUM_ARCHIVE_BYTES => {
                FetchAnswer::Archive(Arc::new(bytes))
            }
            Ok(Ok(None)) if state.fetches.enabled() => state.fetches.archive(cid).await,
            Ok(Ok(None)) => FetchAnswer::NotServed,
            _ => FetchAnswer::Busy,
        };
    match answer {
        FetchAnswer::Archive(bytes) => {
            // Bytes owns the Arc, so concurrent responses share the same archive.
            let mut response = Response::new(Body::from(axum::body::Bytes::from_owner(
                SharedArchive(bytes),
            )));
            response.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("application/vnd.lattice.volume"),
            );
            response.headers_mut().insert(
                "cache-control",
                HeaderValue::from_static(super::read_cache::IMMUTABLE),
            );
            response
        }
        FetchAnswer::NotServed => StatusCode::NOT_FOUND.into_response(),
        FetchAnswer::Busy => busy(),
    }
}
fn busy() -> Response {
    let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
    response
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("1"));
    response
}

struct SharedArchive(Arc<Vec<u8>>);
impl AsRef<[u8]> for SharedArchive {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Gated {
        calls: AtomicUsize,
        gate: Arc<Semaphore>,
    }
    impl RpcBackend for Gated {
        fn health(&self, route: String) -> api::RpcFuture {
            self.request(route, serde_json::json!({}))
        }
        fn request(&self, _: String, _: Value) -> api::RpcFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.clone();
            Box::pin(async move {
                gate.acquire().await.unwrap().forget();
                Ok((200, serde_json::json!({"height":"3"})))
            })
        }
    }
    #[tokio::test]
    async fn cancelled_health_waiter_does_not_cancel_shared_load() {
        let backend = Arc::new(Gated {
            calls: AtomicUsize::new(0),
            gate: Arc::new(Semaphore::new(0)),
        });
        let state = PublicState {
            submit: false,
            fetches: crate::runtime::public_fetch::PublicVolumeFetches::new(
                0,
                0,
                Arc::new(BackendFetcher(backend.clone())),
            ),
            backend: backend.clone(),
            limiter: Arc::new(std::sync::Mutex::new(
                RateLimiter::new(RateLimits::default(), 0.).unwrap(),
            )),
            start: Instant::now(),
            slots: Arc::new(Semaphore::new(64)),
            health: Arc::new(Mutex::new(HealthCache::default())),
        };
        let first_state = state.clone();
        let first = tokio::spawn(async move { health(&first_state, "/status".into()).await });
        while backend.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        first.abort();
        let _ = first.await;
        backend.gate.add_permits(1);
        let response =
            tokio::time::timeout(Duration::from_secs(2), health(&state, "/status".into()))
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            health(&state, "/status".into()).await.status(),
            StatusCode::OK
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }
}
