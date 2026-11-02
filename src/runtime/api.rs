//! Cookie-authenticated loopback RPC. All non-health routes require the cookie;
//! browser origins are rejected unless explicitly listed by the operator.
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use rand_core::{OsRng, RngCore};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
pub type RpcFuture = Pin<Box<dyn Future<Output = anyhow::Result<(u16, Value)>> + Send>>;
pub type VolumeFuture = Pin<Box<dyn Future<Output = anyhow::Result<Option<Vec<u8>>>> + Send>>;
pub trait RpcBackend: Send + Sync + 'static {
    fn request(&self, route: String, body: Value) -> RpcFuture;
    fn remote_volume(&self, _cid: String) -> VolumeFuture {
        Box::pin(async { Ok(None) })
    }
    fn volume(&self, _cid: String) -> VolumeFuture {
        Box::pin(async { Ok(None) })
    }
}
/// Secret intentionally has no Debug/Serialize implementation.
pub struct Cookie {
    token: String,
    path: PathBuf,
    allowed_origins: BTreeSet<String>,
}
impl Cookie {
    pub fn create(path: &Path, allowed_origins: BTreeSet<String>) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let mut bytes = [0; 32];
        OsRng.fill_bytes(&mut bytes);
        let token = hex::encode(bytes);
        let temp = path.with_extension(format!("tmp-{}", &token[..16]));
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| {
            let mut file = options.open(&temp)?;
            file.write_all(format!("__cookie__:{token}").as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp, path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        result?;
        Ok(Self {
            token,
            path: path.to_owned(),
            allowed_origins,
        })
    }
    fn accepts(&self, value: Option<&str>) -> bool {
        let Some((kind, value)) = value.and_then(|v| v.trim_start_matches(' ').split_once(' '))
        else {
            return false;
        };
        let value = value.trim();
        let presented = if kind.eq_ignore_ascii_case("bearer") {
            format!("__cookie__:{value}").into_bytes()
        } else if kind.eq_ignore_ascii_case("basic") {
            let Ok(v) = STANDARD.decode(value) else {
                return false;
            };
            v
        } else {
            return false;
        };
        let expected = format!("__cookie__:{}", self.token);
        if presented.len() != expected.len() {
            return false;
        }
        presented
            .iter()
            .zip(expected.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            == 0
    }
}
impl Drop for Cookie {
    fn drop(&mut self) {
        if std::fs::read(&self.path).ok().as_deref()
            == Some(format!("__cookie__:{}", self.token).as_bytes())
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
#[derive(Clone)]
struct ApiState {
    backend: Arc<dyn RpcBackend>,
    cookie: Arc<Cookie>,
    slots: Arc<Semaphore>,
}
pub fn router(backend: Arc<dyn RpcBackend>, cookie: Arc<Cookie>) -> Router {
    Router::new().fallback(any(dispatch)).with_state(ApiState {
        backend,
        cookie,
        slots: Arc::new(Semaphore::new(64)),
    })
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error":message}))).into_response()
}
async fn dispatch(State(state): State<ApiState>, request: Request) -> Response {
    let origin = request.headers().get("origin").cloned();
    if origin.as_ref().is_some_and(|o| {
        o.to_str()
            .ok()
            .is_none_or(|o| !state.cookie.allowed_origins.contains(o))
    }) {
        return error(StatusCode::FORBIDDEN, "origin not allowed on operator RPC");
    }
    let mut response = if origin.is_some() && request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        response.headers_mut().insert(
            "access-control-allow-methods",
            HeaderValue::from_static("GET, HEAD, POST, DELETE"),
        );
        response.headers_mut().insert(
            "access-control-allow-headers",
            HeaderValue::from_static("Authorization, Content-Type"),
        );
        response
            .headers_mut()
            .insert("access-control-max-age", HeaderValue::from_static("600"));
        response
    } else {
        authorized(&state, request).await
    };
    if let Some(origin) = origin {
        response
            .headers_mut()
            .insert("access-control-allow-origin", origin);
        response
            .headers_mut()
            .insert("vary", HeaderValue::from_static("Origin"));
    }
    response
}
async fn authorized(state: &ApiState, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if path == "/health" && (method == Method::GET || method == Method::HEAD) {
        let mut response = axum::Json(json!({"status":"ok"})).into_response();
        if method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        return response;
    }
    if request.headers().get_all("authorization").iter().count() != 1
        || !state.cookie.accepts(
            request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
        )
    {
        return error(
            StatusCode::UNAUTHORIZED,
            "operator RPC requires the node's cookie",
        );
    }
    serve(&state.backend, &state.slots, request).await
}
/// Shared bounded transport, called only after the listener's authorization
/// or explicit public-route allowlist has succeeded.
pub(super) async fn serve(
    backend: &Arc<dyn RpcBackend>,
    slots: &Semaphore,
    request: Request,
) -> Response {
    serve_with_submit(backend, slots, request, None).await
}

pub(super) async fn serve_with_submit(
    backend: &Arc<dyn RpcBackend>,
    slots: &Semaphore,
    request: Request,
    public_submit: Option<bool>,
) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    let expected = match path.as_str() {
        "/hosted-chains" if method == Method::POST => Method::POST,
        "/status" | "/hosted-chains" => Method::GET,
        path if path.starts_with("/api/")
            || path.starts_with("/accounts/")
            || path.starts_with("/transactions/") =>
        {
            Method::GET
        }
        path if path.starts_with("/fetch/") => Method::GET,
        path if path.starts_with("/volumes/") => Method::GET,
        "/sweep" | "/child-blocks" | "/child-blocks/build" => Method::POST,
        "/child-blocks/commitment" => Method::GET,
        "/pins" if method == Method::POST => Method::POST,
        "/pins" => Method::GET,
        path if path.starts_with("/pins/") => Method::DELETE,
        path if path.starts_with("/serving-credit/") => Method::GET,
        "/serving-credit" => Method::POST,
        "/mining/templates" | "/mining/work" | "/transactions" => Method::POST,
        _ => return error(StatusCode::NOT_FOUND, "unknown route"),
    };
    if method != expected && !(expected == Method::GET && method == Method::HEAD) {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if request.uri().query().is_some()
        && !(path == "/status"
            || path == "/child-blocks/commitment"
            || path.starts_with("/api/")
            || path.starts_with("/accounts/")
            || path.starts_with("/transactions/"))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "root host does not accept route queries",
        );
    }
    let Ok(_permit) = slots.try_acquire() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "RPC busy");
    };
    if let Some(cid) = path.strip_prefix("/volumes/") {
        if lattice_cashew::canonical_cid(cid).is_err() {
            return error(StatusCode::BAD_REQUEST, "invalidCID");
        }
        return match tokio::time::timeout(Duration::from_secs(30), backend.volume(cid.into())).await
        {
            Ok(Ok(Some(bytes))) if bytes.len() <= 64 * 1024 * 1024 => {
                let mut response = Response::new(if method == Method::HEAD {
                    Body::empty()
                } else {
                    Body::from(bytes)
                });
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
            Ok(Ok(None)) => error(StatusCode::NOT_FOUND, "notServed"),
            _ => error(StatusCode::SERVICE_UNAVAILABLE, "volume unavailable"),
        };
    }
    let backend_route = if (path == "/pins" || path == "/hosted-chains" || path == "/sweep")
        && method == Method::POST
    {
        format!("POST {path}")
    } else if method == Method::DELETE {
        format!("DELETE {path}")
    } else {
        request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or(&path)
            .to_owned()
    };
    let maximum_body = if path == "/pins" { 2 << 20 } else { 1 << 20 };
    let bytes = match tokio::time::timeout(
        Duration::from_secs(15),
        to_bytes(request.into_body(), maximum_body),
    )
    .await
    {
        Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "request body timed out"),
        Ok(Err(_)) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds route limit",
            );
        }
        Ok(Ok(b)) => b,
    };
    let body = if bytes.is_empty() {
        json!({})
    } else {
        match crate::core::wire::decode_json_value(&bytes) {
            Ok(value) => value,
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid JSON"),
        }
    };
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        backend.request(backend_route, body),
    )
    .await;
    let mut response = match result {
        Ok(Ok((status, mut value))) => match StatusCode::from_u16(status) {
            Ok(status) => {
                if status.is_success()
                    && path == "/api/chain/info"
                    && let Some(accepts_submit) = public_submit
                    && let Some(info) = value.as_object_mut()
                {
                    // The public listener's opt-in controls admission; the
                    // operator backend's capability is a different surface.
                    info.insert("acceptsSubmit".into(), json!(accepts_submit));
                }
                let cap = if path.starts_with("/fetch/")
                    || matches!(
                        path.as_str(),
                        "/api/deposits" | "/api/deposit-state" | "/api/receipt-state"
                    ) {
                    96 << 20
                } else {
                    4 << 20
                };
                bounded_json(status, &value, cap)
            }
            Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "invalid host response"),
        },
        Ok(Err(_)) => error(StatusCode::SERVICE_UNAVAILABLE, "node request failed"),
        Err(_) => error(StatusCode::GATEWAY_TIMEOUT, "node request timed out"),
    };
    super::read_cache::apply(&mut response, &method, &path);
    if method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    response
}

pub(super) fn bounded_json(status: StatusCode, value: &Value, maximum: usize) -> Response {
    struct Limited {
        bytes: Vec<u8>,
        maximum: usize,
    }
    impl std::io::Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("response exceeds route limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Limited {
        bytes: Vec::new(),
        maximum,
    };
    if serde_json::to_writer(&mut output, value).is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "response exceeds route limit",
        );
    }
    let mut response = (status, output.bytes).into_response();
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}
