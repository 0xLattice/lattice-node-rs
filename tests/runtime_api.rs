use lattice_node::runtime::api::{self, Cookie, RpcBackend, RpcFuture};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Backend(AtomicUsize);
impl RpcBackend for Backend {
    fn request(&self, route: String, body: Value) -> RpcFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok((200, json!({"route":route,"received":body}))) })
    }
}
async fn call(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    // A bounded endpoint may answer 413 and close while the request is still
    // being written. Read concurrently so that early response is retained.
    let (mut reader, mut writer) = stream.split();
    let mut bytes = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (write, read) = tokio::join!(
            writer.write_all(request.as_bytes()),
            reader.read_to_end(&mut bytes)
        );
        if let Err(error) = write {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ));
        }
        if let Err(error) = read {
            assert!(!bytes.is_empty() && error.kind() == std::io::ErrorKind::ConnectionReset);
        }
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap()
    .unwrap();
    String::from_utf8(bytes).unwrap()
}
fn request(method: &str, path: &str, headers: &str, body: &str) -> String {
    format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}
#[tokio::test]
async fn real_http_cookie_origins_methods_and_payload_bounds() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(".cookie");
    let cookie = Arc::new(
        Cookie::create(&path, BTreeSet::from(["https://trusted.example".into()])).unwrap(),
    );
    let token = std::fs::read_to_string(&path).unwrap();
    let auth = format!("Authorization: Basic {}\r\n", STANDARD.encode(&token));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = api::router(backend.clone(), cookie.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (method, path, headers, body, status) in [
        ("GET", "/health", "", "", 200),
        ("GET", "/status", "", "", 401),
        ("GET", "/status", auth.as_str(), "", 200),
        ("GET", "/hosted-chains", auth.as_str(), "", 200),
        ("GET", "/hosted-chains", "", "", 401),
        ("GET", "/status", "Authorization: Bearer wrong\r\n", "", 401),
        (
            "POST",
            "/mining/work",
            auth.as_str(),
            "{\"nonce\":\"4\"}",
            200,
        ),
        (
            "POST",
            "/mining/work",
            auth.as_str(),
            "{\"nonce\":1,\"nonce\":2}",
            200,
        ),
        ("POST", "/status", auth.as_str(), "{}", 405),
        ("GET", "/status?chainPath=Other", auth.as_str(), "", 200),
        ("GET", "/unknown", auth.as_str(), "", 404),
        (
            "GET",
            "/health",
            "Origin: https://untrusted.example\r\n",
            "",
            403,
        ),
        (
            "OPTIONS",
            "/mining/work",
            "Origin: https://trusted.example\r\n",
            "",
            204,
        ),
    ] {
        let result = call(addr, &request(method, path, headers, body)).await;
        assert!(
            result.starts_with(&format!("HTTP/1.1 {status}")),
            "{method} {path}: {result}"
        );
    }
    let bearer = format!(
        "Authorization: Bearer {}\r\n",
        token.strip_prefix("__cookie__:").unwrap()
    );
    assert!(
        call(addr, &request("GET", "/status", &bearer, ""))
            .await
            .starts_with("HTTP/1.1 200")
    );
    let denied = call(
        addr,
        &request("GET", "/status", "Origin: https://trusted.example\r\n", ""),
    )
    .await;
    assert!(denied.starts_with("HTTP/1.1 401"));
    assert!(denied.contains("access-control-allow-origin: https://trusted.example"));
    assert!(!denied.to_ascii_lowercase().contains("www-authenticate"));
    let large = " ".repeat((1 << 20) + 1);
    assert!(
        call(addr, &request("POST", "/transactions", &auth, &large))
            .await
            .starts_with("HTTP/1.1 413")
    );
    assert_eq!(backend.0.load(Ordering::SeqCst), 6);
    task.abort();
    let _ = task.await;
    drop(cookie);
    assert!(!path.exists());
}
#[test]
fn cookie_rotation_old_owner_cannot_remove_new_secret() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(".cookie");
    let old = Cookie::create(&path, BTreeSet::new()).unwrap();
    let old_text = std::fs::read(&path).unwrap();
    let new = Cookie::create(&path, BTreeSet::new()).unwrap();
    assert_ne!(old_text, std::fs::read(&path).unwrap());
    drop(old);
    assert!(path.exists());
    drop(new);
    assert!(!path.exists());
}

#[tokio::test]
async fn query_routes_binary_archives_and_distinct_pin_limits() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    struct Routes;
    impl RpcBackend for Routes {
        fn request(&self, route: String, _body: Value) -> RpcFuture {
            Box::pin(async move {
                if route == "/api/mempool" {
                    return Ok((200, json!({"large":"x".repeat((4<<20)+1)})));
                }
                Ok((200, json!({"route":route})))
            })
        }
        fn volume(&self, _cid: String) -> api::VolumeFuture {
            Box::pin(async { Ok(Some(b"archive".to_vec())) })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let cookie_path = temp.path().join(".cookie");
    let cookie = Arc::new(Cookie::create(&cookie_path, BTreeSet::new()).unwrap());
    let auth = format!(
        "Authorization: Basic {}\r\n",
        STANDARD.encode(std::fs::read(&cookie_path).unwrap())
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, api::router(Arc::new(Routes), cookie))
            .await
            .unwrap()
    });
    let result = call(
        addr,
        &request("GET", "/api/deposits?after=a%2Fb&limit=2", &auth, ""),
    )
    .await;
    assert!(result.starts_with("HTTP/1.1 200"));
    assert!(result.contains("/api/deposits?after=a%2Fb&limit=2"));
    let large = format!("{{\"padding\":\"{}\"}}", "x".repeat(3 << 19));
    assert!(
        call(addr, &request("POST", "/pins", &auth, &large))
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(
        call(addr, &request("POST", "/transactions", &auth, &large))
            .await
            .starts_with("HTTP/1.1 413")
    );
    let cid = lattice_cashew::cid_for_bytes(0x55, b"archive");
    let result = call(addr, &request("GET", &format!("/volumes/{cid}"), &auth, "")).await;
    assert!(result.starts_with("HTTP/1.1 200"));
    assert!(result.contains("application/vnd.lattice.volume"));
    assert!(result.ends_with("archive"));
    let head = call(
        addr,
        &request("HEAD", &format!("/volumes/{cid}"), &auth, ""),
    )
    .await;
    assert!(!head.ends_with("archive"));
    assert!(
        call(addr, &request("GET", "/api/mempool", &auth, ""))
            .await
            .starts_with("HTTP/1.1 503")
    );
    task.abort();
}
