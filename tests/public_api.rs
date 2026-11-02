use lattice_node::runtime::{
    api::{RpcBackend, RpcFuture},
    public_api,
    public_rate::RateLimits,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Backend(AtomicUsize);
impl RpcBackend for Backend {
    fn request(&self, route: String, _body: Value) -> RpcFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok((200, json!({"route":route})))
        })
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
async fn public_allowlist_cors_budgets_and_coalesced_health() {
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = public_api::router(
        backend.clone(),
        RateLimits {
            general: 0.,
            expensive: 0.,
            listener: 0.01,
            submit: 0.,
        },
    )
    .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let first = call(
        address,
        &request(
            "GET",
            "//api/chain//info/",
            "Origin: https://elsewhere.example\r\n",
            "",
        ),
    )
    .await;
    assert!(first.starts_with("HTTP/1.1 200"), "{first}");
    assert!(first.contains("access-control-allow-origin: *"));
    assert!(first.contains("/api/chain/info"));
    let blocked = call(
        address,
        &request(
            "GET",
            "/api/chain/info",
            "X-Forwarded-For: 10.0.0.2\r\n",
            "",
        ),
    )
    .await;
    assert!(blocked.starts_with("HTTP/1.1 429"));
    assert!(blocked.contains("access-control-allow-origin: *"));
    let before = backend.0.load(Ordering::SeqCst);
    let mut requests = vec![];
    for _ in 0..16 {
        requests.push(tokio::spawn(async move {
            call(address, &request("GET", "/health", "", "")).await
        }));
    }
    for task in requests {
        let r = task.await.unwrap();
        assert!(r.starts_with("HTTP/1.1 200"));
        assert!(r.contains("cache-control: public, max-age=3"));
    }
    assert_eq!(backend.0.load(Ordering::SeqCst), before + 1);
    let head = call(address, &request("HEAD", "/health", "", "")).await;
    assert!(head.starts_with("HTTP/1.1 200"));
    assert_eq!(head.split_once("\r\n\r\n").unwrap().1, "");
    let preflight = call(
        address,
        &request(
            "OPTIONS",
            "/transactions",
            "Access-Control-Request-Method: POST\r\n",
            "",
        ),
    )
    .await;
    assert!(preflight.starts_with("HTTP/1.1 403"));
    server.abort();
    let _ = server.await;
    // Unlimited rates isolate route denial from arrival rejection.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = public_api::router(
        backend.clone(),
        RateLimits {
            general: 0.,
            expensive: 0.,
            listener: 0.,
            submit: 0.,
        },
    )
    .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let before = backend.0.load(Ordering::SeqCst);
    for (method, path) in [
        ("POST", "/transactions"),
        ("POST", "/pins"),
        ("GET", "/pins"),
        ("GET", "/status"),
        ("POST", "/sweep"),
        ("GET", "/fetch/x"),
        ("GET", "/hosted-chains"),
        ("POST", "/mining/work"),
        ("POST", "/child-blocks"),
        ("DELETE", "/pins/x"),
    ] {
        let response = call(
            address,
            &request(method, path, "Authorization: Bearer arbitrary\r\n", ""),
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 404"),
            "{method} {path}: {response}"
        );
    }
    assert_eq!(before, backend.0.load(Ordering::SeqCst));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn opt_in_submit_has_separate_budget_and_keeps_operator_routes_closed() {
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = public_api::router_with_submit(
        backend.clone(),
        RateLimits {
            general: 0.,
            expensive: 0.,
            listener: 0.001,
            submit: 0.001,
        },
        true,
    )
    .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let preflight = call(
        address,
        &request(
            "OPTIONS",
            "/transactions",
            "Access-Control-Request-Method: POST\r\n",
            "",
        ),
    )
    .await;
    assert!(preflight.starts_with("HTTP/1.1 204"));
    assert!(preflight.contains("GET, HEAD, POST, OPTIONS"));
    let read = call(address, &request("GET", "/api/chain/info", "", "")).await;
    assert!(read.starts_with("HTTP/1.1 200"));
    let submit = call(
        address,
        &request(
            "POST",
            "/transactions",
            "Origin: https://example.test\r\n",
            "{}",
        ),
    )
    .await;
    assert!(submit.starts_with("HTTP/1.1 200"), "{submit}");
    assert!(submit.contains("/transactions"));
    let second = call(address, &request("POST", "/transactions", "", "{}")).await;
    assert!(second.starts_with("HTTP/1.1 429"));
    assert_eq!(backend.0.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
}

struct Volumes {
    local: AtomicUsize,
    remote: AtomicUsize,
}
impl RpcBackend for Volumes {
    fn request(&self, _route: String, _body: Value) -> RpcFuture {
        Box::pin(async { Ok((404, json!({"error":"unknown chain"}))) })
    }
    fn volume(&self, _cid: String) -> lattice_node::runtime::api::VolumeFuture {
        self.local.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
    fn remote_volume(&self, _cid: String) -> lattice_node::runtime::api::VolumeFuture {
        self.remote.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(anyhow::anyhow!("busy")) })
    }
}
#[tokio::test]
async fn volumes_validate_canonical_cid_and_scope_before_content_and_disabled_is_absent() {
    let backend = Arc::new(Volumes {
        local: AtomicUsize::new(0),
        remote: AtomicUsize::new(0),
    });
    let cid = "bafyreif7ge4xll3xcp2yvp44e4pbqhjdtex6fzufmwdhxe2gqcols4e47i";
    for capacity in [0, 1] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = public_api::router_with_fetches(
            backend.clone(),
            RateLimits {
                general: 0.,
                expensive: 0.,
                listener: 0.,
                submit: 0.,
            },
            false,
            capacity,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        });
        let before = backend.local.load(Ordering::SeqCst);
        for (path, code) in [
            (format!("/volumes/B{}", cid[1..].to_uppercase()), 400),
            (format!("/volumes/{cid}?chainPath=Nexus/Unknown"), 404),
            (format!("/volumes/{cid}?chainPath=Nexus//Alpha"), 404),
        ] {
            let result = call(address, &request("GET", &path, "", "")).await;
            assert!(result.starts_with(&format!("HTTP/1.1 {code}")), "{result}");
        }
        assert_eq!(backend.local.load(Ordering::SeqCst), before);
        let result = call(address, &request("GET", &format!("/volumes/{cid}"), "", "")).await;
        assert!(
            result.starts_with(if capacity == 0 {
                "HTTP/1.1 404"
            } else {
                "HTTP/1.1 503"
            }),
            "{result}"
        );
        if capacity > 0 {
            assert!(result.contains("retry-after: 1"));
        }
        server.abort();
        let _ = server.await;
    }
    assert_eq!(backend.remote.load(Ordering::SeqCst), 1);
}

struct CacheReads;
impl RpcBackend for CacheReads {
    fn request(&self, route: String, _: Value) -> RpcFuture {
        Box::pin(async move {
            if route.contains("missing") {
                Ok((404, json!({"error":"notFound"})))
            } else if route.contains("unavailable") {
                Err(anyhow::anyhow!("unavailable"))
            } else {
                // Private operator admission does not imply public admission.
                Ok((200, json!({"acceptsSubmit":true})))
            }
        })
    }
}

#[tokio::test]
async fn cache_headers_follow_content_identity_and_submit_capability_follows_listener() {
    let immutable = "cache-control: public, max-age=31536000, immutable\r\n";
    let changing = "cache-control: public, max-age=3\r\n";
    for submit in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = public_api::router_with_submit(
            Arc::new(CacheReads),
            RateLimits {
                general: 0.,
                expensive: 0.,
                listener: 0.,
                submit: 0.,
            },
            submit,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        });
        let cid = "bafyreif7ge4xll3xcp2yvp44e4pbqhjdtex6fzufmwdhxe2gqcols4e47i";
        // Inclusion in a canonical chain can change while transaction bytes do
        // not. Height selectors can change on reorg; CID selectors cannot.
        for (path, header) in [
            (format!("/transactions/{cid}"), immutable),
            (format!("/accounts/{cid}?block={cid}"), immutable),
            (format!("/api/transaction/{cid}"), changing),
            (format!("/api/state/account/{cid}"), changing),
            (format!("/api/block/{cid}"), immutable),
            (
                format!("/api/block/{cid}/transactions?offset=0&limit=3"),
                immutable,
            ),
            (format!("/api/block/{cid}/children"), immutable),
            ("/api/block/latest".into(), changing),
            ("/api/block/0001".into(), changing),
            ("/api/block/18446744073709551615".into(), changing),
            ("/api/blocks?before=3".into(), changing),
            ("/api/chain/genesis?chainPath=Nexus/Alpha".into(), immutable),
            ("/api/chain/spec".into(), changing),
            ("/api/chain/children".into(), changing),
            (
                "/api/chain/endpoints?chainPath=Nexus/Alpha".into(),
                changing,
            ),
            ("/api/mempool".into(), changing),
            ("/api/fees".into(), changing),
            ("/api/peers".into(), changing),
            ("/api/deposits".into(), changing),
            ("/api/deposit-state?key=a".into(), changing),
            ("/api/receipt-state?chainPath=Nexus/Alpha".into(), changing),
            ("/health".into(), changing),
        ] {
            let response = call(address, &request("GET", &path, "", "")).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert!(response.contains(header), "{path}: {response}");
        }
        for path in ["/api/chain/info", "/api/chain/info?chainPath=Nexus/Alpha"] {
            let response = call(address, &request("GET", path, "", "")).await;
            assert!(response.contains(changing));
            let value: Value =
                serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(value["acceptsSubmit"], submit);
        }
        for (method, path, status) in [
            ("GET", "/transactions/missing", 404),
            ("GET", "/api/block/unavailable", 503),
            ("GET", "/health?chainPath=missing", 404),
            ("GET", "/health?chainPath=unavailable", 503),
            ("POST", "/transactions", if submit { 200 } else { 404 }),
            ("OPTIONS", "/api/chain/info", 204),
        ] {
            let response = call(address, &request(method, path, "", "")).await;
            assert!(
                response.starts_with(&format!("HTTP/1.1 {status}")),
                "{response}"
            );
            assert!(!response.contains("cache-control:"), "{response}");
        }
        server.abort();
        let _ = server.await;
    }
}
