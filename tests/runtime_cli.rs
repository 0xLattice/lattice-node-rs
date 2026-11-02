#![cfg(unix)]
use lattice_cashew::{Header, Volume};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(path: &std::path::Path) -> (Process, Value) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_lattice-node"))
        .args(["--config", path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(out).read_line(&mut line);
        let _ = tx.send((result, line));
    });
    let process = Process(child);
    let (result, line) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("node startup timed out");
    assert!(result.unwrap() > 0, "node exited before ready");
    (process, serde_json::from_str(&line).unwrap())
}
fn rpc(address: &str, path: &str, body: Option<Value>, cookie: &str) -> (u16, Value) {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let body = body.map(|v| v.to_string());
    let method = if body.is_some() { "POST" } else { "GET" };
    let body = body.unwrap_or_default();
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream,"{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Basic {}\r\nContent-Length: {}\r\n\r\n{body}",STANDARD.encode(cookie),body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        serde_json::from_str(body).unwrap(),
    )
}
fn stop(process: &mut Process) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline, "node failed to stop");
        std::thread::sleep(Duration::from_millis(20));
    }
}
#[test]
fn actual_process_rpc_mine_shutdown_and_durable_restart() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Value = serde_json::from_str(include_str!(
        "../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
    ))
    .unwrap();
    let entries: BTreeMap<String, Vec<u8>> =
        serde_json::from_value(rows[0]["entries"].clone()).unwrap();
    let cid = rows[0]["cid"].as_str().unwrap();
    let genesis = lattice_validation::validate_root_genesis(
        &Volume(Header::from_cid(cid).unwrap()),
        &entries,
        i64::MAX,
    )
    .unwrap();
    let volumes = lattice_cashew::volume::serialize_recursive(genesis.header(), &()).unwrap();
    std::fs::write(
        dir.path().join("bootstrap.json"),
        serde_json::to_vec(&volumes).unwrap(),
    )
    .unwrap();
    let config = dir.path().join("node.json");
    let mut value = json!({"directory":"data","identityKey":"identity.key","genesisCid":cid,"bootstrapFiles":["bootstrap.json"],"listen":"127.0.0.1:0","rpc":"127.0.0.1:0","publicRead":"127.0.0.1:0"});
    std::fs::write(&config, value.to_string()).unwrap();
    let (mut process, ready) = spawn(&config);
    let cookie_path = dir.path().join("data/.cookie");
    let cookie = std::fs::read_to_string(&cookie_path).unwrap();
    let address = ready["rpc"].as_str().unwrap();
    let public_address = ready["publicRead"].as_str().unwrap();
    let (code, health) = rpc(public_address, "/health", None, "");
    assert_eq!(code, 200);
    assert_eq!(health["height"], "0");
    let (code, _) = rpc(public_address, "/api/chain/info", None, "");
    assert_eq!(code, 200);
    let (code, _) = rpc(address, "/status", None, "");
    assert_eq!(code, 401);

    let (code, status) = rpc(address, "/status", None, &cookie);
    assert_eq!(code, 200);
    assert_eq!(status["height"], "0");
    let (code, template) = rpc(
        address,
        "/mining/templates",
        Some(json!({"recipients":[]})),
        &cookie,
    );
    assert_eq!(code, 200, "{template}");
    let (code, work) = rpc(
        address,
        "/mining/work",
        Some(json!({"workID":template["workID"],"nonce":0})),
        &cookie,
    );
    assert_eq!(code, 200, "{work}");
    let (_, status) = rpc(address, "/status", None, &cookie);
    assert_eq!(status["height"], "1");
    let tip = status["tipCID"].clone();
    stop(&mut process);
    assert!(!cookie_path.exists());
    value["bootstrapFiles"] = json!([]);
    std::fs::write(&config, value.to_string()).unwrap();
    let (mut process, restarted) = spawn(&config);
    assert_eq!(ready["publicKey"], restarted["publicKey"]);
    let fresh = std::fs::read_to_string(&cookie_path).unwrap();
    assert_ne!(fresh, cookie);
    let (code, status) = rpc(restarted["rpc"].as_str().unwrap(), "/status", None, &fresh);
    assert_eq!(code, 200);
    assert_eq!(status["tipCID"], tip);
    assert_eq!(status["height"], "1");
    stop(&mut process);
    assert!(!cookie_path.exists());
}
