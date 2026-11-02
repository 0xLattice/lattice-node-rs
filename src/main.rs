//! Root node process entry point. Configuration paths are relative to the JSON file.
use anyhow::{Context, bail};
mod identity;
use ivy_core::PeerEndpoint;
use lattice_cashew::SerializedVolume;
use lattice_node::runtime::{
    api::{self, Cookie, RpcBackend, RpcFuture},
    host::{DiscoveryConfig, HostConfig, HostHandle, start_with_hosted_configuration},
};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    directory: PathBuf,
    #[serde(default)]
    state: lattice_node::storage::StateRetention,
    #[serde(default)]
    block_bodies: lattice_node::storage::BodyRetention,
    genesis_cid: String,
    #[serde(default)]
    bootstrap_files: Vec<PathBuf>,
    identity_key: PathBuf,
    #[serde(default = "relay_fee_default")]
    min_relay_fee: u64,
    #[serde(default = "overlay_default")]
    listen: SocketAddr,
    #[serde(default = "rpc_default")]
    rpc: SocketAddr,
    #[serde(default)]
    cookie_file: Option<PathBuf>,
    #[serde(default)]
    public_read: Option<SocketAddr>,
    #[serde(default)]
    public_read_url: Option<String>,
    #[serde(default)]
    public_submit: bool,
    #[serde(default = "public_fetches_default")]
    public_read_fetches: usize,
    #[serde(default)]
    public_read_rates: lattice_node::runtime::public_rate::RateLimits,
    #[serde(default)]
    allowed_origins: BTreeSet<String>,
    #[serde(default)]
    seeds: Vec<Seed>,
    #[serde(default)]
    seed_peers: Vec<Seed>,
    #[serde(default)]
    fixed_seed_peers: Vec<Seed>,
    #[serde(default)]
    host_chain: Vec<String>,
    #[serde(default)]
    premine_to: Vec<String>,
    #[serde(default)]
    genesis_transactions: Vec<String>,
    #[serde(default)]
    chain_peer: Vec<String>,
    #[serde(default)]
    policy_module: Vec<PathBuf>,
    #[serde(default = "peer_search_default")]
    peer_search_interval: f64,
    #[serde(default = "peer_target_default")]
    peer_target: usize,
    #[serde(default = "peers_before_seeds_default")]
    peers_before_seeds: usize,
    #[serde(default = "announce_lifetime_default")]
    announce_lifetime: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Seed {
    public_key: String,
    host: String,
    port: u16,
}
fn public_fetches_default() -> usize {
    4
}
fn peer_search_default() -> f64 {
    DiscoveryConfig::default().peer_search_interval
}
fn peer_target_default() -> usize {
    DiscoveryConfig::default().peer_target
}
fn peers_before_seeds_default() -> usize {
    DiscoveryConfig::default().peers_before_seeds
}
fn announce_lifetime_default() -> u64 {
    DiscoveryConfig::default().announce_lifetime
}
fn relay_fee_default() -> u64 {
    1
}
fn overlay_default() -> SocketAddr {
    "127.0.0.1:7447".parse().unwrap()
}
fn rpc_default() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}
fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    }
}
fn read_bounded(path: &Path, limit: usize) -> anyhow::Result<Vec<u8>> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        bail!("{} exceeds size limit", path.display());
    }
    Ok(bytes)
}
struct Backend(HostHandle);
impl RpcBackend for Backend {
    fn health(&self, route: String) -> RpcFuture {
        let value = self.0.health(&route);
        Box::pin(async move { Ok(value) })
    }
    fn remote_volume(&self, cid: String) -> lattice_node::runtime::api::VolumeFuture {
        let handle = self.0.clone();
        Box::pin(async move {
            let (status, value) = handle
                .request(&format!("/fetch/{cid}"), serde_json::json!({}))
                .await?;
            match status {
                200 => Ok(lattice_node::runtime::public_fetch::decode_rpc_archive(
                    &cid, &value,
                )),
                404 => Ok(None),
                _ => Err(anyhow::anyhow!("volume retrieval busy")),
            }
        })
    }

    fn volume(&self, cid: String) -> lattice_node::runtime::api::VolumeFuture {
        let handle = self.0.clone();
        Box::pin(async move { handle.volume(&cid).await })
    }

    fn request(&self, route: String, body: Value) -> RpcFuture {
        let handle = self.0.clone();
        Box::pin(async move { handle.request(&route, body).await })
    }
}
async fn stop_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {r=tokio::signal::ctrl_c()=>r?,_=term.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] || args == ["-h"] {
        println!(
            "Usage: lattice-node --config PATH\n\nRoot platform node with cookie-authenticated loopback RPC. See docs/RUNTIME.md."
        );
        return Ok(());
    }
    if args.len() != 2 || args[0] != "--config" {
        bail!("usage: lattice-node --config PATH");
    }
    let path = std::fs::canonicalize(&args[1]).context("config file not found")?;
    let base = path.parent().context("config has no parent")?;
    let config: Config =
        serde_json::from_slice(&read_bounded(&path, 1 << 20)?).context("invalid node config")?;
    if !config.rpc.ip().is_loopback() {
        bail!("operator RPC must bind a loopback address");
    }
    lattice_cashew::canonical_cid(&config.genesis_cid).context("invalid genesis CID")?;
    let directory = resolve(base, &config.directory);
    let key = identity::load_or_create(&resolve(base, &config.identity_key))?;
    let mut volumes: Vec<SerializedVolume> = Vec::new();
    for file in config.bootstrap_files {
        let mut batch: Vec<SerializedVolume> =
            serde_json::from_slice(&read_bounded(&resolve(base, &file), 64 << 20)?)
                .context("invalid bootstrap volumes")?;
        volumes.append(&mut batch);
    }
    let endpoint = |s: Seed| -> anyhow::Result<PeerEndpoint> {
        if s.host.is_empty()
            || s.port == 0
            || s.public_key.len() != 64
            || hex::decode(&s.public_key).map_or(true, |k| k.len() != 32)
        {
            bail!("invalid seed endpoint");
        }
        Ok(PeerEndpoint {
            public_key: s.public_key,
            host: s.host,
            port: s.port,
        })
    };
    let seeds = config
        .seeds
        .into_iter()
        .map(endpoint)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let seed_peers = config
        .seed_peers
        .into_iter()
        .map(endpoint)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let fixed_seed_peers = config
        .fixed_seed_peers
        .into_iter()
        .map(endpoint)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let assignment_path = |entry: String| match entry.split_once('=') {
        Some((chain, file)) => format!("{}={}", chain, resolve(base, Path::new(file)).display()),
        None => entry,
    };
    let hosting = serde_json::json!({
        "hostChain": config.host_chain.into_iter().map(assignment_path).collect::<Vec<_>>(),
        "premineTo": config.premine_to,
        "genesisTransactions": config.genesis_transactions.into_iter().map(assignment_path).collect::<Vec<_>>(),
        "chainPeer": config.chain_peer,
        "policyModule": config.policy_module.iter().map(|p|resolve(base,p)).collect::<Vec<_>>()
    });
    let mut host = start_with_hosted_configuration(
        HostConfig {
            directory: directory.clone(),
            genesis_cid: config.genesis_cid,
            bootstrap_volumes: volumes,
            signing_key: key,
            listen: config.listen,
            min_relay_fee: config.min_relay_fee,
            seeds,
        },
        lattice_node::storage::RetentionPolicy {
            state: config.state,
            bodies: config.block_bodies,
        },
        DiscoveryConfig {
            peer_search_interval: config.peer_search_interval,
            peer_target: config.peer_target,
            peers_before_seeds: config.peers_before_seeds,
            announce_lifetime: config.announce_lifetime,
            seed_peers,
            fixed_seed_peers,
        },
        lattice_node::runtime::host::ReadEndpointConfig {
            public_read_url: config.public_read_url,
            public_submit: config.public_submit,
        },
        hosting,
    )
    .await?;
    let setup = async {
        if config.public_submit && config.public_read.is_none() {
            bail!("publicSubmit requires publicRead");
        }
        config
            .public_read_rates
            .validate()
            .map_err(anyhow::Error::msg)?;
        let public_listener = match config.public_read {
            Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
            None => None,
        };
        let listener = tokio::net::TcpListener::bind(config.rpc).await?;
        let address = listener.local_addr()?;
        let cookie_path = config
            .cookie_file
            .map(|p| resolve(base, &p))
            .unwrap_or_else(|| directory.join(".cookie"));
        let cookie = Arc::new(Cookie::create(&cookie_path, config.allowed_origins)?);
        Ok::<_, anyhow::Error>((listener, address, cookie, public_listener))
    }
    .await;
    let (listener, address, cookie, public_listener) = match setup {
        Ok(v) => v,
        Err(error) => {
            let _ = host.handle.shutdown().await;
            let _ = host.task.await;
            return Err(error);
        }
    };
    let app = api::router(Arc::new(Backend(host.handle.clone())), cookie.clone());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let public_address = public_listener
        .as_ref()
        .map(|listener| listener.local_addr())
        .transpose()?;
    let public_app = lattice_node::runtime::public_api::router_with_fetches(
        Arc::new(Backend(host.handle.clone())),
        config.public_read_rates,
        config.public_submit,
        config.public_read_fetches,
    )?;
    let (public_stop, public_stopped) = tokio::sync::oneshot::channel::<()>();
    let mut public_server = tokio::spawn(async move {
        if let Some(listener) = public_listener {
            axum::serve(
                listener,
                public_app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = public_stopped.await;
            })
            .await
        } else {
            let _ = public_stopped.await;
            Ok(())
        }
    });
    println!(
        "{}",
        serde_json::json!({"event":"ready","rpc":address.to_string(),"overlay":host.overlay_addr.to_string(),"publicKey":host.public_key,"publicRead":public_address.map(|a|a.to_string())})
    );
    enum End {
        Signal(anyhow::Result<()>),
        Host(Result<anyhow::Result<()>, tokio::task::JoinError>),
        Http(Result<std::io::Result<()>, tokio::task::JoinError>),
        PublicHttp(Result<std::io::Result<()>, tokio::task::JoinError>),
    }
    let end = tokio::select! {r=stop_signal()=>End::Signal(r),r=&mut host.task=>End::Host(r),r=&mut server=>End::Http(r),r=&mut public_server=>End::PublicHttp(r)};
    let _ = stop.send(());
    let _ = public_stop.send(());
    let _ = host.handle.shutdown().await;
    if !matches!(&end, End::Host(_)) {
        host.task.await??;
    }
    if !matches!(&end, End::Http(_)) {
        server.await??;
    }
    if !matches!(&end, End::PublicHttp(_)) {
        public_server.await??;
    }
    drop(cookie);
    match end {
        End::Signal(r) => r,
        End::Host(r) => {
            r??;
            Ok(())
        }
        End::Http(r) | End::PublicHttp(r) => {
            r??;
            Ok(())
        }
    }
}
