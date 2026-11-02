//! Hosted rendezvous maintenance uses async Ivy requests; no actor-held dial wait.
use super::*;
use crate::core::peer_search::{PeerSearch, rendezvous};

/// Operator discovery policy, with the pinned Swift defaults.
#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub peer_search_interval: f64,
    pub peer_target: usize,
    pub peers_before_seeds: usize,
    pub announce_lifetime: u64,
    pub seed_peers: Vec<ivy_core::PeerEndpoint>,
    pub fixed_seed_peers: Vec<ivy_core::PeerEndpoint>,
}
impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            peer_search_interval: 600.0,
            peer_target: 8,
            peers_before_seeds: 2,
            announce_lifetime: 1200,
            seed_peers: vec![],
            fixed_seed_peers: vec![],
        }
    }
}
impl DiscoveryConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=86400).contains(&self.announce_lifetime),
            "announceLifetime must be 1 to 86400 seconds"
        );
        Ok(())
    }
    fn interval(&self) -> i64 {
        if self.peer_search_interval.is_finite() {
            (self
                .peer_search_interval
                .clamp(0.0, (i64::MAX / 1000) as f64)
                * 1000.0) as i64
        } else {
            0
        }
    }
    fn cadence(&self) -> i64 {
        let interval = self.interval();
        if interval > 0 {
            interval.clamp(1000, 600_000)
        } else {
            600_000
        }
    }
}

struct DiscoveryPlan {
    path: Vec<String>,
    stalled: bool,
    top_up: bool,
    stage: u8,
}
struct DiscoveryPass {
    plans: VecDeque<DiscoveryPlan>,
    attempted: BTreeSet<(String, String, u16)>,
}
pub(super) struct Discovery {
    search: PeerSearch,
    announced: BTreeMap<String, u64>,
    seeds: Vec<ivy_core::PeerEndpoint>,
    memory: Option<super::super::peer_memory::PeerMemory>,
    pass: Option<DiscoveryPass>,
    connected: bool,
    config: DiscoveryConfig,
    next_pass: i64,
    last_paths: BTreeSet<Vec<String>>,
    announcements: VecDeque<String>,
    next_announcement_batch: i64,
}
impl Discovery {
    pub(super) fn dialed(
        &mut self,
        endpoint: ivy_core::PeerEndpoint,
        paths: &[Vec<String>],
        result: Option<bool>,
        now: i64,
    ) {
        if let Some(memory) = &mut self.memory {
            match result {
                Some(true) => memory.reached(endpoint, paths, now),
                Some(false) => memory.failed(&endpoint.public_key),
                None => {}
            }
        }
    }
    pub(super) fn session_lost(&mut self, now: i64) {
        self.next_pass = self.next_pass.min(now.saturating_add(10_000));
    }

    pub(super) fn changed(&mut self) {
        self.next_pass = 0;
        self.next_announcement_batch = 0;
    }
    pub(super) fn new(
        directory: &std::path::Path,
        own_key: String,
        seeds: Vec<ivy_core::PeerEndpoint>,
        config: DiscoveryConfig,
        initial_paths: &[Vec<String>],
        child_seeds: bool,
    ) -> Self {
        let excluded = std::iter::once(own_key)
            .chain(seeds.iter().map(|e| e.public_key.clone()))
            .collect();
        let memory =
            (!seeds.is_empty() || !config.seed_peers.is_empty() || child_seeds).then(|| {
                super::super::peer_memory::PeerMemory::open(
                    directory,
                    initial_paths,
                    excluded,
                    now(),
                )
            });
        Self {
            search: PeerSearch::default(),
            announced: BTreeMap::new(),
            seeds,
            memory,
            pass: None,
            config,
            next_pass: 0,
            last_paths: BTreeSet::new(),
            announcements: VecDeque::new(),
            next_announcement_batch: 0,
            connected: false,
        }
    }
}
impl Host {
    async fn discovery_dials(
        &mut self,
        mut endpoints: Vec<ivy_core::PeerEndpoint>,
        seed_path: Option<&[String]>,
    ) -> anyhow::Result<()> {
        super::super::peer_memory::shuffle(&mut endpoints);
        let connected = self.network.connected_keys();
        for endpoint in endpoints.into_iter().take(256) {
            let key = (
                endpoint.public_key.clone(),
                endpoint.host.clone(),
                endpoint.port,
            );
            if connected.contains(&endpoint.public_key)
                || !self.discovery.pass.as_mut().unwrap().attempted.insert(key)
            {
                continue;
            }
            if let Some(path) = seed_path {
                self.network
                    .connect_for_paths(
                        endpoint,
                        (1..=path.len()).map(|n| path[..n].to_vec()).collect(),
                    )
                    .await?;
            } else {
                self.network.connect(endpoint).await?;
            }
        }
        Ok(())
    }
    async fn advance_discovery_pass(&mut self) -> anyhow::Result<()> {
        loop {
            let Some(plan) = self
                .discovery
                .pass
                .as_ref()
                .and_then(|pass| pass.plans.front())
            else {
                self.discovery.pass = None;
                if let Some(memory) = &mut self.discovery.memory {
                    let mut current =
                        BTreeMap::from([(vec!["Nexus".into()], self.network.connected_keys())]);
                    for path in self.levels.ordered() {
                        current.insert(
                            path.clone(),
                            self.network
                                .peers_hosting(&path)
                                .into_iter()
                                .map(|p| p.key)
                                .collect(),
                        );
                    }
                    memory.still_open(&current, now());
                }
                return Ok(());
            };
            let (path, stage, stalled, top_up) =
                (plan.path.clone(), plan.stage, plan.stalled, plan.top_up);
            if path != ["Nexus"] && self.levels.get(&path).is_none() {
                self.discovery.pass.as_mut().unwrap().plans.pop_front();
                continue;
            }
            let is_root = path == ["Nexus"];
            let count = if is_root {
                self.network.connected_keys().len()
            } else {
                self.network.peers_hosting(&path).len()
            };
            if stage > 0 && (self.network.connecting() || self.network.searching_hosted(&path)) {
                return Ok(());
            }
            self.discovery
                .pass
                .as_mut()
                .unwrap()
                .plans
                .front_mut()
                .unwrap()
                .stage += 1;
            match stage {
                0 => {
                    let mut endpoints = if is_root && stalled {
                        self.discovery.seeds.clone()
                    } else {
                        vec![]
                    };
                    if (count < self.discovery.config.peer_target || stalled)
                        && let Some(memory) = &self.discovery.memory
                    {
                        endpoints.extend(memory.candidates(
                            &path,
                            &self.network.connected_keys(),
                            4,
                        ));
                    }
                    self.discovery_dials(endpoints, None).await?;
                }
                1 if !is_root => {
                    if !self.network.peers().is_empty()
                        && (count == 0
                            || stalled
                            || (top_up && count < self.discovery.config.peer_target))
                    {
                        self.network.discover_hosted(path).await?;
                    }
                }
                2 if count < self.discovery.config.peers_before_seeds => {
                    let endpoints = if is_root {
                        self.discovery.config.seed_peers.clone()
                    } else {
                        self.child_settings
                            .get(&path)
                            .map(|settings| settings.peers.clone())
                            .unwrap_or_default()
                    };
                    self.discovery_dials(endpoints, Some(&path)).await?;
                }
                3 if is_root && count < self.discovery.config.peers_before_seeds => {
                    self.discovery_dials(
                        self.discovery.config.fixed_seed_peers.clone(),
                        Some(&path),
                    )
                    .await?;
                }
                4 => {
                    self.discovery.pass.as_mut().unwrap().plans.pop_front();
                }
                _ => {}
            }
        }
    }
    fn drain_announcements(&mut self, now: i64) -> anyhow::Result<()> {
        if now < self.discovery.next_announcement_batch {
            return Ok(());
        }
        self.discovery.next_announcement_batch = now.saturating_add(250);
        let expiry =
            (now.max(0) as u64 / 1000).saturating_add(self.discovery.config.announce_lifetime);
        for _ in 0..16 {
            let Some(key) = self.discovery.announcements.pop_front() else {
                break;
            };
            self.network.announce_provider(key.clone(), expiry)?;
            self.discovery.announced.insert(key, expiry);
        }
        Ok(())
    }
    pub(super) async fn maintain_discovery(&mut self) -> anyhow::Result<()> {
        // Pinned Swift defaults. Each request schedules transport work and returns;
        // the Network adapter bounds outstanding per-level lookups independently.
        let interval = self.discovery.config.interval();
        let cadence = self.discovery.config.cadence();
        let target = self.discovery.config.peer_target;
        let now = now();
        if self.discovery.pass.is_some() {
            self.advance_discovery_pass().await?;
            if self.discovery.pass.is_some() {
                return self.drain_announcements(now);
            }
        }
        let connected = !self.network.peers().is_empty();
        let paths = self.levels.ordered();
        let path_set: BTreeSet<_> = paths.iter().cloned().collect();
        let new_connection = connected && !self.discovery.connected;
        if !new_connection
            && self.discovery.last_paths == path_set
            && now < self.discovery.next_pass
        {
            self.discovery.connected = connected;
            return self.drain_announcements(now);
        }
        self.discovery.next_pass = now.saturating_add(cadence);
        self.discovery.last_paths = path_set;
        if new_connection {
            // Announcements queued without an open session did not reach the DHT.
            self.discovery.announced.clear();
        }
        self.discovery.connected = connected;
        let mut wanted: BTreeSet<_> = paths.iter().map(|p| rendezvous(&self.genesis, p)).collect();
        wanted.extend(self.storage.operator_pins()?);
        wanted.extend(self.read_endpoint_keys());
        let withdrawn: Vec<_> = self
            .discovery
            .announced
            .keys()
            .filter(|k| !wanted.contains(*k))
            .cloned()
            .collect();
        for key in withdrawn {
            self.network.stop_providing(key.clone())?;
            self.discovery.announced.remove(&key);
        }
        let seconds = now.max(0) as u64 / 1000;
        self.discovery.announcements = wanted
            .into_iter()
            .filter(|key| {
                self.discovery.announced.get(key).is_none_or(|expiry| {
                    *expiry <= seconds.saturating_add((cadence as u64 * 3) / 2000)
                })
            })
            .collect();
        self.drain_announcements(now)?;
        let root = vec!["Nexus".into()];
        let root_tip = self.chain.tree().heaviest_executed_tip(&BTreeSet::new());
        let mut heights = vec![(
            root.clone(),
            self.chain.tree().height(&root_tip).unwrap_or(0),
        )];
        for path in &paths {
            let tree = self.levels.get(path).unwrap().tree();
            let tip = tree.heaviest_executed_tip(&BTreeSet::new());
            heights.push((path.clone(), tree.height(&tip).unwrap_or(0)));
        }
        let due = self
            .discovery
            .search
            .due(&heights, now, interval, connected);
        let low: Vec<_> = paths
            .iter()
            .filter(|p| self.network.peers_hosting(p).len() < target)
            .cloned()
            .collect();
        let top = self
            .discovery
            .search
            .top_ups(&low, now, interval, connected);
        self.discovery
            .search
            .retain(&heights.iter().map(|(p, _)| p.clone()).collect());
        if let Some(memory) = &mut self.discovery.memory {
            for path in &paths {
                memory.track(path.clone(), now);
            }
        }
        self.discovery.pass = Some(DiscoveryPass {
            plans: std::iter::once(root)
                .chain(paths)
                .map(|path| DiscoveryPlan {
                    stalled: due.contains(&path),
                    top_up: top.contains(&path),
                    path,
                    stage: 0,
                })
                .collect(),
            attempted: BTreeSet::new(),
        });
        self.advance_discovery_pass().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operator_boundaries_disable_search_without_disabling_maintenance() {
        let mut config = DiscoveryConfig::default();
        for interval in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            config.peer_search_interval = interval;
            assert_eq!(config.interval(), 0);
            assert_eq!(config.cadence(), 600_000);
        }
        config.peer_search_interval = 0.001;
        assert_eq!(config.interval(), 1);
        assert_eq!(config.cadence(), 1000);
        config.peer_search_interval = f64::MAX;
        assert!(config.interval() > 0);
        assert_eq!(config.cadence(), 600_000);
        for lifetime in [0, 86401, u64::MAX] {
            config.announce_lifetime = lifetime;
            assert!(config.validate().is_err());
        }
        for lifetime in [1, 1200, 86400] {
            config.announce_lifetime = lifetime;
            assert!(config.validate().is_ok());
        }
    }
}
