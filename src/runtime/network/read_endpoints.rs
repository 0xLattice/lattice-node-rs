//! Bounded endpoint lookups share the transport task, never block the host actor.
use super::*;
use crate::core::read_endpoints::*;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub struct ReadEndpointConfig {
    pub public_read_url: Option<String>,
    pub public_submit: bool,
}
impl ReadEndpointConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.public_read_url.as_deref().is_none_or(valid_url),
            "invalid publicReadUrl"
        );
        Ok(())
    }
}
struct Lookup {
    path: Vec<String>,
    tags: Vec<u64>,
    deadline: Instant,
    grace: Option<Instant>,
    searching: bool,
    asked: BTreeSet<String>,
    unanswered: BTreeSet<u64>,
    requests: BTreeMap<u64, String>,
    waiting: BTreeMap<String, ReadEndpointRequest>,
    urls: Vec<DeclaredReadEndpoint>,
    per_responder: BTreeMap<String, usize>,
}
#[derive(Default)]
pub(super) struct ReadEndpoints {
    pub(super) config: ReadEndpointConfig,
    pub(super) keep: BTreeSet<String>,
    lookups: BTreeMap<u64, Lookup>,
    cache: BTreeMap<Vec<String>, (Instant, Vec<DeclaredReadEndpoint>)>,
    found: BTreeMap<Vec<String>, (Instant, Vec<DeclaredReadEndpoint>)>,
    holds: BTreeMap<String, BTreeSet<u64>>,
    next: u64,
}
impl ReadEndpoints {
    fn kept(&self, path: &[String], now: Instant) -> Vec<DeclaredReadEndpoint> {
        self.found
            .get(path)
            .filter(|(at, _)| now.saturating_duration_since(*at) < Duration::from_secs(600))
            .map(|(_, urls)| urls.clone())
            .unwrap_or_default()
    }
    fn receive(&mut self, response: ReadEndpointResponse, key: &str, now: Instant) {
        if !valid_path(&response.chain_path) || !valid_url(&response.url) {
            return;
        }
        let Some(lookup) = self.lookups.values_mut().find(|l| {
            l.path == response.chain_path
                && l.requests
                    .get(&response.request_id)
                    .is_some_and(|k| k == key)
        }) else {
            return;
        };
        let accepts_submit = response.accepts_submit.unwrap_or(false);
        if let Some(held) = lookup.urls.iter_mut().find(|e| e.url == response.url) {
            held.accepts_submit |= accepts_submit;
        } else {
            let count = lookup.per_responder.entry(key.into()).or_default();
            if *count >= 2 {
                return;
            }
            *count += 1;
            lookup.urls.push(DeclaredReadEndpoint {
                url: response.url,
                accepts_submit,
            });
        }
        lookup.unanswered.remove(&response.request_id);
        lookup.grace.get_or_insert(now + Duration::from_millis(500));
    }
}
impl Network {
    pub fn configure_read_endpoints(
        &mut self,
        config: ReadEndpointConfig,
        keep: impl IntoIterator<Item = String>,
    ) -> anyhow::Result<()> {
        config.validate()?;
        self.read_endpoints.config = config;
        self.read_endpoints.keep.extend(keep);
        Ok(())
    }
    pub async fn lookup_read_endpoints(
        &mut self,
        tag: u64,
        path: Vec<String>,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        if let Some((_, urls)) = self
            .read_endpoints
            .cache
            .get(&path)
            .filter(|(until, _)| now < *until)
        {
            self.events
                .push_back(NetworkEvent::ReadEndpoints(tag, path, urls.clone()));
            return Ok(());
        }
        if let Some(lookup) = self
            .read_endpoints
            .lookups
            .values_mut()
            .find(|l| l.path == path)
        {
            lookup.tags.push(tag);
            return Ok(());
        }
        if self.read_endpoints.lookups.len() >= 16 {
            let urls = self.read_endpoints.kept(&path, now);
            self.events
                .push_back(NetworkEvent::ReadEndpoints(tag, path, urls));
            return Ok(());
        }
        self.read_endpoints.lookups.insert(
            tag,
            Lookup {
                path: path.clone(),
                tags: vec![tag],
                deadline: now + Duration::from_secs(5),
                grace: None,
                searching: true,
                asked: BTreeSet::from([self.public_key.clone()]),
                unanswered: BTreeSet::new(),
                requests: BTreeMap::new(),
                waiting: BTreeMap::new(),
                urls: vec![],
                per_responder: BTreeMap::new(),
            },
        );
        self.request(
            Request::ProviderMemberships {
                root_cid: rendezvous(&self.genesis, &path),
            },
            Some(Pending::ReadMemberships { lookup: tag }),
        )
        .await
    }
    pub(super) async fn read_memberships(
        &mut self,
        lookup: u64,
        result: RequestResult,
    ) -> anyhow::Result<()> {
        let Some(path) = self
            .read_endpoints
            .lookups
            .get(&lookup)
            .map(|l| l.path.clone())
        else {
            return Ok(());
        };
        let keys = match result {
            RequestResult::ProviderMemberships(keys) => keys
                .into_iter()
                .filter(|(_, expiry)| *expiry > unix_seconds())
                .map(|(key, _)| key.hex())
                .collect::<BTreeSet<_>>(),
            _ => BTreeSet::new(),
        };
        let endpoints = self
            .sessions
            .values()
            .filter(|s| s.ready && keys.contains(&s.id.key))
            .map(|s| PeerEndpoint {
                public_key: s.id.key.clone(),
                host: String::new(),
                port: 0,
            })
            .collect();
        self.ask_read_providers(lookup, endpoints, true).await?;
        self.request(
            Request::DiscoverProviders {
                root_cid: rendezvous(&self.genesis, &path),
            },
            Some(Pending::ReadProviders { lookup }),
        )
        .await
    }
    pub(super) async fn read_providers(
        &mut self,
        lookup: u64,
        result: RequestResult,
    ) -> anyhow::Result<()> {
        let endpoints = match result {
            RequestResult::Endpoints(endpoints) => endpoints,
            _ => vec![],
        };
        self.ask_read_providers(lookup, endpoints, false).await?;
        if let Some(l) = self.read_endpoints.lookups.get_mut(&lookup) {
            l.searching = false;
        }
        self.tick_read_endpoints().await
    }
    async fn ask_read_providers(
        &mut self,
        lookup: u64,
        mut candidates: Vec<PeerEndpoint>,
        connected: bool,
    ) -> anyhow::Result<()> {
        let Some(l) = self.read_endpoints.lookups.get(&lookup) else {
            return Ok(());
        };
        let room = if connected {
            4
        } else {
            8usize.saturating_sub(l.asked.len().saturating_sub(1))
        };
        use rand_core::RngCore;
        let mut rng = rand_core::OsRng;
        let mut selected = vec![];
        while !candidates.is_empty() && selected.len() < room {
            let n = candidates.len() as u64;
            let limit = u64::MAX - u64::MAX % n;
            let mut draw = rng.next_u64();
            while draw >= limit {
                draw = rng.next_u64();
            }
            let mut endpoint = candidates.swap_remove((draw % n) as usize);
            let Some(key) = ivy_core::PeerKey::from_hex(&endpoint.public_key) else {
                continue;
            };
            endpoint.public_key = key.hex();
            let l = self.read_endpoints.lookups.get_mut(&lookup).unwrap();
            if !l.asked.insert(endpoint.public_key.clone()) {
                continue;
            }
            selected.push(endpoint);
        }
        for endpoint in selected {
            self.read_endpoints.next = self
                .read_endpoints
                .next
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("read request IDs exhausted"))?;
            let request_id = self.read_endpoints.next;
            let l = self.read_endpoints.lookups.get_mut(&lookup).unwrap();
            let request = ReadEndpointRequest {
                chain_path: l.path.clone(),
                request_id,
            };
            l.requests.insert(request_id, endpoint.public_key.clone());
            l.unanswered.insert(request_id);
            l.waiting.insert(endpoint.public_key.clone(), request);
            if !self.sessions.contains_key(&endpoint.public_key) {
                self.read_endpoints
                    .holds
                    .entry(endpoint.public_key.clone())
                    .or_default()
                    .insert(lookup);
                self.request(
                    Request::Connect(endpoint.clone()),
                    Some(Pending::ReadDial {
                        lookup,
                        key: endpoint.public_key,
                    }),
                )
                .await?;
            } else if let Some(holds) = self.read_endpoints.holds.get_mut(&endpoint.public_key) {
                holds.insert(lookup);
            }
        }
        self.flush_read_asks().await
    }
    pub(super) async fn read_dialed(
        &mut self,
        lookup: u64,
        key: String,
        result: RequestResult,
    ) -> anyhow::Result<()> {
        if matches!(result, RequestResult::Connected) {
            if self.read_endpoints.lookups.contains_key(&lookup) {
                self.read_endpoints
                    .holds
                    .entry(key)
                    .or_default()
                    .insert(lookup);
            } else if !self.read_endpoints.keep.contains(&key)
                && !self.read_endpoints.holds.contains_key(&key)
                && let Some(s) = self.sessions.get(&key)
            {
                let peer = s.id.clone();
                self.disconnect(&peer).await?;
            }
        }
        self.flush_read_asks().await
    }
    async fn flush_read_asks(&mut self) -> anyhow::Result<()> {
        let mut asks = vec![];
        for lookup in self.read_endpoints.lookups.values_mut() {
            let ready = lookup
                .waiting
                .keys()
                .filter_map(|key| {
                    self.sessions
                        .get(key)
                        .filter(|s| s.ready)
                        .map(|s| s.id.clone())
                })
                .collect::<Vec<_>>();
            for peer in ready {
                if let Some(request) = lookup.waiting.remove(&peer.key) {
                    asks.push((peer, request));
                }
            }
        }
        for (peer, request) in asks {
            self.send(peer, REQUEST.into(), encode(&request)?, 0)
                .await?;
        }
        Ok(())
    }
    pub(super) async fn read_endpoint_message(
        &mut self,
        peer: PeerId,
        topic: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        if topic == REQUEST {
            if let Some(request) =
                decode::<ReadEndpointRequest>(payload).filter(|r| valid_path(&r.chain_path))
                && (request.chain_path == ["Nexus"] || self.hosted.contains(&request.chain_path))
                && let Some(url) = self.read_endpoints.config.public_read_url.clone()
            {
                let mut response = ReadEndpointResponse {
                    chain_path: request.chain_path,
                    request_id: request.request_id,
                    url,
                    accepts_submit: None,
                };
                if self.read_endpoints.config.public_submit {
                    response.accepts_submit = Some(true);
                    self.send(peer.clone(), RESPONSE_V2.into(), encode(&response)?, 0)
                        .await?;
                    response.accepts_submit = None;
                }
                self.send(peer, RESPONSE.into(), encode(&response)?, 0)
                    .await?;
            }
        } else if let Some(response) = decode::<ReadEndpointResponse>(payload).filter(|r| {
            if topic == RESPONSE_V2 {
                r.accepts_submit.is_some()
            } else {
                r.accepts_submit.is_none()
            }
        }) {
            self.read_endpoints
                .receive(response, &peer.key, Instant::now());
        }
        self.tick_read_endpoints().await
    }
    pub(super) async fn tick_read_endpoints(&mut self) -> anyhow::Result<()> {
        self.flush_read_asks().await?;
        let now = Instant::now();
        let done = self
            .read_endpoints
            .lookups
            .iter()
            .filter(|(_, l)| {
                now >= l.deadline
                    || l.grace.is_some_and(|g| now >= g)
                    || (!l.searching && l.unanswered.is_empty())
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in done {
            let lookup = self.read_endpoints.lookups.remove(&id).unwrap();
            let found = !lookup.urls.is_empty();
            let urls = if found {
                self.read_endpoints
                    .found
                    .insert(lookup.path.clone(), (now, lookup.urls.clone()));
                lookup.urls
            } else {
                self.read_endpoints.kept(&lookup.path, now)
            };
            self.read_endpoints.cache.insert(
                lookup.path.clone(),
                (
                    now + Duration::from_secs(if found { 30 } else { 5 }),
                    urls.clone(),
                ),
            );
            for tag in lookup.tags {
                self.events.push_back(NetworkEvent::ReadEndpoints(
                    tag,
                    lookup.path.clone(),
                    urls.clone(),
                ));
            }
            let pending=self.pending.iter().filter_map(|(request,p)|matches!(p,Pending::ReadMemberships{lookup}|Pending::ReadProviders{lookup}|Pending::ReadDial{lookup,..}if *lookup==id).then_some(*request)).collect::<Vec<_>>();
            for request in pending {
                self.pending.remove(&request);
                self.emit(Event::Cancel(request))?;
            }
            let held = self
                .read_endpoints
                .holds
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for key in held {
                let holds = self.read_endpoints.holds.get_mut(&key).unwrap();
                holds.remove(&id);
                if holds.is_empty() {
                    self.read_endpoints.holds.remove(&key);
                    if !self.read_endpoints.keep.contains(&key)
                        && let Some(s) = self.sessions.get(&key)
                    {
                        let peer = s.id.clone();
                        self.disconnect(&peer).await?;
                    }
                }
            }
        }
        self.read_endpoints
            .cache
            .retain(|_, (until, _)| now < *until);
        self.read_endpoints
            .found
            .retain(|_, (at, _)| now.saturating_duration_since(*at) < Duration::from_secs(600));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn network() -> (tempfile::TempDir, Network) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(NodeStorage::open(dir.path(), "fixture", "reads").unwrap());
        let network = Network::bind(
            "127.0.0.1:0".parse().unwrap(),
            ed25519_dalek::SigningKey::from_bytes(&[99; 32]),
            "fixture".into(),
            storage,
        )
        .await
        .unwrap();
        // No scripted provider ever reaches a live dial. Only state/command
        // generation is exercised here; daemon tests cover actual transport.
        network.task.abort();
        (dir, network)
    }
    fn path(n: usize) -> Vec<String> {
        vec!["Nexus".into(), format!("Child{n}")]
    }
    fn providers() -> Vec<PeerEndpoint> {
        (1..=200)
            .map(|n| PeerEndpoint {
                public_key: ivy_core::PeerKey::from_bytes(
                    ed25519_dalek::SigningKey::from_bytes(&[n; 32])
                        .verifying_key()
                        .as_bytes(),
                )
                .unwrap()
                .hex(),
                host: "127.0.0.1".into(),
                port: 1,
            })
            .collect()
    }
    #[tokio::test]
    async fn coalescing_concurrency_fanout_and_deadline_are_bounded() {
        let (_dir, mut n) = network().await;
        for i in 0..16 {
            n.lookup_read_endpoints(i as u64, path(i)).await.unwrap();
        }
        n.lookup_read_endpoints(100, path(0)).await.unwrap();
        n.lookup_read_endpoints(101, path(99)).await.unwrap();
        assert_eq!(n.read_endpoints.lookups.len(), 16);
        assert_eq!(n.read_endpoints.lookups[&0].tags, [0, 100]);
        assert!(
            matches!(n.events.pop_front(),Some(NetworkEvent::ReadEndpoints(101,_,urls))if urls.is_empty())
        );
        n.ask_read_providers(0, providers(), true).await.unwrap();
        assert_eq!(n.read_endpoints.lookups[&0].requests.len(), 4);
        n.read_providers(0, RequestResult::Endpoints(providers()))
            .await
            .unwrap();
        assert_eq!(n.read_endpoints.lookups[&0].requests.len(), 8);
        n.read_endpoints.lookups.get_mut(&0).unwrap().deadline = Instant::now();
        n.tick_read_endpoints().await.unwrap();
        assert!(!n.read_endpoints.lookups.contains_key(&0));
        assert!(!n.pending.values().any(|p| matches!(
            p,
            Pending::ReadDial { lookup: 0, .. }
                | Pending::ReadMemberships { lookup: 0 }
                | Pending::ReadProviders { lookup: 0 }
        )));
        assert!(
            n.read_endpoints
                .holds
                .values()
                .all(|holds| !holds.contains(&0))
        );
        let finished = n
            .events
            .iter()
            .filter_map(|e| {
                if let NetworkEvent::ReadEndpoints(id, _, _) = e {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(finished, [0, 100]);
    }
    #[tokio::test]
    async fn authenticated_response_binding_cap_upgrade_and_source_ttl() {
        let (_dir, mut n) = network().await;
        n.lookup_read_endpoints(1, path(1)).await.unwrap();
        n.ask_read_providers(1, providers().into_iter().take(1).collect(), false)
            .await
            .unwrap();
        let (&request_id, key) = n.read_endpoints.lookups[&1].requests.iter().next().unwrap();
        let key = key.clone();
        let response = |chain_path, url: &str, accepts_submit| ReadEndpointResponse {
            chain_path,
            request_id,
            url: url.into(),
            accepts_submit,
        };
        let now = Instant::now();
        n.read_endpoints.receive(
            response(path(2), "https://wrong-path.example", None),
            &key,
            now,
        );
        n.read_endpoints.receive(
            response(path(1), "https://wrong-peer.example", None),
            "other",
            now,
        );
        assert!(n.read_endpoints.lookups[&1].urls.is_empty());
        for i in 0..5 {
            n.read_endpoints.receive(
                response(path(1), &format!("https://host{i}.example"), None),
                &key,
                now,
            );
        }
        assert_eq!(n.read_endpoints.lookups[&1].urls.len(), 2);
        n.read_endpoints.receive(
            response(path(1), "https://host0.example", Some(true)),
            &key,
            now,
        );
        n.read_endpoints
            .receive(response(path(1), "https://host0.example", None), &key, now);
        assert!(n.read_endpoints.lookups[&1].urls[0].accepts_submit);
        n.read_endpoints.lookups.get_mut(&1).unwrap().grace = Some(now);
        n.tick_read_endpoints().await.unwrap();
        assert_eq!(n.read_endpoints.cache[&path(1)].1.len(), 2);
        n.lookup_read_endpoints(2, path(1)).await.unwrap();
        assert!(n.read_endpoints.lookups.is_empty());
        // An empty refresh keeps only a genuinely found source, and for 600s.
        n.read_endpoints.cache.clear();
        n.lookup_read_endpoints(3, path(1)).await.unwrap();
        n.read_providers(3, RequestResult::Endpoints(vec![]))
            .await
            .unwrap();
        assert_eq!(n.read_endpoints.cache[&path(1)].1.len(), 2);
        assert!(n.read_endpoints.cache[&path(1)].0.duration_since(now) < Duration::from_secs(6));
        n.read_endpoints.cache.clear();
        n.read_endpoints.found.get_mut(&path(1)).unwrap().0 = now - Duration::from_secs(601);
        n.lookup_read_endpoints(4, path(1)).await.unwrap();
        n.read_providers(4, RequestResult::Endpoints(vec![]))
            .await
            .unwrap();
        assert!(n.read_endpoints.cache[&path(1)].1.is_empty());
    }
}
