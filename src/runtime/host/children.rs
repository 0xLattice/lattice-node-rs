//! Hosted levels share the actor's durable storage and authenticated sessions.
use super::*;
impl Host {
    pub(super) fn child_parent(
        &self,
        path: &[String],
    ) -> anyhow::Result<lattice_block_tree::ChainTree> {
        let parent = &path[..path.len() - 1];
        if parent == ["Nexus"] {
            Ok(self.chain.tree().clone())
        } else {
            self.levels
                .get(parent)
                .map(|c| c.tree().clone())
                .ok_or_else(|| anyhow::anyhow!("hosted parent missing"))
        }
    }
    pub(super) fn wake_children(&mut self) -> anyhow::Result<()> {
        for (path, effects) in self.levels.parent_advanced(&self.chain, now())? {
            self.queue.push_back(Work::Child(path, effects));
        }
        self.refresh_child_mining()?;
        Ok(())
    }
    pub(super) fn child_peer_changed(&mut self, peer: &PeerId) {
        for path in self.levels.ordered() {
            let hosted = self.network.peers_hosting(&path).contains(peer);
            let core = self.levels.get_mut(&path).unwrap();
            if hosted {
                if !core.sync().peers.contains_key(peer) {
                    let effects = core.peer_ready(peer.clone(), now());
                    self.queue.push_back(Work::Child(path, effects));
                }
            } else {
                core.peer_closed(peer);
            }
        }
    }
    pub(super) fn child_tick(&mut self) -> anyhow::Result<()> {
        for path in self.levels.ordered() {
            let effects = self.levels.get_mut(&path).unwrap().tick(now())?;
            self.queue.push_back(Work::Child(path.clone(), effects));
            let parent = self.child_parent(&path)?;
            self.levels
                .get_mut(&path)
                .unwrap()
                .schedule_derivations(&parent, &[]);
            self.child_proofs(&path)?;
        }
        Ok(())
    }
    pub(super) fn child_message(
        &mut self,
        path: &[String],
        peer: &PeerId,
        message: SyncMessage,
    ) -> anyhow::Result<()> {
        let parent = self.child_parent(path)?;
        let core = self.levels.get_mut(path).unwrap();
        let effects = match message {
            SyncMessage::Stream(page) => core.stream_page(peer, page, now())?,
            SyncMessage::Headers(response) => {
                core.received_headers(peer, response, Some(&parent), now())?
            }
            request => core.request(peer, request, now()),
        };
        self.queue.push_back(Work::Child(path.to_vec(), effects));
        self.child_proofs(path)
    }
    pub(super) fn child_proofs(&mut self, path: &[String]) -> anyhow::Result<()> {
        // Each check is the real verifier. Index results cannot mint facts and
        // completions still cross ChildChainCore's private admission boundary.
        loop {
            let work = self.levels.get_mut(path).unwrap().proof_work();
            let derived = self.levels.get_mut(path).unwrap().derivation_work();
            if work.lookup.is_empty() && work.checks.is_empty() && derived.is_empty() {
                break;
            }
            let mut derivation_inputs = Vec::with_capacity(derived.len());
            for job in derived {
                let stored = self
                    .storage
                    .journal()
                    .header(job.child_cid())?
                    .map(|(b, _)| b);
                let upstream = if path.len() > 2 {
                    self.storage
                        .journal()
                        .saved_proofs(&path[..path.len() - 1], &[job.carrier().to_owned()])?
                        .remove(job.carrier())
                        .unwrap_or_default()
                } else {
                    vec![]
                };
                derivation_inputs.push((job, stored, upstream));
            }
            let completions = validation::map(derivation_inputs, |(job, stored, upstream)| {
                job.run(stored.as_ref(), &upstream, self.storage.as_ref())
            });
            for completion in completions {
                let effects = self
                    .levels
                    .get_mut(path)
                    .unwrap()
                    .derivation_completed(completion, now())?;
                self.queue.push_back(Work::Child(path.to_vec(), effects));
            }
            if !work.lookup.is_empty() {
                let found = self.storage.journal().saved_proofs(path, &work.lookup)?;
                for cid in work.lookup {
                    self.levels
                        .get_mut(path)
                        .unwrap()
                        .proofs_found(&cid, found.get(&cid).cloned().unwrap_or_default());
                }
            }
            let mut check_inputs = Vec::with_capacity(work.checks.len());
            for job in work.checks {
                let stored = if job.block().is_none() {
                    self.storage
                        .journal()
                        .header(&job.key().child_cid)?
                        .map(|(b, _)| b)
                } else {
                    None
                };
                check_inputs.push((job, stored));
            }
            let completions =
                validation::map(check_inputs, |(job, stored)| job.run(stored.as_ref()));
            let missing = completions
                .iter()
                .any(|result| result.missing_block().is_some());
            for completion in completions {
                let effects = self
                    .levels
                    .get_mut(path)
                    .unwrap()
                    .proof_completed(completion, now())?;
                self.queue.push_back(Work::Child(path.to_vec(), effects));
            }
            // A missing held block asks the index again on a later actor turn.
            // Immediately draining that retry would spin on unavailable content.
            if missing {
                break;
            }
        }
        Ok(())
    }
    pub(super) async fn child_effects(
        &mut self,
        path: Vec<String>,
        effects: ChainEffects,
    ) -> anyhow::Result<()> {
        if self.levels.get(&path).is_none() {
            return Ok(());
        }
        for effect in effects.sync {
            match effect {
                SyncEffect::Send(peer, message) => {
                    let (topic, bytes) = wire::encode(&message, &path)?;
                    self.network.send(peer, topic.into(), bytes, 0).await?;
                }
                SyncEffect::DisconnectStalled(peer) => self.network.disconnect(&peer).await?,
                SyncEffect::ServeHeaders {
                    peer,
                    token,
                    request_id,
                    cids,
                    has_more,
                } => {
                    let entries = self.levels.get(&path).unwrap().served_entries(&cids)?;
                    let mut response = HeadersResponse {
                        request_id,
                        entries,
                        has_more,
                    };
                    let (topic, bytes) = loop {
                        match wire::encode(&SyncMessage::Headers(response.clone()), &path) {
                            Ok(value) => break value,
                            Err(wire::WireError::Oversized) if !response.entries.is_empty() => {
                                response.entries.pop();
                                response.has_more = true;
                            }
                            Err(e) => return Err(e.into()),
                        }
                    };
                    let id = self.id()?;
                    self.child_served
                        .insert(id, (path.clone(), peer.clone(), token));
                    self.network.send(peer, topic.into(), bytes, id).await?;
                }
            }
        }
        for effect in effects.headers {
            match effect {
                HeaderEffect::DisconnectInvalidProof(peer) => {
                    self.network.disconnect_invalid_proof(&peer).await?
                }
                HeaderEffect::FetchChildIndex { peer, cid } => {
                    let id = self.id()?;
                    self.fetches.insert(
                        id,
                        Fetch::ChildIndex(path.clone(), peer.clone(), cid.clone()),
                    );
                    self.network.fetch(id, cid, false, Some(&peer)).await?;
                }
            }
        }
        for effect in effects.bodies {
            match effect {
                BodyEffect::Fetch(cid) => {
                    if !self.needs_fetch.contains(&cid) && self.storage.fetch_volume(&cid).is_some()
                    {
                        let effects = self
                            .levels
                            .get_mut(&path)
                            .unwrap()
                            .body_arrived(&cid, now());
                        self.queue.push_back(Work::Child(path.clone(), effects));
                    } else {
                        let id = self.id()?;
                        self.fetches
                            .insert(id, Fetch::ChildBody(path.clone(), cid.clone()));
                        let peer = self.network.peers_hosting(&path).into_iter().next();
                        self.network.fetch(id, cid, true, peer.as_ref()).await?;
                    }
                }
                BodyEffect::Cancel(cid) => {
                    let ids: Vec<_> = self
                        .fetches
                        .iter()
                        .filter_map(|(id, f)| {
                            matches!(f,Fetch::ChildBody(p,c)if p==&path&&c==&cid).then_some(*id)
                        })
                        .collect();
                    for id in ids {
                        self.fetches.remove(&id);
                        self.network.cancel_fetch(id).await?;
                    }
                    self.needs_fetch.remove(&cid);
                }
                BodyEffect::Connect(job) => {
                    let storage = self.storage.clone();
                    storage.atomic_fact_turn(|| -> anyhow::Result<()> {
                        let parent = self.child_parent(&path)?;
                        let core = self.levels.get_mut(&path).unwrap();
                        let effects = core.run_connection(&job, &parent, now())?;
                        if core.tree().is_executed(job.block_hash())
                            || core.tree().is_excluded_root(job.block_hash())
                        {
                            self.needs_fetch.remove(job.block_hash());
                        } else if !core.awaiting_parent().contains_key(job.block_hash()) {
                            self.needs_fetch.insert(job.block_hash().into());
                        }
                        self.queue.push_back(Work::Child(path.clone(), effects));
                        // Executed ancestry can advance while the header tip stays fixed.
                        self.wake_children()?;
                        self.child_proofs(&path)?;
                        Ok(())
                    })?;
                }
            }
        }
        Ok(())
    }
}

impl Host {
    pub(super) async fn configure_children(
        &mut self,
        configuration: configuration::HostedConfiguration,
    ) -> anyhow::Result<()> {
        let mut paths = configuration.children.keys().cloned().collect::<Vec<_>>();
        paths.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        let mut volumes = vec![];
        for bytes in &configuration.modules {
            volumes.extend(lattice_cashew::volume::serialize_recursive(
                &Volume::new(lattice_primitives::WasmPolicyModule {
                    bytes: bytes.clone(),
                })?,
                &(),
            )?);
        }
        self.storage
            .replace_cached_volumes("hosted-config", &volumes)?;
        let desired: BTreeSet<_> = paths.iter().cloned().collect();
        for path in self.levels.ordered() {
            if !desired.contains(&path) {
                self.levels.unhost(&path);
            }
        }
        let cancelled = self
            .fetches
            .iter()
            .filter_map(|(id, fetch)| match fetch {
                Fetch::ChildBody(path, _) | Fetch::ChildIndex(path, _, _)
                    if self.levels.get(path).is_none() =>
                {
                    Some(*id)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for id in cancelled {
            self.fetches.remove(&id);
            self.network.cancel_fetch(id).await?;
        }
        self.child_mining
            .retain(|path, _| self.levels.get(path).is_some());
        self.child_served
            .retain(|_, (path, _, _)| self.levels.get(path).is_some());
        let mut failure = None;
        for path in paths {
            if let Err(error) = self.levels.host(
                &mut self.chain,
                path.clone(),
                configuration.children[&path]
                    .spec
                    .clone()
                    .into_iter()
                    .collect(),
                StreamConfig::default(),
                64,
            ) {
                failure = Some(error);
            }
        }
        self.child_settings = configuration
            .children
            .into_iter()
            .filter(|(path, _)| self.levels.get(path).is_some())
            .collect();
        // Child seeds are a fallback layer, not unconditional configured peers.
        // Reapplying the same paths with changed seeds must also wake maintenance.
        self.discovery.changed();
        self.network.set_hosted(&self.levels.ordered()).await?;
        for peer in self.network.peers() {
            self.child_peer_changed(&peer);
        }
        self.wake_children()?;
        if let Some(error) = failure {
            return Err(error.into());
        }
        Ok(())
    }
}

impl Host {
    pub(super) fn child_templates(
        &self,
        root_tip: &str,
    ) -> anyhow::Result<BTreeMap<Vec<String>, super::super::merged_template::ChildTemplateContext>>
    {
        use super::super::merged_template::ChildTemplateContext;
        let mut entering =
            BTreeMap::from([(vec!["Nexus".to_owned()], self.block(root_tip)?.post_state)]);
        let mut contexts = BTreeMap::new();
        for path in self.levels.ordered() {
            let tree = self.levels.get(&path).unwrap().tree();
            let original = tree.heaviest_executed_tip(&BTreeSet::new());
            let tip = tree.is_executed(&original).then_some(original);
            let parent = self.child_parent(&path)?;
            let selected = tip.as_ref().and_then(|tip| {
                entering.get(&path[..path.len() - 1]).and_then(|state| {
                    tree.select_child_template_tip(tip, &parent, &state.0.raw_cid)
                })
            });
            let previous = selected
                .as_ref()
                .map(|s| {
                    Header::<Block>::from_cid(&s.cid)?
                        .resolve(self.storage.as_ref(), None)
                        .map(Volume)
                })
                .transpose()?;
            if let Some(previous) = &previous
                && !selected.as_ref().unwrap().below_act_on_tip
            {
                entering.insert(
                    path.clone(),
                    previous.0.node.as_ref().unwrap().post_state.clone(),
                );
            }
            let settings = self.child_settings.get(&path);
            contexts.insert(
                path.clone(),
                ChildTemplateContext {
                    tip_cid: tip,
                    best_header_tip: tree.canonical_tip().into(),
                    previous,
                    anchor: selected
                        .as_ref()
                        .and_then(|s| s.difficulty_anchor.as_ref())
                        .map(|a| lattice_validation::DifficultyAnchor {
                            block_height: a.block_height,
                            timestamp: a.timestamp,
                            target: UInt256(a.target),
                        }),
                    transactions: self
                        .child_mining
                        .get(&path)
                        .map(|m| {
                            m.core
                                .state()
                                .mempool()
                                .items()
                                .into_iter()
                                .map(|i| i.transaction)
                                .collect()
                        })
                        .unwrap_or_default(),
                    genesis_spec: if tree.genesis_roots().is_empty() {
                        settings.and_then(|s| s.spec.clone())
                    } else {
                        None
                    },
                    genesis_premine_recipient: settings.and_then(|s| s.premine_recipient.clone()),
                    genesis_transactions: settings
                        .map(|s| s.genesis_transactions.clone())
                        .unwrap_or_default(),
                    genesis_target: UInt256([255; 32]),
                    below_act_on_tip: selected.is_some_and(|s| s.below_act_on_tip),
                },
            );
        }
        Ok(contexts)
    }
    pub(super) fn mined_children(&mut self, root: &Block) -> anyhow::Result<Vec<(String, String)>> {
        let root_cid = content_id(root)?;
        let mut pending = vec![(
            vec!["Nexus".into()],
            root.clone(),
            None::<lattice_block_tree::ChildBlockProof>,
        )];
        let mut durable = vec![];
        while let Some((parent_path, carrier, parent_proof)) = pending.pop() {
            let children = carrier.children.resolve(self.storage.as_ref(), None)?;
            for (directory, header) in children.node.as_ref().unwrap().iter() {
                let mut path = parent_path.clone();
                path.push(directory.clone());
                if self.levels.get(&path).is_none() {
                    continue;
                }
                let block = header
                    .0
                    .resolve(self.storage.as_ref(), None)?
                    .node
                    .as_deref()
                    .unwrap()
                    .clone();
                let hop = lattice_block_tree::ChildBlockProof::generate(
                    &content_id(&carrier)?,
                    directory,
                    self.storage.as_ref(),
                )?;
                let proof = parent_proof
                    .as_ref()
                    .map(|p| p.composing(&hop))
                    .unwrap_or(hop);
                debug_assert_eq!(proof.root_cid, root_cid);
                let child_index = block
                    .children
                    .resolve(self.storage.as_ref(), None)?
                    .node
                    .as_deref()
                    .unwrap()
                    .clone();
                let spec = if block.height == 0 {
                    Some(
                        block
                            .spec
                            .0
                            .resolve(self.storage.as_ref(), None)?
                            .node
                            .as_deref()
                            .unwrap()
                            .clone(),
                    )
                } else {
                    None
                };
                let parent = self.child_parent(&path)?;
                self.levels.get_mut(&path).unwrap().queue_header(
                    HeaderEntry {
                        block: block.clone(),
                        children: Some(child_index),
                        spec,
                        proofs: vec![proof.clone()],
                    },
                    None,
                    Some(&parent),
                    now(),
                )?;
                self.child_proofs(&path)?;
                if parent_path.len() == 1 {
                    durable.push((directory.clone(), header.0.raw_cid.clone()));
                }
                pending.push((path, block, Some(proof)));
            }
        }
        Ok(durable)
    }
}
