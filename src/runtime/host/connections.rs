//! Root execution validates immutable content off the actor; live fact admission
//! and all resulting effects remain inside the synchronous durable barrier.
use super::*;
use lattice_block_tree::{
    AdmissionError, ConnectJob, RootConnectionCompletion, RootConnectionExecution,
    RootConnectionTask,
};

pub(super) struct ConnectionTask {
    task: RootConnectionTask,
    session: acquire::Session,
    content: crate::storage::ConnectionContent,
}
pub(super) struct ConnectionCompletion {
    verdict: RootConnectionCompletion,
    content: crate::storage::ConnectionContent,
}
impl ConnectionTask {
    pub(super) fn run(self) -> Result<ConnectionCompletion, crate::storage::StoreError> {
        let verdict = self.task.run(
            &acquire::Source {
                snapshot: self.content.snapshot(),
                session: &self.session,
            },
            now(),
        )?;
        self.session.check()?;
        if let Some(header) = verdict.accepted_header() {
            self.content.preserve_acquired(
                header,
                &self.session.volumes(),
                &self.session.entries(),
            )?;
        }
        Ok(ConnectionCompletion {
            verdict,
            content: self.content,
        })
    }
}
impl Host {
    fn release_connection_pins(&mut self, cid: &str) -> anyhow::Result<()> {
        self.queued_template_content.remove(cid);
        for scope in self.connection_pins.remove(cid).unwrap_or_default() {
            self.storage.release_cached_volumes(&scope)?;
        }
        Ok(())
    }
    pub(super) fn release_unused_connection_pins(&mut self) -> anyhow::Result<()> {
        let mut wanted = self.chain.wanted_root_bodies();
        for path in self.levels.ordered() {
            if let Some(core) = self.levels.get(&path) {
                wanted.extend(
                    core.wanted_bodies()
                        .into_iter()
                        .map(|cid| format!("{}:{cid}", path.join("/"))),
                );
            }
        }
        let stale: Vec<_> = self
            .connection_pins
            .keys()
            .filter(|cid| !wanted.contains(*cid))
            .cloned()
            .collect();
        for cid in stale {
            self.release_connection_pins(&cid)?;
        }
        Ok(())
    }
    pub(super) fn prepare_connection(
        &mut self,
        job: ConnectJob,
    ) -> anyhow::Result<Option<ConnectionTask>> {
        if !self.chain.owns_root_connection(job.block_hash())
            || self.active_connections.contains(job.block_hash())
        {
            return Ok(None);
        }
        let task = match self.chain.tree().prepare_root_connection(&job) {
            Ok(task) => task,
            Err(
                AdmissionError::StalePreparation
                | AdmissionError::UnavailableEvidence
                | AdmissionError::NotAcceptedAtCurrentChain,
            ) => {
                let effects = self
                    .chain
                    .root_connection_superseded(job.block_hash(), now());
                self.queue.push_back(Work::Chain(effects));
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let snapshot = self.storage.preflight_snapshot()?;
        // The WAL reader now owns the issued template inputs.
        self.queued_template_content.remove(task.block_hash());
        // The completion owns both the snapshot and original body pins. It
        // additionally preserves reachable available dependencies on its worker.
        let content = crate::storage::ConnectionContent::new(
            snapshot,
            self.storage.clone(),
            task.block_hash(),
            self.connection_pins
                .remove(task.block_hash())
                .unwrap_or_default(),
        );
        self.active_connections.insert(task.block_hash().to_owned());
        Ok(Some(ConnectionTask {
            task,
            content,
            session: acquire::Session::new(
                self.acquisition_requests.clone(),
                self.content_cache.clone(),
            ),
        }))
    }
    pub(super) fn complete_connection(
        &mut self,
        completion: ConnectionCompletion,
    ) -> anyhow::Result<()> {
        let ConnectionCompletion { verdict, content } = completion;
        let cid = verdict.block_hash().to_owned();
        // An issued worker is independent of fetch/pipeline ownership. A
        // reorg/refetch may clear that slot while its content verdict remains
        // valid. Only an actually issued completion may cross this boundary.
        if !self.active_connections.remove(&cid) {
            return Ok(());
        }
        let Some(result) = self.chain.tree().complete_root_connection(verdict)? else {
            let effects = self.chain.root_connection_superseded(&cid, now());
            self.queue.push_back(Work::Chain(effects));
            return Ok(());
        };
        let storage = self.storage.clone();
        storage.atomic_fact_turn(|| -> anyhow::Result<()> {
            let effects = match result {
                RootConnectionExecution::Accepted(verified) => {
                    self.needs_fetch.remove(&cid);
                    let effects = self.chain.connected_issued_root(&verified, now())?;
                    if let Some(block) = verified.header().0.node.as_ref()
                        && let Some(children) = block.children.node.as_ref()
                    {
                        self.executed_children.extend(children.keys().cloned());
                    }
                    effects
                }
                RootConnectionExecution::Rejected(rejected) => {
                    self.chain.connected_issued_rejection(rejected, now())?
                }
                RootConnectionExecution::Unavailable => {
                    if self.chain.owns_root_connection(&cid) {
                        self.needs_fetch.insert(cid.clone());
                        self.chain.body_unavailable(&cid, now())
                    } else {
                        ChainEffects::default()
                    }
                }
            };
            self.queue.push_back(Work::Chain(effects));
            self.tip_changed()?;
            self.wake_children()?;
            Ok(())
        })?;
        // Descendant effects consume input/body dependencies in the next drain;
        // keep the lease alive until that complete synchronous drain finishes.
        self.completed_connection_content.push(content);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};
    pub(super) async fn inspect<T: Send + 'static>(
        handle: &HostHandle,
        inspect: impl FnOnce(&mut Host) -> T + Send + 'static,
    ) -> T {
        let (tx, rx) = oneshot::channel();
        handle
            .commands
            .send(Command::Inspect(std::sync::Mutex::new(Box::new(
                move |host| {
                    let _ = tx.send(inspect(host));
                },
            ))))
            .await
            .unwrap();
        timeout(Duration::from_secs(5), rx).await.unwrap().unwrap()
    }
    pub(super) fn config(path: &std::path::Path) -> HostConfig {
        let rows: Value = serde_json::from_str(include_str!(
            "../../../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
        ))
        .unwrap();
        let entries: BTreeMap<String, Vec<u8>> =
            serde_json::from_value(rows[0]["entries"].clone()).unwrap();
        let cid = rows[0]["cid"].as_str().unwrap().to_owned();
        let verified = lattice_validation::validate_root_genesis(
            &Volume(Header::from_cid(&cid).unwrap()),
            &entries,
            i64::MAX,
        )
        .unwrap();
        HostConfig {
            directory: path.into(),
            genesis_cid: cid,
            bootstrap_volumes: lattice_cashew::volume::serialize_recursive(verified.header(), &())
                .unwrap(),
            signing_key: ed25519_dalek::SigningKey::from_bytes(&[74; 32]),
            listen: "127.0.0.1:0".parse().unwrap(),
            min_relay_fee: 1,
            seeds: vec![],
        }
    }
    pub(super) async fn block(handle: &HostHandle) -> (Value, Block) {
        let (code, template) = handle
            .request("/mining/templates", json!({"recipients":[]}))
            .await
            .unwrap();
        assert_eq!(code, 200, "{template}");
        let work_id = template["workID"].as_str().unwrap().to_owned();
        let mut block = inspect(handle, move |host| {
            host.mining
                .state()
                .templates()
                .template(&work_id)
                .unwrap()
                .block
                .clone()
        })
        .await;
        block.nonce = (0..10000)
            .find(|nonce| {
                block.nonce = *nonce;
                block.has_valid_pow()
            })
            .unwrap();
        (template, block)
    }
    pub(super) fn prepare(host: &mut Host, block: &Block) -> ConnectionTask {
        let cid = content_id(block).unwrap();
        let scope = format!("test-connect-{cid}");
        let volumes =
            lattice_cashew::volume::serialize_recursive(&Volume::new(block.clone()).unwrap(), &())
                .unwrap();
        host.storage.cache_volumes(&scope, &volumes).unwrap();
        host.connection_pins
            .entry(cid.clone())
            .or_default()
            .push(scope);
        host.chain
            .admit_header(block, block.children.node.as_deref().unwrap(), now())
            .unwrap();
        let effects = host.chain.body_arrived(&cid, now());
        let job = effects
            .bodies
            .into_iter()
            .find_map(|e| match e {
                BodyEffect::Connect(job) => Some(job),
                _ => None,
            })
            .unwrap();
        host.prepare_connection(job).unwrap().unwrap()
    }
    #[tokio::test(flavor = "current_thread")]
    async fn root_worker_survives_reorg_fetch_cancellation_and_lost_pipeline_slot() {
        let dir = tempfile::tempdir().unwrap();
        let running = start(config(dir.path())).await.unwrap();
        let (_, candidate) = block(&running.handle).await;
        let cid = content_id(&candidate).unwrap();
        let mut sibling = candidate.clone();
        sibling.nonce = (candidate.nonce + 1..candidate.nonce + 100000)
            .find(|nonce| {
                sibling.nonce = *nonce;
                sibling.has_valid_pow()
                    && lattice_block_tree::fork_choice_prefers_block(
                        &content_id(&sibling).unwrap(),
                        &cid,
                    )
            })
            .unwrap();
        let sibling_cid = content_id(&sibling).unwrap();
        let (release, gate) = std::sync::mpsc::channel();
        let (started, began) = oneshot::channel();
        inspect(&running.handle, move |host| {
            let task = prepare(host, &candidate);
            host.preflights.spawn(move || {
                let result = task.run();
                let _ = started.send(());
                gate.recv().unwrap();
                result.map(preflight::Completion::Connection)
            });
        })
        .await;
        timeout(Duration::from_secs(5), began)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            timeout(
                Duration::from_secs(2),
                running.handle.request("/status", json!({}))
            )
            .await
            .unwrap()
            .unwrap()
            .0,
            200
        );
        let old = cid.clone();
        inspect(&running.handle, move |host| {
            host.chain
                .admit_header(&sibling, sibling.children.node.as_deref().unwrap(), now())
                .unwrap();
            assert_eq!(host.chain.tree().canonical_tip(), sibling_cid);
            // A delayed failed fetch can park the old pipeline slot. Its
            // already-issued valid worker must still execute the side fork.
            host.chain.body_unavailable(&old, now());
            assert!(!host.chain.owns_root_connection(&old));
            host.queue.push_back(Work::Chain(ChainEffects {
                bodies: vec![BodyEffect::Cancel(old)],
                ..Default::default()
            }));
        })
        .await;
        release.send(()).unwrap();
        let executed = cid.clone();
        timeout(Duration::from_secs(5), async {
            while !inspect(&running.handle, {
                let cid = executed.clone();
                move |host| host.chain.tree().is_executed(&cid)
            })
            .await
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
        let mut restart = config(dir.path());
        restart.bootstrap_volumes.clear();
        let running = start(restart).await.unwrap();
        inspect(&running.handle, move |host| {
            assert!(host.chain.tree().is_executed(&cid))
        })
        .await;
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn queued_body_pins_survive_sweep_and_mined_reply_waits_for_execution() {
        let dir = tempfile::tempdir().unwrap();
        let running = start_with_retention(
            config(dir.path()),
            crate::storage::RetentionPolicy {
                state: crate::storage::StateRetention::Tip,
                bodies: crate::storage::BodyRetention::None,
            },
        )
        .await
        .unwrap();
        let (template, candidate) = block(&running.handle).await;
        let cid = content_id(&candidate).unwrap();
        let releases = inspect(&running.handle, |host| {
            let (tx, rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), tx).unwrap();
            // Keep the coalesced template request live until the blockers exit.
            let index = host.queue.iter().position(|w| matches!(w, Work::Mining(e) if matches!(e.as_ref(), MiningEffect::BuildTemplate(_)))).unwrap();
            let Work::Mining(effect) = host.queue.remove(index).unwrap() else { unreachable!() };
            let MiningEffect::BuildTemplate(job) = *effect else { unreachable!() };
            let mut releases = vec![];
            while !host.preflights.full() {
                let task = host.prepare_template(job.clone()).unwrap().unwrap();
                let (tx, gate) = std::sync::mpsc::channel(); releases.push(tx);
                host.preflights.spawn(move || { gate.recv().unwrap(); Ok(preflight::Completion::Template(Box::new(task.run()))) });
            }
            (releases, rx)
        }).await;
        let handle = running.handle.clone();
        let submit = tokio::spawn(async move {
            handle
                .request(
                    "/mining/work",
                    json!({"workID":template["workID"],"nonce":candidate.nonce}),
                )
                .await
        });
        let pending = cid.clone();
        timeout(Duration::from_secs(5), async {
            while !inspect(&running.handle, {
                let cid = pending.clone();
                move |host| host.connection_pins.contains_key(&cid)
            })
            .await
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!submit.is_finished());
        assert_eq!(
            running
                .handle
                .request("POST /sweep", json!({}))
                .await
                .unwrap()
                .0,
            200
        );
        let held = cid.clone();
        inspect(&running.handle, move |host| {
            assert!(host.storage.fetch_volume(&held).is_some());
            assert!(!host.active_connections.contains(&held));
            assert!(!host.chain.tree().is_executed(&held));
        })
        .await;
        for release in releases.0 {
            release.send(()).unwrap();
        }
        let (code, result) = timeout(Duration::from_secs(5), submit)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(code, 200, "{result}");
        assert_eq!(result["accepted"], true);
        inspect(&running.handle, move |host| {
            assert!(host.chain.tree().is_executed(&cid));
            assert!(!host.connection_pins.contains_key(&cid));
        })
        .await;
        drop(releases.1);
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn failed_worker_admission_stops_actor_and_recovery_has_no_execution() {
        let dir = tempfile::tempdir().unwrap();
        let running = start(config(dir.path())).await.unwrap();
        let (_, candidate) = block(&running.handle).await;
        let cid = content_id(&candidate).unwrap();
        let (release, gate) = std::sync::mpsc::channel();
        let (started, began) = oneshot::channel();
        inspect(&running.handle, move |host| {
            let task = prepare(host, &candidate);
            host.preflights.spawn(move || {
                let result = task.run();
                let _ = started.send(());
                gate.recv().unwrap();
                result.map(preflight::Completion::Connection)
            });
        })
        .await;
        timeout(Duration::from_secs(5), began)
            .await
            .unwrap()
            .unwrap();
        let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        db.execute_batch("CREATE TRIGGER fail_execution BEFORE INSERT ON admission_batches BEGIN SELECT RAISE(ABORT,'execution failure'); END;").unwrap();
        release.send(()).unwrap();
        assert!(
            timeout(Duration::from_secs(5), running.task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        db.execute_batch("DROP TRIGGER fail_execution").unwrap();
        drop(db);
        let storage =
            NodeStorage::open(dir.path(), &config(dir.path()).genesis_cid, "recovery").unwrap();
        let (tree, _) = storage
            .journal()
            .restore_chain(
                lattice_primitives::ChainRuntimeContext::new(
                    vec!["Nexus".into()],
                    Some(&config(dir.path()).genesis_cid),
                )
                .unwrap(),
                None,
                vec![],
            )
            .unwrap();
        assert!(tree.contains(&cid));
        assert!(!tree.is_executed(&cid));
    }
    #[tokio::test(flavor = "current_thread")]
    async fn held_side_mine_is_answered_before_its_worker_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let running = start(config(dir.path())).await.unwrap();
        let (_, candidate) = block(&running.handle).await;
        let cid = content_id(&candidate).unwrap();
        let mut sibling = candidate.clone();
        sibling.nonce = (candidate.nonce + 1..candidate.nonce + 100000)
            .find(|nonce| {
                sibling.nonce = *nonce;
                sibling.has_valid_pow()
                    && lattice_block_tree::fork_choice_prefers_block(
                        &content_id(&sibling).unwrap(),
                        &cid,
                    )
            })
            .unwrap();
        let (release, gate) = std::sync::mpsc::channel();
        let watched = cid.clone();
        let reply = inspect(&running.handle, move |host| {
            let task = prepare(host, &candidate);
            host.preflights.spawn(move || {
                gate.recv().unwrap();
                task.run().map(preflight::Completion::Connection)
            });
            let volumes = lattice_cashew::volume::serialize_recursive(
                &Volume::new(sibling.clone()).unwrap(),
                &(),
            )
            .unwrap();
            host.storage
                .cache_volumes("test-side-sibling", &volumes)
                .unwrap();
            host.transient.push("test-side-sibling".into());
            host.chain
                .admit_header(&sibling, sibling.children.node.as_deref().unwrap(), now())
                .unwrap();
            let other = host
                .chain
                .tree()
                .connect_job(&content_id(&sibling).unwrap(), None)
                .unwrap();
            let RootConnectionExecution::Accepted(verified) = host
                .chain
                .tree()
                .execute_root_connection(&other, host.storage.as_ref(), now())
                .unwrap()
            else {
                panic!("valid sibling")
            };
            host.chain.connected_issued_root(&verified, now()).unwrap();
            assert!(!host.chain.tree().is_executed(&watched));
            assert!(!crate::core::body::fork_children(host.chain.tree()).contains(&watched));
            let id = host.id().unwrap();
            let (tx, rx) = oneshot::channel();
            host.replies.insert(id, tx);
            host.mined_replies.push(MinedReply {
                id,
                cid: watched,
                children: vec![],
            });
            rx
        })
        .await;
        let (code, response) = timeout(Duration::from_secs(5), reply)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(response["accepted"], true);
        assert_eq!(response["disposition"], "acceptedSide");
        let pending = cid.clone();
        inspect(&running.handle, move |host| {
            assert!(!host.chain.tree().is_executed(&pending));
            assert!(host.active_connections.contains(&pending));
        })
        .await;
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), async {
            while !inspect(&running.handle, {
                let cid = cid.clone();
                move |host| host.chain.tree().is_executed(&cid)
            })
            .await
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completed_root_result_preserves_unchanged_state_across_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let running = start_with_retention(
            config(dir.path()),
            crate::storage::RetentionPolicy {
                state: crate::storage::StateRetention::Tip,
                bodies: crate::storage::BodyRetention::None,
            },
        )
        .await
        .unwrap();
        let owner = config(dir.path()).genesis_cid;
        let request = json!({"recipients":[{"chainPath":["Nexus"],"address":owner}]});
        let (_, first) = running
            .handle
            .request("/mining/templates", request.clone())
            .await
            .unwrap();
        let id = first["workID"].as_str().unwrap().to_owned();
        let mut parent = inspect(&running.handle, move |host| {
            host.mining
                .state()
                .templates()
                .template(&id)
                .unwrap()
                .block
                .clone()
        })
        .await;
        parent.nonce = (0..10000)
            .find(|nonce| {
                parent.nonce = *nonce;
                parent.has_valid_pow()
            })
            .unwrap();
        let parent_state = parent.post_state.0.node.as_deref().unwrap();
        let reused = parent_state.account_state.0.raw_cid.clone();
        let (_, mined) = running
            .handle
            .request(
                "/mining/work",
                json!({"workID":first["workID"],"nonce":parent.nonce}),
            )
            .await
            .unwrap();
        assert_eq!(mined["accepted"], true);
        let (_, candidate) = block(&running.handle).await; // no reward: account subtree unchanged
        let candidate_cid = content_id(&candidate).unwrap();
        let (_, alternate) = running
            .handle
            .request("/mining/templates", request)
            .await
            .unwrap();
        let id = alternate["workID"].as_str().unwrap().to_owned();
        let mut sibling = inspect(&running.handle, move |host| {
            host.mining
                .state()
                .templates()
                .template(&id)
                .unwrap()
                .block
                .clone()
        })
        .await;
        sibling.nonce = (0..100000)
            .find(|nonce| {
                sibling.nonce = *nonce;
                sibling.has_valid_pow()
                    && lattice_block_tree::fork_choice_prefers_block(
                        &candidate_cid,
                        &content_id(&sibling).unwrap(),
                    )
            })
            .unwrap();
        let unbundled = reused.clone();
        let task = inspect(&running.handle, move |host| {
            let task = prepare(host, &candidate);
            // A body bundle need not repeat an unchanged state Volume that was
            // locally held by the executed predecessor when the job started.
            let mut body = lattice_cashew::volume::serialize_recursive(
                &Volume::new(candidate.clone()).unwrap(),
                &(),
            )
            .unwrap();
            body.retain(|volume| volume.root != unbundled);
            host.storage
                .replace_cached_volumes(
                    &format!("test-connect-{}", content_id(&candidate).unwrap()),
                    &body,
                )
                .unwrap();
            task
        })
        .await;
        // Pause between pure validation and dependency preservation to also
        // exercise restoration when a sweep wins before the worker pins bytes.
        let (verdict, content) = tokio::task::spawn_blocking(move || {
            let verdict = task
                .task
                .run(&SnapshotSource(task.content.snapshot()), now())
                .unwrap();
            (verdict, task.content)
        })
        .await
        .unwrap();
        // Withhold its sealed verdict while another branch replaces the old
        // account state. The WAL snapshot alone is not a live CAS pin.
        let old = reused.clone();
        inspect(&running.handle, move |host| {
            let volumes = lattice_cashew::volume::serialize_recursive(
                &Volume::new(sibling.clone()).unwrap(),
                &(),
            )
            .unwrap();
            host.storage
                .cache_volumes("replacement-state", &volumes)
                .unwrap();
            host.chain
                .admit_header(&sibling, sibling.children.node.as_deref().unwrap(), now())
                .unwrap();
            let job = host
                .chain
                .tree()
                .connect_job(&content_id(&sibling).unwrap(), None)
                .unwrap();
            let RootConnectionExecution::Accepted(verified) = host
                .chain
                .tree()
                .execute_root_connection(&job, host.storage.as_ref(), now())
                .unwrap()
            else {
                panic!("valid sibling")
            };
            host.chain.connected_issued_root(&verified, now()).unwrap();
            host.storage
                .release_cached_volumes("replacement-state")
                .unwrap();
            // Issued template leases are independently valid owners now.
            // Expire setup templates through the real book, then prune their
            // leases so this fixture isolates the completed root result.
            for work_id in host.template_content.keys().cloned().collect::<Vec<_>>() {
                host.mining
                    .step(
                        MiningEvent::SubmitWork {
                            reply_id: u64::MAX,
                            work_id,
                            nonce: 0,
                        },
                        i64::MAX,
                    )
                    .unwrap();
            }
            host.cancel_abandoned_templates();
            host.maintain_retention();
            host.storage.sweep().unwrap();
            assert!(
                host.storage.fetch_volume(&old).is_none(),
                "fixture must actually sweep the unchanged old subtree"
            );
        })
        .await;
        let completion = tokio::task::spawn_blocking(move || {
            content
                .preserve(verdict.accepted_header().unwrap())
                .unwrap();
            ConnectionCompletion { verdict, content }
        })
        .await
        .unwrap();
        let pinned = reused.clone();
        inspect(&running.handle, move |host| {
            host.storage.sweep().unwrap();
            assert!(
                host.storage.fetch_volume(&pinned).is_some(),
                "completed result owns restored data before admission"
            );
        })
        .await;
        inspect(&running.handle, move |host| {
            host.complete_connection(completion).unwrap();
            assert!(host.chain.tree().is_executed(&candidate_cid));
            assert_eq!(
                host.chain.tree().heaviest_executed_tip(&BTreeSet::new()),
                candidate_cid
            );
            host.maintain_retention();
            host.completed_connection_content.clear();
            host.storage.sweep().unwrap();
            assert!(
                host.storage.fetch_volume(&reused).is_some(),
                "accepted result must preserve reused state bytes after worker leases close"
            );
        })
        .await;
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
}

#[cfg(test)]
#[path = "connections_acquisition_tests.rs"]
mod acquisition_tests;
