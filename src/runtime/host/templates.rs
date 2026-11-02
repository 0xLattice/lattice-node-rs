//! Assembly runs off the actor; only an unchanged whole-host basis may apply it.
use super::*;
use crate::runtime::merged_template::{ChildTemplateContext, MergedTemplateBuildOutput};
use crate::runtime::template::TemplateBuildError;

#[derive(Clone, Debug, PartialEq, Eq)]
struct LevelFence {
    revision: u64,
    executed: String,
    header: String,
    mining: Option<(u64, String, u64)>,
    configuration: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fence {
    root_revision: u64,
    root_epoch: u64,
    root_tip: String,
    root_pool: u64,
    children: BTreeMap<Vec<String>, LevelFence>,
}
pub(super) struct TemplateTask {
    session: acquire::Session,
    job: TemplateJob,
    fence: Fence,
    anchor: Option<lattice_validation::DifficultyAnchor>,
    contexts: BTreeMap<Vec<String>, ChildTemplateContext>,
    content: crate::storage::ConnectionContent,
}
pub(super) struct TemplateCompletion {
    job: TemplateJob,
    fence: Fence,
    output: Result<MergedTemplateBuildOutput, TemplateBuildError>,
    content: Arc<crate::storage::ConnectionContent>,
    preserved: Result<(), crate::storage::StoreError>,
}
impl TemplateTask {
    pub(super) fn run(self) -> TemplateCompletion {
        let output = super::super::merged_template::build_merged_template(
            &self.job,
            &acquire::Source {
                snapshot: self.content.snapshot(),
                session: &self.session,
            },
            now(),
            self.anchor,
            &self.contexts,
        );
        let preserved = self.session.check().and_then(|()| match &output {
            Ok(output) => Volume::new(output.build.block.clone())
                .map_err(crate::storage::StoreError::from)
                .and_then(|header| {
                    self.content.preserve_acquired(
                        &header,
                        &self.session.volumes(),
                        &self.session.entries(),
                    )
                }),
            Err(_) => Ok(()),
        });
        TemplateCompletion {
            content: Arc::new(self.content),
            preserved,
            job: self.job,
            fence: self.fence,
            output,
        }
    }
}
impl Host {
    fn template_fence(&self) -> anyhow::Result<Fence> {
        let mut children = BTreeMap::new();
        for path in self.levels.ordered() {
            let tree = self.levels.get(&path).unwrap().tree();
            let settings = self.child_settings.get(&path);
            // Peer addresses do not change assembly. Everything that does is
            // compared, including not-yet-executed child genesis configuration.
            let configuration = content_id(&(
                settings.and_then(|s| s.spec.as_ref()),
                settings.and_then(|s| s.premine_recipient.as_ref()),
                settings.map(|s| &s.genesis_transactions),
            ))?;
            children.insert(
                path.clone(),
                LevelFence {
                    revision: tree.current_revision(),
                    executed: tree.heaviest_executed_tip(&BTreeSet::new()),
                    header: tree.canonical_tip().into(),
                    mining: self.child_mining.get(&path).map(|m| {
                        (
                            m.core.state().tip_epoch(),
                            m.core.state().tip_cid().into(),
                            m.core.state().mempool().version(),
                        )
                    }),
                    configuration,
                },
            );
        }
        Ok(Fence {
            root_revision: self.chain.tree().current_revision(),
            root_epoch: self.mining.state().tip_epoch(),
            root_tip: self.mining.state().tip_cid().into(),
            root_pool: self.mining.state().mempool().version(),
            children,
        })
    }
    pub(super) fn prepare_template(
        &mut self,
        job: TemplateJob,
    ) -> anyhow::Result<Option<TemplateTask>> {
        if !self.mining.state().template_job_pending(&job) {
            return Ok(None);
        }
        if job.pool_version != self.mining.state().mempool().version() {
            self.event(MiningEvent::TemplateContextChanged(Box::new(job)))?;
            return Ok(None);
        }
        let fence = self.template_fence()?;
        let contexts = self.child_templates(&job.tip_cid)?;
        let anchor = self.chain.tree().difficulty_anchor(&job.tip_cid).map(|a| {
            lattice_validation::DifficultyAnchor {
                block_height: a.block_height,
                timestamp: a.timestamp,
                target: UInt256(a.target),
            }
        });
        let content = crate::storage::ConnectionContent::new(
            self.storage.preflight_snapshot()?,
            self.storage.clone(),
            &format!("template-{}", job.id),
            vec![],
        );
        let session = acquire::Session::new(
            self.acquisition_requests.clone(),
            self.content_cache.clone(),
        );
        self.template_acquisitions
            .insert(job.id, (job.clone(), Arc::downgrade(&session.cancelled)));
        Ok(Some(TemplateTask {
            session,
            content,
            job,
            fence,
            contexts,
            anchor,
        }))
    }
    pub(super) fn complete_template(&mut self, done: TemplateCompletion) -> anyhow::Result<()> {
        done.preserved?;
        self.cancel_abandoned_templates();
        if !self.mining.state().template_job_pending(&done.job) {
            return Ok(());
        }
        if self.template_fence()? != done.fence {
            return self.event(MiningEvent::TemplateContextChanged(Box::new(done.job)));
        }
        match done.output {
            Ok(output) => {
                // No policy verdict from an old child, pool or configuration
                // may remove transactions in the current host.
                self.child_policy_refused(&output.refused)?;
                self.event(MiningEvent::PolicyRefused {
                    tip_cid: done.job.tip_cid.clone(),
                    cids: output
                        .refused
                        .get(&vec!["Nexus".into()])
                        .cloned()
                        .unwrap_or_default(),
                })?;
                self.template_content
                    .insert(output.build.work_id.clone(), done.content);
                self.event(MiningEvent::TemplateBuilt(
                    Box::new(done.job),
                    Some(Box::new(output.build)),
                ))
            }
            Err(_) => self.event(MiningEvent::TemplateBuilt(Box::new(done.job), None)),
        }
    }
    pub(super) fn cancel_abandoned_templates(&mut self) {
        let closed: BTreeSet<_> = self
            .replies
            .iter()
            .filter_map(|(id, reply)| reply.is_closed().then_some(*id))
            .collect();
        self.mining.cancel_template_requests(&closed);
        self.template_acquisitions.retain(|_, (job, cancel)| {
            let Some(cancel) = cancel.upgrade() else {
                return false;
            };
            if !self.mining.state().template_job_pending(job) {
                cancel.store(true, std::sync::atomic::Ordering::Release);
                return false;
            }
            true
        });
        let time = now();
        self.template_content.retain(|id, _| {
            self.mining
                .state()
                .templates()
                .template(id)
                .is_some_and(|work| time < work.expires_at)
        });
        // Fetch cancellation has its own network/lease cleanup; do not consume
        // those IDs before it runs at the top of the actor iteration.
        for id in closed {
            if !self.fetches.contains_key(&id) {
                self.replies.remove(&id);
                self.child_build_replies.remove(&id);
                // A submitted nonce has already queued Mined/WorkRefused.
                // Its lease belongs to that effect, independently of HTTP cancellation.
            }
        }
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
    async fn fixture() -> (tempfile::TempDir, RunningHost) {
        fixture_row(0).await
    }
    async fn fixture_row(index: usize) -> (tempfile::TempDir, RunningHost) {
        fixture_row_with_retention(index, crate::storage::RetentionPolicy::default()).await
    }
    pub(super) async fn fixture_row_with_retention(
        index: usize,
        retention: crate::storage::RetentionPolicy,
    ) -> (tempfile::TempDir, RunningHost) {
        let rows: Value = serde_json::from_str(include_str!(
            "../../../../lattice-rs/crates/lattice-validation/tests/genesis-swift.json"
        ))
        .unwrap();
        let entries: BTreeMap<String, Vec<u8>> =
            serde_json::from_value(rows[index]["entries"].clone()).unwrap();
        let cid = rows[index]["cid"].as_str().unwrap().to_owned();
        let verified = lattice_validation::validate_root_genesis(
            &Volume(Header::from_cid(&cid).unwrap()),
            &entries,
            i64::MAX,
        )
        .unwrap();
        let mut bootstrap =
            lattice_cashew::volume::serialize_recursive(verified.header(), &()).unwrap();
        for policy in &verified.spec().0.node.as_deref().unwrap().wasm_policies {
            if let Some(bytes) = entries.get(&policy.module_cid) {
                bootstrap.push(SerializedVolume::new(
                    policy.module_cid.clone(),
                    BTreeMap::from([(policy.module_cid.clone(), bytes.clone())]),
                ));
            }
        }
        let policy_volumes: Vec<_> = bootstrap
            .iter()
            .filter(|v| {
                verified
                    .spec()
                    .0
                    .node
                    .as_deref()
                    .unwrap()
                    .wasm_policies
                    .iter()
                    .any(|p| p.module_cid == v.root)
            })
            .cloned()
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let host = start_with_retention(
            HostConfig {
                directory: dir.path().into(),
                genesis_cid: cid,
                bootstrap_volumes: bootstrap,
                signing_key: ed25519_dalek::SigningKey::from_bytes(&[73; 32]),
                listen: "127.0.0.1:0".parse().unwrap(),
                min_relay_fee: 1,
                seeds: vec![],
            },
            retention,
        )
        .await
        .unwrap();
        if !policy_volumes.is_empty() {
            inspect(&host.handle, move |h| {
                h.storage
                    .cache_volumes("fixture-policy", &policy_volumes)
                    .unwrap()
            })
            .await;
        }
        (dir, host)
    }
    #[tokio::test(flavor = "current_thread")]
    async fn actor_serves_commands_and_retries_all_waiters_after_child_only_change() {
        let (_dir, running) = fixture().await;
        let handle = &running.handle;
        let (release, gate) = std::sync::mpsc::channel();
        let (started, began) = oneshot::channel();
        let (first, second, original_tip) = inspect(handle, move |host| {
            let (first_tx, first_rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), first_tx).unwrap();
            let index = host.queue.iter().position(|work| matches!(work, Work::Mining(e) if matches!(e.as_ref(), MiningEffect::BuildTemplate(_)))).unwrap();
            let Work::Mining(effect) = host.queue.remove(index).unwrap() else { unreachable!() };
            let MiningEffect::BuildTemplate(job) = *effect else { unreachable!() };
            let task = host.prepare_template(job).unwrap().unwrap();
            host.preflights.spawn(move || {
                let mut done = task.run();
                done.output.as_mut().unwrap().build.work_id = "stale-child-basis".into();
                let _ = started.send(());
                gate.recv().unwrap();
                Ok(preflight::Completion::Template(Box::new(done)))
            });
            let (second_tx, second_rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), second_tx).unwrap();
            assert_eq!(host.mining.state().waiting_template_requests(), 2);
            (first_rx, second_rx, host.mining.state().tip_cid().to_owned())
        }).await;
        timeout(Duration::from_secs(5), began)
            .await
            .unwrap()
            .unwrap();
        // These requests go through the actual actor, not its health snapshot.
        let status = timeout(Duration::from_secs(2), handle.request("/status", json!({})))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.0, 200);
        assert_eq!(status.1["tipCID"], original_tip);
        assert_eq!(
            handle
                .request("POST /hosted-chains", json!({"hostChain":["Nexus/Alpha"]}))
                .await
                .unwrap()
                .0,
            200
        );
        release.send(()).unwrap();
        for result in [first, second] {
            let (code, body) = timeout(Duration::from_secs(5), result)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(code, 200, "{body}");
            assert_ne!(body["workID"], "stale-child-basis");
        }
        inspect(handle, |host| {
            assert_eq!(host.mining.state().waiting_template_requests(), 0)
        })
        .await;
        handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_queued_template_releases_waiter_without_starting_worker() {
        let (_dir, running) = fixture().await;
        inspect(&running.handle, |host| {
            let (reply, rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), reply)
                .unwrap();
            assert_eq!(host.mining.state().waiting_template_requests(), 1);
            drop(rx);
            host.cancel_abandoned_templates();
            assert_eq!(host.mining.state().waiting_template_requests(), 0);
            for work in std::mem::take(&mut host.queue) {
                if let Work::Mining(effect) = work
                    && let MiningEffect::BuildTemplate(job) = *effect
                {
                    assert!(host.prepare_template(job).unwrap().is_none());
                }
            }
            assert!(!host.preflights.active());
        })
        .await;
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_running_template_finishes_without_issuing_work() {
        let (_dir, running) = fixture().await;
        let (release, gate) = std::sync::mpsc::channel();
        let reply = inspect(&running.handle, move |host| {
            let (tx, rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), tx).unwrap();
            let index = host.queue.iter().position(|work| matches!(work, Work::Mining(e) if matches!(e.as_ref(), MiningEffect::BuildTemplate(_)))).unwrap();
            let Work::Mining(effect) = host.queue.remove(index).unwrap() else { unreachable!() };
            let MiningEffect::BuildTemplate(job) = *effect else { unreachable!() };
            let task = host.prepare_template(job).unwrap().unwrap();
            host.preflights.spawn(move || {
                gate.recv().unwrap();
                Ok(preflight::Completion::Template(Box::new(task.run())))
            });
            rx
        }).await;
        drop(reply);
        inspect(&running.handle, |host| {
            host.cancel_abandoned_templates();
            assert_eq!(host.mining.state().waiting_template_requests(), 0);
            assert!(host.preflights.active());
        })
        .await;
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), async {
            while inspect(&running.handle, |host| host.preflights.active()).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        inspect(&running.handle, |host| {
            assert_eq!(host.mining.state().templates().count(), 0)
        })
        .await;
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stale_policy_refusals_cannot_remove_current_pool_transactions() {
        let (_dir, running) = fixture().await;
        let (task, cid, reply) = inspect(&running.handle, |host| {
            // Seed a reducer-only pool fixture. This test checks completion
            // authority, not transaction validation; the worker still executes
            // the real assembler against the host's actual content snapshot.
            let transaction = Transaction::new(BTreeMap::new(), Header::new(lattice_primitives::TransactionBody {
                account_actions: vec![lattice_primitives::AccountAction { owner: "fixture".into(), delta: -10 }],
                actions: vec![], deposit_actions: vec![], receipt_actions: vec![], withdrawal_actions: vec![],
                signers: vec!["fixture".into()], nonce: 0, chain_path: vec!["Nexus".into()],
            }).unwrap());
            let cid = content_id(&transaction).unwrap();
            let effects = host.mining.step(MiningEvent::TransactionReceived(transaction, TransactionOrigin::Returned), now()).unwrap();
            let job = effects.into_iter().find_map(|effect| match effect { MiningEffect::Preflight(job)=>Some(job), _=>None }).unwrap();
            host.mining.step(MiningEvent::Preflighted(job, crate::core::mempool::MempoolDisposition::Ready, BTreeMap::from([("fixture".into(),100)])), now()).unwrap();
            assert_eq!(host.mining.state().mempool().count(), 1);
            let (tx, rx) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), tx).unwrap();
            let index = host.queue.iter().position(|work| matches!(work, Work::Mining(e) if matches!(e.as_ref(), MiningEffect::BuildTemplate(_)))).unwrap();
            let Work::Mining(effect) = host.queue.remove(index).unwrap() else { unreachable!() };
            let MiningEffect::BuildTemplate(job) = *effect else { unreachable!() };
            (host.prepare_template(job).unwrap().unwrap(), cid, rx)
        }).await;
        let mut completion = tokio::task::spawn_blocking(move || task.run())
            .await
            .unwrap();
        completion
            .output
            .as_mut()
            .unwrap()
            .refused
            .insert(vec!["Nexus".into()], vec![cid.clone()]);
        assert_eq!(
            running
                .handle
                .request("POST /hosted-chains", json!({"hostChain":["Nexus/Alpha"]}))
                .await
                .unwrap()
                .0,
            200
        );
        inspect(&running.handle, move |host| {
            host.complete_template(completion).unwrap();
            // Inspect before the queued fresh assembly can run. An old verdict
            // must not mutate this pool, even though it names a real live CID.
            assert_eq!(host.mining.state().mempool().count(), 1);
            assert!(
                host.mining
                    .state()
                    .mempool()
                    .items()
                    .iter()
                    .any(|item| item.cid == cid)
            );
            assert_eq!(host.mining.state().waiting_template_requests(), 1);
        })
        .await;
        assert_eq!(
            timeout(Duration::from_secs(5), reply)
                .await
                .unwrap()
                .unwrap()
                .0,
            200
        );
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn coalesced_runtime_requests_cannot_exceed_configured_waiter_cap() {
        let (_dir, running) = fixture().await;
        inspect(&running.handle, |host| {
            let mut waiters = vec![];
            for _ in 0..64 {
                let (tx, rx) = oneshot::channel();
                host.api("/mining/templates".into(), json!({"recipients":[]}), tx).unwrap();
                waiters.push(rx);
            }
            assert_eq!(host.mining.state().waiting_template_requests(), 64);
            let (tx, mut overflow) = oneshot::channel();
            host.api("/mining/templates".into(), json!({"recipients":[]}), tx).unwrap();
            assert_eq!(overflow.try_recv().unwrap().0, 503);
            assert_eq!(host.mining.state().waiting_template_requests(), 64);
            assert_eq!(host.queue.iter().filter(|work| matches!(work, Work::Mining(effect) if matches!(effect.as_ref(), MiningEffect::BuildTemplate(_)))).count(), 1);
            drop(waiters);
            host.cancel_abandoned_templates();
            assert_eq!(host.mining.state().waiting_template_requests(), 0);
        }).await;
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    async fn host_author_child(dir: &std::path::Path, handle: &HostHandle) {
        let spec = dir.join("author-spec.json");
        std::fs::write(
            &spec,
            serde_json::to_vec(&json!({
                "maxNumberOfTransactionsPerBlock":1000,"maxStateGrowth":100000,
                "maxBlockSize":1000000,"premine":1,"targetBlockTime":1000,
                "initialReward":10,"halvingInterval":1000,"halfLife":120,"wasmPolicies":[]
            }))
            .unwrap(),
        )
        .unwrap();
        let owner = inspect(handle, |host| host.genesis.clone()).await;
        let (code, body) = handle
            .request(
                "POST /hosted-chains",
                json!({
                    "hostChain":[format!("Nexus/Alpha={}", spec.display())],
                    "premineTo":[format!("Nexus/Alpha={owner}")]
                }),
            )
            .await
            .unwrap();
        assert_eq!(code, 200, "{body}");
    }
    #[tokio::test(flavor = "current_thread")]
    async fn child_author_uses_fenced_worker_and_actor_stays_responsive() {
        let (dir, running) = fixture_row(9).await;
        let handle = &running.handle;
        host_author_child(dir.path(), handle).await;
        let (release, gate) = std::sync::mpsc::channel();
        let (started, began) = oneshot::channel();
        let reply = inspect(handle, move |host| {
            let (tx, rx) = oneshot::channel();
            host.api("/child-blocks/build".into(), json!({"chainPath":["Nexus","Alpha"]}), tx).unwrap();
            assert_eq!(host.child_build_replies.len(), 1);
            let index = host.queue.iter().position(|work| matches!(work, Work::Mining(e) if matches!(e.as_ref(), MiningEffect::BuildTemplate(_)))).unwrap();
            let Work::Mining(effect) = host.queue.remove(index).unwrap() else { unreachable!() };
            let MiningEffect::BuildTemplate(job) = *effect else { unreachable!() };
            let task = host.prepare_template(job).unwrap().unwrap();
            host.preflights.spawn(move || {
                let done = task.run();
                assert!(done.output.as_ref().unwrap().build.block.children.node.as_ref().unwrap().get("Alpha").is_some());
                let _ = started.send(());
                gate.recv().unwrap();
                Ok(preflight::Completion::Template(Box::new(done)))
            });
            rx
        }).await;
        timeout(Duration::from_secs(5), began)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), handle.request("/status", json!({})))
                .await
                .unwrap()
                .unwrap()
                .0,
            200
        );
        assert_eq!(
            handle
                .request("POST /hosted-chains", json!({"hostChain":[]}))
                .await
                .unwrap()
                .0,
            200
        );
        release.send(()).unwrap();
        let (code, body) = timeout(Duration::from_secs(5), reply)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(code, 400, "{body}");
        assert_eq!(body["error"]["message"], "noChildCandidate");
        inspect(handle, |host| assert!(host.child_build_replies.is_empty())).await;
        handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn child_author_worker_builds_candidate_and_abandoned_request_is_cancelled() {
        let (dir, running) = fixture_row(9).await;
        let handle = &running.handle;
        host_author_child(dir.path(), handle).await;
        inspect(handle, |host| {
            let (tx, rx) = oneshot::channel();
            host.api(
                "/child-blocks/build".into(),
                json!({"chainPath":["Nexus","Alpha"]}),
                tx,
            )
            .unwrap();
            assert_eq!(host.mining.state().waiting_template_requests(), 1);
            drop(rx);
            host.cancel_abandoned_templates();
            assert_eq!(host.mining.state().waiting_template_requests(), 0);
            assert!(host.child_build_replies.is_empty());
        })
        .await;
        let (code, body) = handle
            .request(
                "/child-blocks/build",
                json!({"chainPath":["Nexus","Alpha"]}),
            )
            .await
            .unwrap();
        assert_eq!(code, 200, "{body}");
        assert!(body["submit"]["block"].as_str().is_some(), "{body}");
        assert!(body["submit"]["children"].as_str().is_some(), "{body}");
        inspect(handle, |host| {
            assert!(host.child_build_replies.is_empty());
            assert_eq!(host.mining.state().waiting_template_requests(), 0);
        })
        .await;
        handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
}

#[cfg(test)]
#[path = "templates/lifetime_tests.rs"]
mod lifetime_tests;

#[cfg(test)]
#[path = "templates/acquisition_tests.rs"]
mod acquisition_tests;
