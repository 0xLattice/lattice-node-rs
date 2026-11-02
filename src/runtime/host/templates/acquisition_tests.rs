use super::tests::{fixture_row_with_retention, inspect};
use super::*;

/// The host has an executed basis, but the worker snapshot has no bytes at all.
/// Real authenticated Ivy reads must supply previous state, spec and body.
#[tokio::test(flavor = "current_thread")]
async fn template_acquires_remote_previous_state_and_preserves_it_before_budget_release() {
    scenario(9).await;
}
#[tokio::test(flavor = "current_thread")]
async fn template_acquires_remote_policy_module_and_retains_it_after_cache_eviction() {
    scenario(27).await;
}
async fn scenario(index: usize) {
    let (_remote_dir, remote) =
        fixture_row_with_retention(index, crate::storage::RetentionPolicy::default()).await;
    let local_dir = tempfile::tempdir().unwrap();
    let (genesis, volumes) = inspect(&remote.handle, |host| {
        let genesis = host.genesis.clone();
        let block: Volume<Block> = Volume(Header::from_cid(&genesis).unwrap());
        let verified =
            lattice_validation::validate_root_genesis(&block, host.storage.as_ref(), i64::MAX)
                .unwrap();
        let mut volumes =
            lattice_cashew::volume::serialize_recursive(verified.header(), &()).unwrap();
        for policy in &verified.spec().0.node.as_deref().unwrap().wasm_policies {
            volumes.push(host.storage.fetch_volume(&policy.module_cid).unwrap());
        }
        (genesis, volumes)
    })
    .await;
    let policy_volumes: Vec<_> = volumes
        .iter()
        .filter(|v| {
            v.entries.values().any(|b| {
                lattice_cashew::cbor::decode::<lattice_primitives::WasmPolicyModule>(b).is_ok()
            })
        })
        .cloned()
        .collect();
    let local = start(HostConfig {
        directory: local_dir.path().into(),
        genesis_cid: genesis.clone(),
        bootstrap_volumes: volumes,
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[114; 32]),
        listen: "127.0.0.1:0".parse().unwrap(),
        min_relay_fee: 1,
        seeds: vec![ivy_core::PeerEndpoint {
            public_key: remote.public_key.clone(),
            host: "127.0.0.1".into(),
            port: remote.overlay_addr.port(),
        }],
    })
    .await
    .unwrap();
    if !policy_volumes.is_empty() {
        inspect(&local.handle, move |h| {
            h.storage
                .cache_volumes("fixture-policy", &policy_volumes)
                .unwrap()
        })
        .await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !inspect(&local.handle, |host| !host.network.peers().is_empty()).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let empty_dir = tempfile::tempdir().unwrap();
    let empty = Arc::new(NodeStorage::open(empty_dir.path(), genesis, "remote-worker").unwrap());
    let target = empty.clone();
    let (task,reply)=inspect(&local.handle,move|host| {
        let(tx,rx)=oneshot::channel();host.api("/mining/templates".into(),json!({"recipients":[{"chainPath":["Nexus"],"address":lattice_cashew::cid_for_bytes(0x71,b"lazy-worker-reward")} ]}),tx).unwrap();
        let index=host.queue.iter().position(|w|matches!(w,Work::Mining(e) if matches!(e.as_ref(),MiningEffect::BuildTemplate(_)))).unwrap();
        let Work::Mining(effect)=host.queue.remove(index).unwrap() else {unreachable!()};
        let MiningEffect::BuildTemplate(job)=*effect else {unreachable!()};
        let mut task=host.prepare_template(job).unwrap().unwrap();
        task.content=crate::storage::ConnectionContent::new(target.preflight_snapshot().unwrap(),target,"remote-template",vec![]);
        (task,rx)
    }).await;
    let done = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::task::spawn_blocking(move || task.run()),
    )
    .await
    .unwrap()
    .unwrap();
    done.preserved.as_ref().unwrap();
    let output = done.output.as_ref().unwrap();
    let state = output.build.block.post_state.raw_cid.clone();
    inspect(&local.handle, |host| {
        *host.content_cache.lock().unwrap() =
            super::super::super::content_cache::ContentCache::new(0)
    })
    .await;
    empty.sweep().unwrap();
    if index == 27 {
        for policy in &output
            .build
            .block
            .spec
            .node
            .as_deref()
            .unwrap()
            .wasm_policies
        {
            assert!(
                empty.fetch_volume(&policy.module_cid).is_some(),
                "remote-only policy must survive completed session/cache eviction"
            );
        }
    }
    assert!(
        empty.fetch_volume(&state).is_some(),
        "acquired output preserved before remote session drop"
    );
    assert_eq!(
        local.handle.request("/status", json!({})).await.unwrap().0,
        200
    );
    inspect(&local.handle, move |host| {
        host.complete_template(done).unwrap()
    })
    .await;
    assert_eq!(reply.await.unwrap().0, 200);
    local.handle.shutdown().await;
    local.task.await.unwrap().unwrap();
    remote.handle.shutdown().await;
    remote.task.await.unwrap().unwrap();
}
