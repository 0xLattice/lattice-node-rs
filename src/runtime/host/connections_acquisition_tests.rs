use super::tests::{block, config, inspect, prepare};
use super::*;
#[tokio::test(flavor = "current_thread")]
async fn root_execution_lazily_acquires_body_and_dependencies_over_authenticated_peer() {
    let remote_dir = tempfile::tempdir().unwrap();
    let local_dir = tempfile::tempdir().unwrap();
    let empty_dir = tempfile::tempdir().unwrap();
    let remote = start(config(remote_dir.path())).await.unwrap();
    let mut options = config(local_dir.path());
    options.signing_key = ed25519_dalek::SigningKey::from_bytes(&[115; 32]);
    options.seeds = vec![ivy_core::PeerEndpoint {
        public_key: remote.public_key.clone(),
        host: "127.0.0.1".into(),
        port: remote.overlay_addr.port(),
    }];
    let local = start(options).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !inspect(&local.handle, |host| !host.network.peers().is_empty()).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, candidate) = block(&local.handle).await;
    let cid = content_id(&candidate).unwrap();
    let volumes =
        lattice_cashew::volume::serialize_recursive(&Volume::new(candidate.clone()).unwrap(), &())
            .unwrap();
    inspect(&remote.handle, move |host| {
        host.storage
            .cache_volumes("remote-candidate", &volumes)
            .unwrap()
    })
    .await;
    let empty = Arc::new(NodeStorage::open(empty_dir.path(), "test", "remote-execution").unwrap());
    let target = empty.clone();
    let task = inspect(&local.handle, move |host| {
        let mut task = prepare(host, &candidate);
        task.content = crate::storage::ConnectionContent::new(
            target.preflight_snapshot().unwrap(),
            target,
            "remote-root",
            vec![],
        );
        task
    })
    .await;
    let done = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::task::spawn_blocking(move || task.run()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(done.verdict.accepted_header().is_some());
    empty.sweep().unwrap();
    assert!(empty.fetch_volume(&cid).is_some());
    inspect(&local.handle, move |host| {
        host.complete_connection(done).unwrap()
    })
    .await;
    assert!(
        inspect(&local.handle, move |host| host
            .chain
            .tree()
            .is_executed(&cid))
        .await
    );
    local.handle.shutdown().await;
    local.task.await.unwrap().unwrap();
    remote.handle.shutdown().await;
    remote.task.await.unwrap().unwrap();
}
