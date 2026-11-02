use super::tests::{fixture_row_with_retention, inspect};
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn issued_template_keeps_unchanged_state_after_side_execution_and_sweep() {
    lifetime_scenario(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_submission_keeps_evicted_template_state_until_queued_connection_starts() {
    lifetime_scenario(true).await;
}

async fn lifetime_scenario(cancel_submit: bool) {
    let (_dir, running) = fixture_row_with_retention(
        0,
        crate::storage::RetentionPolicy {
            state: crate::storage::StateRetention::None,
            bodies: crate::storage::BodyRetention::None,
        },
    )
    .await;
    async fn candidate(handle: &HostHandle, owner: &str) -> (Value, Block) {
        let (code, response) = handle
            .request(
                "/mining/templates",
                json!({"recipients":[{"chainPath":["Nexus"],"address":owner}]}),
            )
            .await
            .unwrap();
        assert_eq!(code, 200, "{response}");
        let id = response["workID"].as_str().unwrap().to_owned();
        let mut block = inspect(handle, move |host| {
            host.mining
                .state()
                .templates()
                .template(&id)
                .unwrap()
                .block
                .clone()
        })
        .await;
        block.nonce = (0..100000)
            .find(|nonce| {
                block.nonce = *nonce;
                block.has_valid_pow()
            })
            .unwrap();
        (response, block)
    }
    let (first, a) = candidate(
        &running.handle,
        &lattice_cashew::cid_for_bytes(0x71, b"template-owner-a"),
    )
    .await;
    let (_, mut b) = candidate(
        &running.handle,
        &lattice_cashew::cid_for_bytes(0x71, b"template-owner-b"),
    )
    .await;
    let a_cid = content_id(&a).unwrap();
    b.nonce = (0..100000)
        .find(|nonce| {
            b.nonce = *nonce;
            b.has_valid_pow()
                && lattice_block_tree::fork_choice_prefers_block(&a_cid, &content_id(&b).unwrap())
        })
        .unwrap();
    let reused = a
        .post_state
        .node
        .as_ref()
        .unwrap()
        .account_state
        .raw_cid
        .clone();
    let (_, mined) = running
        .handle
        .request(
            "/mining/work",
            json!({"workID":first["workID"],"nonce":a.nonce}),
        )
        .await
        .unwrap();
    assert_eq!(mined["accepted"], true);
    let side = b.clone();
    inspect(&running.handle, move |host| {
        let volumes =
            lattice_cashew::volume::serialize_recursive(&Volume::new(side.clone()).unwrap(), &())
                .unwrap();
        host.storage
            .cache_volumes("template-side", &volumes)
            .unwrap();
        host.chain
            .admit_header(&side, side.children.node.as_deref().unwrap(), now())
            .unwrap();
        assert_eq!(host.chain.tree().canonical_tip(), a_cid);
    })
    .await;
    let (task, reply)=inspect(&running.handle,|host| {
        let (tx,rx)=oneshot::channel();
        host.api("/mining/templates".into(),json!({"recipients":[]}),tx).unwrap();
        let index=host.queue.iter().position(|w| matches!(w,Work::Mining(e) if matches!(e.as_ref(),MiningEffect::BuildTemplate(_)))).unwrap();
        let Work::Mining(effect)=host.queue.remove(index).unwrap() else {panic!()};
        let MiningEffect::BuildTemplate(job)=*effect else {panic!()};
        (host.prepare_template(job).unwrap().unwrap(),rx)
    }).await;
    let done = tokio::task::spawn_blocking(move || task.run())
        .await
        .unwrap();
    let expected = done.fence.clone();
    inspect(&running.handle, move |host| {
        host.complete_template(done).unwrap()
    })
    .await;
    let (code, issued) = reply.await.unwrap();
    assert_eq!(code, 200);
    let id = issued["workID"].as_str().unwrap().to_owned();
    let submitted_id = id.clone();
    let old = reused.clone();
    let retained = inspect(&running.handle, move |host| {
        let job = host
            .chain
            .tree()
            .connect_job(&content_id(&b).unwrap(), None)
            .unwrap();
        let lattice_block_tree::RootConnectionExecution::Accepted(verified) = host
            .chain
            .tree()
            .execute_root_connection(&job, host.storage.as_ref(), now())
            .unwrap()
        else {
            panic!()
        };
        let storage = host.storage.clone();
        storage
            .atomic_fact_turn(|| -> anyhow::Result<()> {
                host.chain.connected_issued_root(&verified, now())?;
                host.tip_changed()?;
                host.wake_children()?;
                Ok(())
            })
            .unwrap();
        host.storage
            .release_cached_volumes("template-side")
            .unwrap();
        assert_eq!(
            host.template_fence().unwrap(),
            expected,
            "side execution must not invalidate template basis"
        );
        assert!(host.mining.state().templates().template(&id).is_some());
        host.storage.sweep().unwrap();
        host.storage.fetch_volume(&old).is_some()
    })
    .await;
    if cancel_submit {
        let (gates, cid) = inspect(&running.handle, move |host| {
            let mut block = host
                .mining
                .state()
                .templates()
                .template(&submitted_id)
                .unwrap()
                .block
                .clone();
            block.nonce = (0..100000)
                .find(|nonce| {
                    block.nonce = *nonce;
                    block.has_valid_pow()
                })
                .unwrap();
            let cid = content_id(&block).unwrap();
            let mut gates = vec![];
            while !host.preflights.full() {
                let (tx, rx) = std::sync::mpsc::channel();
                gates.push(tx);
                host.preflights.spawn(move || {
                    rx.recv().unwrap();
                    Err(crate::storage::StoreError::Corrupt(
                        "test blocker released".into(),
                    ))
                });
            }
            let (tx, rx) = oneshot::channel();
            host.api(
                "/mining/work".into(),
                json!({"workID":submitted_id,"nonce":block.nonce}),
                tx,
            )
            .unwrap();
            assert!(
                host.mining
                    .state()
                    .templates()
                    .template(&submitted_id)
                    .is_none(),
                "valid submission evicts issued work"
            );
            drop(rx);
            host.cancel_abandoned_templates();
            (gates, cid)
        })
        .await;
        let held = inspect(&running.handle, move |host| {
            assert!(
                host.connection_pins.contains_key(&cid),
                "submitted work reached pending connection"
            );
            assert!(
                !host.active_connections.contains(&cid),
                "pool keeps connection queued"
            );
            host.storage.sweep().unwrap();
            host.storage.fetch_volume(&reused).is_some()
        })
        .await;
        for gate in gates {
            let _ = gate.send(());
        }
        assert!(running.task.await.unwrap().is_err());
        assert!(
            held,
            "cancelled submitted work lost its evicted template dependency before connection snapshot"
        );
    } else {
        running.handle.shutdown().await;
        running.task.await.unwrap().unwrap();
    }
    assert!(
        retained,
        "issued template lost unchanged state after side execution and sweep"
    );
}
