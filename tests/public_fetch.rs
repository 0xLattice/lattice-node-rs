use lattice_node::runtime::{api::VolumeFuture, public_fetch::*};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;
struct Fetcher {
    calls: AtomicUsize,
    gate: Arc<Semaphore>,
}
impl ArchiveFetcher for Fetcher {
    fn fetch(&self, cid: String) -> VolumeFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.clone();
        Box::pin(async move {
            gate.acquire().await.unwrap().forget();
            match cid.as_str() {
                "missing" => Ok(None),
                "busy" => Err(anyhow::anyhow!("busy")),
                _ => Ok(Some(vec![1, 2, 3])),
            }
        })
    }
}
#[tokio::test]
async fn same_cid_shares_buffer_cancellation_preserves_load_and_capacity_recovers() {
    let source = Arc::new(Fetcher {
        calls: AtomicUsize::new(0),
        gate: Arc::new(Semaphore::new(0)),
    });
    let fetch = PublicVolumeFetches::new(4, 128 << 20, source.clone());
    let first = fetch.clone();
    let first = tokio::spawn(async move { first.archive("a").await });
    while source.calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fetch.archive("b").await, FetchAnswer::Busy);
    use std::future::Future;
    let mut second = std::pin::pin!(fetch.archive("a"));
    let mut third = std::pin::pin!(fetch.archive("a"));
    std::future::poll_fn(|cx| {
        assert!(second.as_mut().poll(cx).is_pending());
        assert!(third.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    first.abort();
    let _ = first.await;
    source.gate.add_permits(1);
    let FetchAnswer::Archive(a) = second.await else {
        panic!()
    };
    let FetchAnswer::Archive(b) = third.await else {
        panic!()
    };
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    for (cid, expected) in [
        ("missing", FetchAnswer::NotServed),
        ("busy", FetchAnswer::Busy),
    ] {
        source.gate.add_permits(1);
        assert_eq!(fetch.archive(cid).await, expected);
    }
    source.gate.add_permits(1);
    assert!(matches!(fetch.archive("a").await, FetchAnswer::Archive(_)));
    assert_eq!(source.calls.load(Ordering::SeqCst), 4); // No completed cache.
}
#[test]
fn public_capacity_reserves_half_content_budget() {
    for (requested, budget, expected) in [
        (0, 128 << 20, 0),
        (4, 128 << 20, 1),
        (4, 512 << 20, 4),
        (4, 0, 1),
        (1, 1024 << 20, 1),
    ] {
        assert_eq!(
            PublicVolumeFetches::effective_limit(requested, budget),
            expected
        );
    }
}

#[test]
fn rpc_archive_is_bound_to_requested_root_and_actual_bytes() {
    use base64::Engine;
    let volume = lattice_cashew::Volume::new("fixture".to_owned()).unwrap();
    let serialized = lattice_cashew::volume::serialize_boundary(&volume, &()).unwrap();
    let entries: std::collections::BTreeMap<_, _> = serialized
        .entries
        .iter()
        .map(|(cid, bytes)| {
            (
                cid.clone(),
                base64::engine::general_purpose::STANDARD.encode(bytes),
            )
        })
        .collect();
    let value = serde_json::json!({"cid":serialized.root,"entries":entries});
    let archive = decode_rpc_archive(&serialized.root, &value).unwrap();
    assert_eq!(
        ivy_core::Volume::decode(&archive, &serialized.root, serialized.entries.len() as u16)
            .unwrap()
            .root_cid,
        serialized.root
    );
    assert!(decode_rpc_archive("different", &value).is_none());
    let mut corrupted = value.clone();
    corrupted["entries"][&serialized.root] =
        serde_json::json!(base64::engine::general_purpose::STANDARD.encode(b"forged"));
    assert!(decode_rpc_archive(&serialized.root, &corrupted).is_none());
}
