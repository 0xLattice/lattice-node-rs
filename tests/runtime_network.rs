use lattice_cashew::{SerializedVolume, cid_for_bytes};
use lattice_node::{
    runtime::{
        acquisition::{ResourcePolicy, SESSION_BYTES},
        network::{Network, NetworkEvent, OverlayConfig},
    },
    storage::NodeStorage,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn real_ivy_sessions_exchange_hello_messages_and_validated_stored_volumes() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let genesis = "bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia";
    let a_store = Arc::new(NodeStorage::open(a_dir.path(), genesis, "node").unwrap());
    let b_store = Arc::new(NodeStorage::open(b_dir.path(), genesis, "node").unwrap());
    let bytes = b"actual native TCP authenticated content".to_vec();
    let cid = cid_for_bytes(0x55, &bytes);
    let volume = SerializedVolume::new(cid.clone(), BTreeMap::from([(cid.clone(), bytes)]));
    b_store
        .cache_volumes("test", std::slice::from_ref(&volume))
        .unwrap();
    let mut a = Network::bind_with_overlay(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[11; 32]),
        genesis.into(),
        a_store,
        lattice_tally::Tally::default(),
        vec![],
        OverlayConfig {
            resources: ResourcePolicy {
                max_acquisition_bytes: SESSION_BYTES,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut b = Network::bind(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[12; 32]),
        genesis.into(),
        b_store,
    )
    .await
    .unwrap();
    let child_path = vec!["Nexus".into(), "Alpha".into()];
    b.set_hosted(std::slice::from_ref(&child_path))
        .await
        .unwrap();
    a.connect(ivy_core::PeerEndpoint {
        public_key: b.public_key.clone(),
        host: "127.0.0.1".into(),
        port: b.address.port(),
    })
    .await
    .unwrap();
    timeout(Duration::from_secs(15),async {
        let mut a_ready=false;let mut b_ready=false;
        while !(a_ready&&b_ready) {tokio::select!{
            e=a.next()=>if matches!(e.unwrap(),Some(NetworkEvent::Ready(_))){a_ready=true},
            e=b.next()=>if matches!(e.unwrap(),Some(NetworkEvent::Ready(_))){b_ready=true},
        }}
        let peer=a.peers().remove(0);
        assert_eq!(a.peers_hosting(&child_path),vec![peer.clone()]);
        b.set_hosted(&[]).await.unwrap();
        loop {tokio::select!{
            e=a.next()=>match e.unwrap(){Some(NetworkEvent::HostingChanged(changed))=>{assert_eq!(changed,peer);break},Some(NetworkEvent::Ready(_))=>panic!("update must not restart root sync"),_=>{}},
            e=b.next()=>{e.unwrap();},
        }}
        assert!(a.peers_hosting(&child_path).is_empty());
        assert_eq!(a.peers_hosting(&["Nexus".into()]),vec![peer.clone()]);

        a.send(peer.clone(),"test.echo".into(),vec![7;200_000],77).await.unwrap();
        let mut sent=false;let mut received=false;
        while !(sent&&received) {tokio::select!{
            e=a.next()=>if let Some(NetworkEvent::Sent(77,ok))=e.unwrap(){assert!(ok);sent=true},
            e=b.next()=>if let Some(NetworkEvent::Message(_,topic,bytes))=e.unwrap(){assert_eq!(topic,"test.echo");assert_eq!(bytes,vec![7;200_000]);received=true},
        }}
        a.fetch(88,cid.clone(),false,Some(&peer)).await.unwrap();
        a.fetch(89,cid.clone(),false,Some(&peer)).await.unwrap();
        let held = loop {tokio::select!{
            e=a.next()=>if let Some(NetworkEvent::Fetched(88,volumes,reservation))=e.unwrap(){let volumes=volumes.unwrap();assert_eq!(volumes.len(),1);assert_eq!(volumes[0].root,volume.root);assert_eq!(volumes[0].entries,volume.entries);break reservation},
            e=b.next()=>{e.unwrap();},
        }};
        assert!(held.is_some());
        // Keep pumping both real actors while the consumer owns the first
        // completion. The queued second session must not consume capacity.
        assert!(timeout(Duration::from_millis(100), async {
            loop {tokio::select! {
                e=a.next()=>if matches!(e.unwrap(),Some(NetworkEvent::Fetched(89,_,_))){break},
                e=b.next()=>{e.unwrap();},
            }}
        }).await.is_err());
        drop(held);
        loop {tokio::select!{
            e=a.next()=>if let Some(NetworkEvent::Fetched(89,volumes,_reservation))=e.unwrap(){assert_eq!(volumes.unwrap()[0].entries,volume.entries);break},
            e=b.next()=>{e.unwrap();},
        }}
        a.disconnect(&peer).await.unwrap();
        loop {tokio::select!{
            e=a.next()=>if let Some(NetworkEvent::Closed(closed))=e.unwrap(){assert_eq!(closed,peer);break},
            e=b.next()=>{e.unwrap();},
        }}
        a.send(peer,"test.stale".into(),vec![],99).await.unwrap();
        assert!(matches!(a.next().await.unwrap(),Some(NetworkEvent::Sent(99,false))));
        let address = a.address;
        a.stop().await;
        a.stop().await; // Host cleanup can safely repeat shutdown.
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(listener);
        b.stop().await;
    }).await.unwrap();
}
