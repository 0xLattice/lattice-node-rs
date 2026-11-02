use lattice_cashew::SerializedVolume;
use lattice_node::{
    runtime::network::{Network, NetworkEvent},
    storage::NodeStorage,
};
use std::{collections::BTreeSet, sync::Arc};
#[tokio::test]
async fn authenticated_peer_receives_exact_block_bundle_and_nonblock_singleton() {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("serving-bundle-swift.json")).unwrap();
    let row = rows.iter().find(|r| r["name"] == "combined").unwrap();
    let root = row["root"].as_str().unwrap();
    let volumes: Vec<SerializedVolume> = serde_json::from_value(row["volumes"].clone()).unwrap();
    let expected: BTreeSet<String> = serde_json::from_value(row["swift"].clone()).unwrap();
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let a_store = Arc::new(NodeStorage::open(a_dir.path(), root, "test").unwrap());
    let b_store = Arc::new(NodeStorage::open(b_dir.path(), root, "test").unwrap());
    b_store.cache_volumes("test", &volumes).unwrap();
    let block: lattice_primitives::Block =
        lattice_cashew::cbor::decode(&volumes.iter().find_map(|v| v.entries.get(root)).unwrap())
            .unwrap();
    let spec = block.spec.raw_cid.clone();
    let mut a = Network::bind(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[41; 32]),
        root.into(),
        a_store,
    )
    .await
    .unwrap();
    let mut b = Network::bind(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[42; 32]),
        root.into(),
        b_store,
    )
    .await
    .unwrap();
    a.connect(ivy_core::PeerEndpoint {
        public_key: b.public_key.clone(),
        host: "127.0.0.1".into(),
        port: b.address.port(),
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        let(mut ar,mut br)=(false,false);
        while !ar || !br {tokio::select!{
            e=a.next()=>if matches!(e.unwrap(),Some(NetworkEvent::Ready(_))){ar=true},
            e=b.next()=>if matches!(e.unwrap(),Some(NetworkEvent::Ready(_))){br=true},
        }}
        let peer=a.peers().remove(0);
        for (tag,cid,wanted) in [(1,root.to_owned(),expected),(2,spec.clone(),BTreeSet::from([spec]))] {
            a.fetch(tag,cid,true,Some(&peer)).await.unwrap();
            loop {tokio::select!{
                e=a.next()=>if let Some(NetworkEvent::Fetched(id,volumes,_reservation))=e.unwrap(){assert_eq!(id,tag);let got:BTreeSet<_>=volumes.unwrap().into_iter().map(|v|v.root).collect();assert_eq!(got,wanted);break},
                e=b.next()=>{e.unwrap();},
            }}
        }
    }).await.unwrap();
    a.stop().await;
    b.stop().await;
}
