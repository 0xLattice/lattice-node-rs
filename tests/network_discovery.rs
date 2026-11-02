//! Three real authenticated Ivy nodes exercise provider discovery without
//! bypassing the rejection of third-party private/loopback referrals.
use lattice_node::{runtime::network::Network, storage::NodeStorage};
use std::sync::Arc;
use tokio::time::{Duration, timeout};

async fn network(seed: u8, directory: &std::path::Path) -> Network {
    let genesis = "bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia";
    Network::bind(
        "127.0.0.1:0".parse().unwrap(),
        ed25519_dalek::SigningKey::from_bytes(&[seed; 32]),
        genesis.into(),
        Arc::new(NodeStorage::open(directory, genesis, "node").unwrap()),
    )
    .await
    .unwrap()
}
fn endpoint(n: &Network) -> ivy_core::PeerEndpoint {
    ivy_core::PeerEndpoint {
        public_key: n.public_key.clone(),
        host: "127.0.0.1".into(),
        port: n.address.port(),
    }
}
#[tokio::test]
async fn hosted_rendezvous_rejects_private_referrals_and_recognizes_direct_providers() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut seeker = network(61, dirs[0].path()).await;
    let mut bridge = network(62, dirs[1].path()).await;
    let mut provider = network(63, dirs[2].path()).await;
    let path = vec!["Nexus".into(), "Alpha".into()];
    seeker
        .set_hosted(std::slice::from_ref(&path))
        .await
        .unwrap();
    seeker.connect(endpoint(&bridge)).await.unwrap();
    provider.connect(endpoint(&bridge)).await.unwrap();
    timeout(Duration::from_secs(60), async {
        while seeker.peers().len() != 1 || provider.peers().len() != 1 || bridge.peers().len() != 2 {
            tokio::select! { e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} }
        }
        assert!(seeker.peers_hosting(&path).is_empty());
        let key = lattice_node::core::peer_search::rendezvous("bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia", &path);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        provider.announce_provider(key, now + 3600).unwrap();
        // Drive all transport actors while the signed announcement traverses DHT.
        let settle = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(settle);
        loop { tokio::select! { _=&mut settle=>break, e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} } }
        seeker.discover_hosted(path.clone()).await.unwrap();
        // Duplicate requests are coalesced per path.
        seeker.discover_hosted(path.clone()).await.unwrap();
        while seeker.searching_hosted(&path) {
            tokio::select! { e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} }
        }
        // A third-party localhost referral cannot initiate a dial.
        assert_eq!(seeker.peers().len(), 1);
        assert!(seeker.peers_hosting(&path).is_empty());
        seeker.connect(endpoint(&provider)).await.unwrap();
        while seeker.peers().len() != 2 || provider.peers().len() != 2 {
            tokio::select! { e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} }
        }
        provider.announce_provider(lattice_node::core::peer_search::rendezvous("bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia", &path), now + 3600).unwrap();
        let settle = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(settle);
        loop { tokio::select! { _=&mut settle=>break, e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} } }
        seeker.discover_hosted(path.clone()).await.unwrap();
        while seeker.searching_hosted(&path) {
            tokio::select! { e=seeker.next()=>{e.unwrap();}, e=bridge.next()=>{e.unwrap();}, e=provider.next()=>{e.unwrap();} }
        }
        assert_eq!(seeker.peers_hosting(&path).len(), 1);
        // The provider deliberately sent no host:Alpha hello claim. Recognition
        // came from the signed rendezvous result, then authenticated connection.
        seeker.set_hosted(&[]).await.unwrap();
        assert!(seeker.peers_hosting(&path).is_empty());
        assert_eq!(seeker.peers().len(), 2);
    }).await.unwrap();
    seeker.stop().await;
    bridge.stop().await;
    provider.stop().await;
}
