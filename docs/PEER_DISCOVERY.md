# Hosted peer discovery

The host advertises each hosted child under the pinned Swift key
`lattice.chain-peers.v1:<Nexus genesis CID>:<joined chain path>`. Root peers need
no rendezvous. Hosting changes withdraw obsolete advertisements; announcements
renew before their expiry and are repeated after an initially disconnected node
connects. Operator pins share the same wanted-key set, renewal/withdrawal policy,
and configured lifetime. Changes to pins force a maintenance pass. Announcements
are sent in batches of at most 16 per 250 milliseconds, so a large pin set does
not exhaust the command queue in one turn. A lookup only supplies candidate peers: headers, child securing proofs,
state execution and fork choice still use their normal authenticated paths.

`DiscoveryConfig` and the CLI expose `peerSearchInterval` (seconds, default 600),
`peerTarget` (8), `peersBeforeSeeds` (2) and `announceLifetime` (1200 seconds).
Lifetime must be between 1 and 86400 seconds. A nonpositive/nonfinite search
interval disables stalled/top-up clocks; maintenance still runs every 600 seconds,
and a connected child with no peers can still query its rendezvous. Positive
maintenance cadence is clamped to 1–600 seconds. Existing host start functions
retain defaults; `start_with_policies` accepts both retention and discovery policy.

Per-level progress high-water, stalled and low-peer clocks follow unchanged Swift
`PeerSearch` observations. Networking runs lookups and dials independently of the
node actor. There is at most one discovery/cache request per path, 64 outstanding
requests overall, and four randomized candidate dials per completed lookup.
Unhosting cancels outstanding requests; their late completions cannot reconnect
that removed level. Configured root seeds retry at maintenance cadence when
below the seed threshold or stalled; at most 64 are retried in a pass.

A peer hosts a child if its compatible hello claims it or the authenticated Ivy
provider cache names it. The additive Ivy `ProviderMemberships` request snapshots
identities and their latest live expiration from that cache; the node does not
invent a TTL from the endpoint-only lookup result. Cached memberships expire
locally and trigger a hosting-change event. Discovery never relaxes Ivy's address
policy: third-party private/loopback referrals remain forbidden.

## Evidence

- `tests/peer_search.rs` compares 512 stateful observations against compiled,
  unchanged pinned Swift `PeerSearch` methods. The reproducible source extraction
  is in `tools/generate-peer-search-fixtures.py`.
- `tests/network_discovery.rs` drives three real authenticated localhost Ivy
  nodes. It verifies rejection of third-party loopback referrals, then direct
  authenticated provider recognition without a host capability claim, and removal
  of that recognition when unhosted.
- The Network adapter unit test supplies controlled Ivy completion messages with
  the transport stopped. It checks the four-dial bound, request coalescing and
  rejection of late results after unhosting. Its public fixture endpoints are
  never dialled; it is not an internet connectivity test.
- Ivy's actual core provider-round test now queries the new membership snapshot
  before and exactly at expiry, verifying live identity/expiry and removal.
- Configuration boundary tests cover disabled searches, minimum cadence, enormous
  finite intervals, nonfinite library input, and accepted/refused TTL boundaries.

## Remaining work

This is not full Swift discovery/operator parity. Operator `providerWalkSessions`
policy and external-address configuration still need porting. Public read-endpoint discovery is documented
separately. Root explicit bootstraps retain the existing `seeds` array. The host uses
its periodic/configuration/first-connection passes; it does not reproduce every
Swift maintenance trigger. Public-address multi-hop discovery/dialing has not
been exercised on a real network. The real localhost test deliberately preserves
address-policy restrictions rather than adding a testing bypass.

## Durable memory and seed fallback

`runtime/peer_memory.rs` ports Swift's advisory `peers.json` books. Each hosted
chain has a book of at most 256 peers, at most 8 per IPv4 /16 (other hosts have
separate case-insensitive groups). Entries expire after 30 days unseen or 10
failed dials. Only an address this process successfully dialled through an
identity-authenticated direct connection is remembered. An inbound connection or
advertised third-party address cannot populate the book. Explicit bootstraps and
this node's own identity are excluded; fallback seeds are ordinary remembered
peers. An isolated startup with no explicit bootstraps, root named seeds or initial
child seeds ignores an existing book. Adding seeds dynamically to such an isolated
process does not enable memory; this matches Swift’s startup policy.

Version 1 node-wide files load as the root book; version 2 stores books per path.
Loads are bounded by 256 KiB per hosted chain and 1024 candidate records per book.
Malformed, oversized or unavailable files mean empty memory. Atomic writes use
mode 0600; write failures remain advisory. Extreme signed timestamps use widened
subtraction rather than trapping on overflow. Swift-compatible key encoding
checks here accept 32-byte values without treating them as verified identities;
Ivy still performs normal connection authentication.

The actor progresses a nonblocking maintenance plan through remembered candidates,
child provider lookup, configured seeds for that level (`DiscoveryConfig.seed_peers`
for root, hosted `chainPeers` for children), and fixed root seeds (`fixed_seed_peers`). It waits for each stage's actual completion before
asking the next seed layer, while RPC and consensus continue to run. Four randomly
selected remembered addresses are tried per level, and an endpoint is attempted
only once in a pass. A dropped session brings maintenance forward, bounded by the
Swift 10-second retry interval. Existing explicit bootstraps are retried when the
root stalls. Seed layers are asked only below `peersBeforeSeeds`.

`tests/peer_memory.rs` compares 328 actual unchanged Swift peer-book observations
and covers durable reopen, child books, exclusions, failure eviction, malformed
and oversized files, v1 migration, private file permissions and advisory write
failure. `tools/generate-peer-memory-fixtures.py` compiles the pinned declarations
against actual IvyCore/Tally/Crypto objects; only the Ivy re-export import is
replaced with IvyCore. Random draws are tested for eligibility and bounds, not
cross-language PRNG sequence identity.

`tests/peer_memory_host.rs` runs actual localhost hosts. It proves a fresh node
remembers reached seeds, rejoins from memory without contacting a newly offered
seed, falls back when remembered peers stop, restarts without bootstrap volumes,
and ignores the book in isolated mode. A separate actual-host test proves failed
named-address dials fall back to the fixed loopback address. No public DNS or
public-network dial is used by these tests.

Initial hosting is applied before maintenance starts, through
`start_with_hosted_configuration` (also used by the CLI). Memory opens every
initially hosted chain book before any save, including child-only seed starts.
Successful child seed dials populate that child’s ancestry books. Updating child
seeds wakes maintenance and respects its peer threshold instead of unconditionally
dialling the new list. The four-host child test exercises all three ancestry books,
seed-update suppression, remembered peers before seeds across restart, fallback
after remembered peers stop, and isolated restart.

`tools/child-peer-memory-swift.py` builds the pinned, unchanged full Swift runtime
and runs four loopback nodes with STUN, health probes and public seeds disabled.
It checks child-only memory enablement, three ancestry books on disk, conditional
seed updates, remembered-peer reconnection, fallback and isolated memory policy.
The report records whether a newly configured seed was also contacted on restart:
Swift can contact it before the reconnected peers' hosted capabilities arrive.
The Rust test's exact seed-suppression assertion describes its controlled fixture;
it is not a claim that every Swift restart suppresses every seed dial. Reports
and compiler/run logs stay under ignored `target/child-peer-memory-swift`.
