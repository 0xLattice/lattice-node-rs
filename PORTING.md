# Source and compatibility coverage

Sources are restricted to the 0xLattice organization:

- `0xLattice/lattice-node` at `3df9e0a786c7b103a3da88d1c3881efc81706255` (MIT).
- `0xLattice/Lattice` at `dda2d852429a27be6043599b63bde81530cd9514` (MIT).
- `0xLattice/lattice-formal` at `a9f4c2879b6565ae11c17d35efe14475c5830978`.

The original node license is preserved in LICENSE. Rust sources are a port of the
current Swift source, not a claim that the old Rust node remains compatible.

## Implemented and checked

| Swift owner | Rust coverage | Evidence |
|---|---|---|
| NodeStore+Schema | epoch47, exact expected table set, genesis binding, indexes, WAL/FULL | reopen and incompatible metadata tests |
| NodeStore+ChainFacts | all levels in one SQLite transaction, fact conflict, identical batch deduplication, initial log id, cursor atomicity | cross-level conflict rolls back header evidence too |
| NodeStore+ImportJournal | level paging, staged records, revision floor | 1,100 batch recovery test |
| NodeStore+BlockIndex | incremental leaf flags, repair, execution status | journal exercises insertion; broad frontier pagination remains |
| NodeStore+HeaderEvidence | immutable header/index evidence, content-bound typed reads, path-bound proof journal and corrupt-row cleanup | transactional rollback, corrupted bytes rejected, typed proof cleanup tests |
| NodeStore+StatePins | atomic count changes + tip, drop counts, block fact lookup | overflow rollback and shared reference release |
| NodeStore+MempoolJournal | canonical CID, first timestamp, ordered journal | invalid records and removal test |
| NodeStorage+RuntimeEffects | all/tip/none state and all/none body retention, content-first persist and sweep exclusion | six-mode daemon matrix, signed state changes, sweep, pins, restart and mode switches; see docs/RETENTION.md |
| StorageDirectoryLock | lifetime exclusive OS lock | competing open + release test |
| ChainFacts / UInt256 | synthesized enum and ID encodings | 22 original Swift-generated batch fixtures and their IDs, byte equality |

`tools/generate-fact-vectors.sh` compiles pinned Swift ChainFacts, StateDiff and
UInt256 sources. Its CIDIdentity shim intentionally preserves the opaque fixture
labels: these fixtures verify JSON/number representation, **not CID normalization**.
Rust primitives and Cashew independently authenticate actual CIDs.

A subprocess exits without running destructors while a WAL write transaction is
open. Reopening preserves earlier committed journal facts and rejects partial
cursor/metadata writes. This is a real SQLite recovery test, not a simulation.

## Recovery model mapping

`lattice-formal/Recovery` separates journaled facts from derived graph projection.
`NodeStore::stage_node_facts` is the atomic durable transaction; `revision_floor`
counts committed batches (not CAS objects). Shared ChainTree replay rebuilds work,
execution flags and branch selection, then seals the floor. Child run work must be
re-derived from already-restored parent levels before sealing the child's floor.

Volumes must be committed and retained before their facts are staged. The volume
broker and node journal are different SQLite databases; they are not a distributed
transaction. An interrupted content-first write can leave unused retained data,
which is safe. A journal fact cannot be used to infer that every body remains
available: the reference node deliberately permits absent body references and
fetches them later. The directory lock must outlive storage tasks; `NodeStorage` serializes sweep with its content-first store/retain sequence.
Callers must keep all writes through that executor for this guarantee.

The formal model's persistence-before-observation and no-effect-on-failed-turn
properties now have a serialized root host effect executor and injected-failure
tests. The formal bridge still checks journal/graph observations, not full host
scheduling or every possible crash point.

## Remaining platform node work

- Broader hosted-child retention/reorganization and content+facts crash coverage.
- Complete owed-grind derivation scheduling and expanded hosted discovery coverage.
- Full read/operator RPC surface and runtime reconfiguration.
- Parallel bounded validation workers and scheduling/performance parity.
- Expanded node-level Swift/Rust traces beyond the signed transaction, reorganization and restart harness.

Overflow handling is deliberately fail-closed: Swift signed reference count
arithmetic traps; Rust reports an error and rolls back the whole transaction.
No full node parity or production readiness is claimed.

## Production traces checked by lattice-formal

Run `python3 tools/formal-recovery-check.py ../lattice-formal` (set `JAVA` if needed).
The runner requires the exact formal commit above and rejects tracked changes.
The upstream TLC script downloads its SHA-256-pinned TLA+ tools jar. No model source
is rewritten: the adapter imports `Recovery.tla` and directly evaluates its
`Fold`, `Apply`, `CanonicalTip`, `SumQty`, and `TrueCumWork` operators.

`examples/recovery_trace.rs` drives the actual NodeStore and ChainTree. It verifies
that the durable journal equals the staged input sequence, captures held metadata,
global grind locations and quantities, executed/excluded sets, canonical tip,
direct/subtree work and revisions from production read APIs, and throws away and
reopens SQLite between recovery snapshots. Two branch scenarios produce 101
observations including durable-before-apply visible prefixes, successful applies,
conflicting immutable metadata refusal, duplicate replay, and reverse-rotated
replay orders. Every recorded graph and output conforms to the pinned model.

Five negative controls must fail: altered direct work, subtree work, canonical tip,
and grind quantity; plus an actual production restore with the floor omitted.
The last reproduces `MCRecovery_RevisionProbe`: a deferred validation restating the
strongest grind sorts before work-only raises, reducing replay's mutation count.
With the production durable-batch floor it passes; without the floor TLC rejects
observed revision monotonicity. These negative controls all passed (were rejected).

Scope is finite trace refinement of the graph/recovery observations. The adapter
uses the model's reducer and projection directly; it does **not** prove the entire
`Recovery.Next` live-admission state machine, unbounded trace equivalence, body
availability, parent hierarchy, or temporal liveness. Atomic failure/crash and
content-first behavior are additionally checked by production SQLite tests.

## Current authenticated header and scheduling integration

`NodeStorage::import_root_header` derives actual PoW and schedule linkage;
`import_child_header` verifies the sealed child proof for the tree's chain path.
A private `PreparedAdmission` captures the exact derived fact batches and base
state. Storage writes the typed header/index and any credited proof in the same
fact transaction before applying that token. An injected SQLite trigger failure
proves the visible tree and revision do not move; retry applies normally.
No live admission API consumes a caller-deserialized work contribution. Typed
proof evidence has neither a public constructor nor Deserialize.

The core now includes decoded sync messages, session-keyed peer state, weigh-log
pagination/deduplication, bounded declined IDs, pending-header priority propagation
and leaf eviction, and BodyPipeline fetch/cancel/connect scheduling. ConnectJob
captures are private-derived from held graph facts. Missing body content parks
with bounded backoff; missing parent continuity waits without a verdict. These
are production building blocks, not yet a complete NodeCore event loop.

`HeaderSync` now implements stream negotiation, contiguous cursor advancement,
bounded holes, log resets, global in-flight data deduplication/retry, bounded
serving queues, session-fenced completion tokens, response filtering, and stalled
peer disconnects. Cursor changes are returned separately for durable staging
before effects; the outer multi-level NodeCore executor remains incomplete.
Seven sync tests cover pagination, cursor holes/reset, old sessions, deduplication,
gap handling, deadlines, queue bounds, and unmatched/empty responses.

Root genesis preparation consumes `lattice-validation::VerifiedRootGenesis`,
checks the configured root CID and child-index binding, and creates block/work/
validation facts with the authenticated state diff. It shares the immutable
prepare/stage/apply token barrier. This capability has no public constructor or
deserializer. `NodeStorage::import_root_genesis` serializes the materialized block and state
volume boundaries, commits/retains them before its facts, then applies the token.
A byte-sourced integration test verifies every stored volume, injected journal
failure leaves the graph untouched, retry, and close/reopen executed-tip recovery.
Root non-genesis execution now consumes `VerifiedRootTransition`: the graph
requires an already-held block, executed predecessor ancestry, matching spec and
post-state, and exact equality with its own inherited difficulty anchor. Its
materialized volumes and validation batch commit before execution is visible.
An authentic empty successor test covers unheld-header rejection, mismatched
anchor refusal, SQL failure/retry, and restart recovery. Child execution uses the parent-continuity capabilities described below.

`RootChainCore` now owns root recovery/bootstrap, peer sessions, stream cursor
transactions, durable header admission and tail relay, request serving, deadlines,
and body scheduling. Stream turns are prepared against cloned session state:
an injected cursor-write failure proves no live cursor/peer request advancement
and no effects are returned, while retry commits the cursor to disk first.
Received-header turns clone graph, sync, and pending state, then commit all header
facts and stream cursors in one transaction before publishing effects. Orphans
request ancestors, future headers wait before index fetch, and index responses
are fenced by peer session and requested CID. SQL failure of the second header
rolls back the entire turn; retries preserve the original request.
This coordinator is deliberately root-only. Child connect verdict orchestration,
mining, mempool coordination, child hosting, and a transport/clock executor remain.

`RootChainCore::connected_root` consumes only the active body job's matching
root capability and clears/schedules jobs after durable execution admission.

Child execution composition is implemented through separate private capabilities:
`VerifiedChildEvidence` proves sealed securing work; `VerifiedParentContinuity`
is minted only from a parent's executed graph, and `VerifiedChildParentAnchor`
binds it to child bytes and the held predecessor's anchor. Parent continuity
checks every executed branch, rejects sideways/backward movement, and requires
actual state production even when the anchors are equal. Child genesis cannot
commit the empty parent state; a later empty anchor is allowed only after an
empty predecessor anchor and without offered continuity evidence.

`NodeStorage::import_child_genesis` and `import_child_transition` derive parent
facts from the supplied parent tree and combine them with composed execution and
sealed proof verification, then persist materialized volumes/proof/facts before
applying the graph token. These APIs do not by themselves implement the node's
hosted-level event routing or asynchronous parent-evidence wakeups.

## Parent continuity against the pinned formal hierarchy

`python3 tools/formal-continuity-check.py ../lattice-formal` runs the production
`ChainTree::prove_parent_continuity` and `verify_child_parent_anchor` decisions
through the unchanged pinned `Hierarchy.tla::Continuity` operator. All **336**
observations conform across six executed-set scenarios, including missing
execution, equal anchors, empty anchors, forward/backward ancestry, sibling
branches and an executed branch outside the heavier canonical branch. Five
negative controls each must fail the invariant: accepting unproduced equality,
sideways movement, backward movement, empty genesis, and refusing the proven
noncanonical branch. All five were rejected.

Scope is a finite observation refinement using the model's distinct changing
state-per-block abstraction. It does not prove the whole hierarchy transition
system, unbounded graphs, repeated-state/no-op producer equivalence, async parent
fact availability, or liveness. Those distinctions remain covered by separate
Rust invariants where applicable. The adapter checks the formal repository SHA
and refuses tracked changes, and generates its wrapper in a temporary directory.

An end-to-end child integration builds and executes a real premine parent,
validates child genesis and a successor, creates actual sealed carrier proofs,
and persists/replays child execution. The same valid carrier proof is refused
against an empty parent graph, proving carrier bytes cannot substitute for the
parent's executed-state fact.

### Canonical chain sync wire

`core::wire` ports all five `ChainSyncWire.swift` topics, their exact field names,
optional-field omission, Base64 bytes, bounded counts/paths/CIDs/log IDs,
canonical DAG-CBOR block/index/spec nodes, and sealed proof binary. The envelope
limit is the pinned Ivy default 4 MiB frame less 256 bytes. JSON comparison uses
original input bytes after Foundation-compatible decoding (including first-key
wins, Unicode encoding detection, and integral numeric syntax). Unknown topics
are left to the shell. `RootChainCore::received_wire` dispatches root messages
through existing transaction barriers and preserves other levels for the host.

`tests/fixtures/swift-wire.json` contains 352 actual Swift outcomes, including
100 proof binary vectors, unknown/duplicate fields, optional nulls, integer
bounds, UTF encodings, malformed paths/content, and re-encoded valid bytes.
Regenerate using `python3 tools/generate-wire-vectors.py /tmp/lattice-genesis-oracle`.
That directory must contain a full cached package copied from the pinned Lattice
source. The generator checks every Swift protocol source byte and the resolved
dependency lock SHA256 `958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6`.
It compiles the unchanged node `ChainSyncWire`, `SyncProtocol`, `WireValidation`,
and `OverlayWire` against those dependencies, removing only imports for types
co-located in the executable target. Adapters supply Ivy's exact frame-size
constant and a canonical configured root CID for the original `ChainAddress`;
actual `ChainRuntimeContext` validates paths. No block/CBOR/proof semantics are
stubbed. This finite corpus checks wire compatibility, not every JSON syntax
or arbitrary-size resource behavior. Root wire integration injects a cursor
SQL failure to verify no premature effects or session advancement, then retries.

### Transaction pool and issued work

`core::mempool` implements the reference pool's measurement and resource policy,
first-signer nonce/state/deposit/receipt claims, exact arbitrary-width fee-rate
comparison, per-account committed debit budgets, next-block revenue replacement,
retention eviction, block fill with its consecutive-miss bound, four-dimensional
fee weight, projections, rebalance, removal, and wrapping mutation version.
`Mempool::balances` authenticates the account-state root and resolves only debited
keys through CAS. Missing or malformed content is unavailable, not a zero balance;
only an authenticated absent account is zero. Consensus preflight and tip fencing
remain prerequisites for `submit`; this value-type API does not mint an admission
capability or verify signatures/policies itself.

`tests/fixtures/swift-mempool.json` records 720 operations from unchanged pinned
`Mempool.swift` compiled against the full pinned protocol package. Eight seeded
traces compare each operation's error, replaced/evicted/removed IDs, pool bytes,
version, deterministic contents, next-block order and projected fees. They vary
block count, pool count, relay floor, debits, nonce/action/deposit/receipt/withdrawal
claims, rebalances and clears. Regenerate with
`python3 tools/generate-mempool-vectors.py`. Separate invariants cover sparse
balance authentication and full-width fee-weight products. This finite corpus
is not all malformed transaction inputs or a proof of policy/signature admission.

`core::work` implements bounded external work issuance, refreshing issuance order,
preserving a live work ID, exact expiry/removal, nonce substitution and search
threshold checks, including the zero-target rule. These tests are invariants,
not Swift differential evidence by themselves. The later mining reducer corpus
covers request reissue fencing; the runnable root host now composes template
assembly, actual preflight and durable pool mutation execution.

`core::mining::MiningState` now ports transaction origins, pending-slot bounds,
tip epochs, reissue limits, confirmation/reorg return, policy rechecks, ordered
durable/reply/job effects, template coalescing and build completion, and submitted
nonce handling. Its 314-turn actual Swift corpus compares every emitted effect
and pool/pending/template state, including stale verdicts across A→B→A. Pending
slot counts currently scan the bounded admission map instead of Swift's cached
counters. Reproduce with `tools/generate-mining-vectors.py` against the same full
pinned Swift cache; do not run while another oracle modifies that cache.

`DurableMiningCore` clones a turn, stores complete transaction boundaries and
live pins, applies one atomic local-pool journal delta, then releases removed
pins and swaps live state before returning replies/jobs. Journal-write failures
are retryable with unchanged reducer state; they may leave safely retained CAS
content. This is stronger atomic journal grouping than the reference shell's
individual row writes/fail-stop. Startup hydrates local rows (seconds converted
back to milliseconds), reconciles volatile roots, and reissues preflight jobs
for the matching level. A row is never treated as a saved validity verdict.

The root preflight executor requires matching current tip/epoch, exact executed
graph prefix and transaction identity, then consumes validation's private,
content-bound preflight result. `RootPreflightCompletion` has private fields;
ready balances cannot be replaced. The raw pure reducer's Preflighted event is a
trusted shell input, not a public consensus fact constructor. Signed debit and
premine integration covers successful authentication, stale completion fencing,
SQL insert/delete rollback, live-pin safety, and close/reopen local versus
volatile recovery. Hosted-child preflight remains under construction. Root transport/API wiring
is implemented as described below.


## Runnable root host

`runtime::{host,network,handshake}` bind actual Ivy TCP sessions to the durable
root core. Version-6 hello gates sync; delivery credit and send backpressure are
acknowledged using exact Ivy sessions. The adapter bounds pending fetch/send
requests, cancels obsolete fetches, authenticates complete received volumes,
serves retained boundaries/bundles and independent header indexes, and releases
transient fetch leases after each completed turn. Startup clears crash-left job
leases without removing durable journal or local-pool pins. Bundle lookup scans
saved import batches rather than Swift's bundle cache.

The serialized host drains core effects through real validation and persistence.
Accepted root bodies import private execution capabilities; refusals come only
from actual composed validation against the graph's executed parent. Missing
content remains retryable and forces a new peer fetch even if the root boundary
was cached. Root tip moves derive confirmed/returned transactions from executed
ancestry and trigger actual epoch-fenced pool preflight. Transaction RPC decodes
the content-bound body with canonical decimal-string wide integers; references
alone are rejected. Template work passes through normal header/body admission
before its API reply, with no public verified-fact bypass.

`runtime_host` covers two live Ivy root hosts, actual Swift genesis content,
template/nonce submission, durable execution, sync and restart without supplied
bootstrap; injected SQLite journal failure must stop without a success response
and restore the prior tip. `runtime_network` separately covers the v6 hello,
200KB payload, volume transfer and stale-session refusal. API/CLI process coverage
and configuration are in `docs/RUNTIME.md`. These local tests submit only fixture
work, not live network transactions. Formal checkers below the host remain finite
production graph/recovery observations, not a refinement proof of async hosting.

Remaining: full read/operator API, public provider discovery policy,
persistent key rotation/reconfiguration,
and parallel bounded validation workers. Current CPU validation and SQLite
execution are serialized in the host turn; expensive jobs can delay host commands.
The underlying Ivy runtime remains a separate task. Root consensus correctness
is tested; production scheduling/performance parity is not claimed.

RPC decimal parsing accepts the model UInt128 range, but content binding rejects
action nonces above UInt64.max because the pinned Swift DAG-CBOR encoder cannot
represent them. The shared preflight corpus confirms this boundary; the host
does not introduce a different consensus encoding to accommodate them.


### Expanded operator reads and child core (current work)

The root host now exposes chain/block/transaction/account reads, sparse deposit
and receipt witnesses, durable operator pins, complete-volume archives, provider
fetches and persistent serving credit. `docs/RUNTIME.md` lists exact routes and
bounds. The network command queue retains acknowledgements across cancelled
polls and drains effects while commands are full; a capacity-one regression
covers the deadlock/cancellation boundary.

`core/levels.rs` is the child orchestration foundation: actual proof-backed
header admission, private validated execution capabilities, parent executed-set
wait/wake, derived parent-run weights, parent-before-child recovery, and recursive
unhosting that retains journal history. No arbitrary callback can mint an
executed child fact. The executable actor still hosts only Nexus; child network
pending evidence, runtime configuration and merged template assembly remain
required. `tests/levels.rs` tests real validation, false-parent refusal, SQL
failure before live publication and successful rehosting. These tests do not
constitute full NodeCore refinement or full hosted-child runtime parity.

### Hosted actor and merged daemon integration

`runtime/host/children.rs` now routes hosted-level sync, proof checks, header-index
and body jobs through durable child cores, propagates parent execution wakeups,
and serves stored proof/spec evidence. Configuration prechecks validate creator
transactions and supplied policies before mutation. Templates select an executed
anchor-compatible child tip, suppress fallback pools/descendants, and generate
actual composed securing proofs on submission. Proof acknowledgements are checked
against persistence. Scoped child status/account/block reads are available.

`tests/runtime_children.rs` covers live Ivy child synchronization, configuration
replacement, unhost/rehost and restart. The independent daemon harness additionally
mines child genesis and successor, checks balances and durable restart without
bootstrap. Child mempool/preflight/submission and signed-transfer gossip/mining now have
actual two-daemon evidence. Remote author workflows, provider discovery
and complete API parity remain unfinished. These integration results do not turn
finite formal graph/recovery checks into a full async-state-machine proof.

### Public listener

Optional public reads and opt-in ordinary transaction submission now use a
separate explicit route allowlist, observed-IP rate budgets, bounded transport
and coalesced health loads. The rate algorithm matches 4,760 actual Swift
observations. See docs/PUBLIC_HTTP.md for operator configuration, real HTTP
tests and outstanding peer-fetch/advertisement/health scheduling limits.

### Hosted rendezvous maintenance

`core/peer_search.rs` matches 512 unchanged Swift scheduler observations.
`runtime/host/discovery.rs` now schedules hosted provider announcements, renewal,
withdrawal, stalled/low-peer lookups and root seed retries outside transport waits.
Operator pins share the configured lifetime and bounded announcement batches.
Network coalesces lookups per path, bounds requests and dials, cancels unhosted
paths, and reads live-expiry provider memberships from Ivy's authenticated cache.
See [docs/PEER_DISCOVERY.md](docs/PEER_DISCOVERY.md) for exact policy controls,
real localhost/adversarial evidence and remaining discovery parity gaps.

### Advisory peer memory and staged seed fallback

Per-chain v1/v2 peer books now persist successful direct-dial addresses, bounded
by identity, network group, age and failures. Maintenance tries remembered peers
before root named/fixed seed layers and records actual asynchronous dial results;
seed layers do not race ahead of pending remembered dials. The 328-observation
unchanged Swift corpus, storage tests and actual localhost rejoin/fallback tests
are described in `docs/PEER_DISCOVERY.md`. Initial hosted configuration now opens
all child books before maintenance begins, including child-only seed starts.
Child seeds follow remembered/provider stages and the peer threshold; successful
dials populate ancestry books. A four-host restart test verifies seed-update
suppression, child-book retention, remembered-peer priority, fallback and isolation.
