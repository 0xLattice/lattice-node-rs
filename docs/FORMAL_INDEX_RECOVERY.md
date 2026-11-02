# Authenticated child-index recovery observations

Run `python3.12 tools/formal-index-recovery-check.py` from this repository with
its sibling `0xLattice/lattice-formal` checkout at
`a9f4c2879b6565ae11c17d35efe14475c5830978`. Tracked formal modifications or another
revision are refused. The pinned TLC launcher verifies its tool JAR checksum.
Rust dependencies must already be available for offline locked compilation;
Java is required for TLC.

The exporter runs the production storage test
`storage::turn_tests::one_child_index_result_commits_unblocked_headers_proofs_and_cursor_together`.
It starts with an authenticated Nexus genesis and two real child headers. The
successor arrives before its parent; real `ChildBlockProof` verification leaves
them pending until `ChildChainCore::child_index_fetched` receives the committed
index. The production fact-turn boundary then admits both headers, saves both
proofs and advances the stream cursor together. This is the production core
and storage route, not a test validity callback or an HTTP/network scheduling
trace. The parent stays unchanged in this fixture.

Four scenarios cover success and SQLite refusal of the second header, second
proof, or final cursor write. All storage connections are closed, the directory
lock is reacquired, and the production root/child restoration routines reopen
the same database. Provisional in-memory cores after a failed turn are discarded;
this does not claim their mutated state can safely continue.

## What the model checks

Seventeen observations encode actual journal batches and independently read
live/recovered tree state. The generated adapter imports unchanged
`Recovery.tla` and calls its `Fold`, `Apply`, `CanonicalTip`, `SumQty` and
`TrueCumWork` operators. TLC compares held blocks, timestamp metadata, grind
locations/quantities, validation/exclusion sets, weights, cumulative work and
tips. A separate adapter invariant checks recovered revision against the actual
durable revision floor. It is not a full `Recovery.Next` execution trace.

The adapter accepts only this fixture's exact batch shape: one block and one
securing work fact, with optional validation. It refuses unknown shapes instead
of silently decomposing a richer atomic batch. Metadata is projected to timestamp;
this is not a refinement of every header field. The fixture exercises child
header admission, not child body execution, arbitrary forks, full UInt256
arithmetic or signature/policy semantics. Work quantities fit TLC's finite
integer domain. CIDs are fixture labels, not a proof of hashing.

Root and child observations are checked separately. `Recovery` models one chain;
its operators do **not** prove cross-level SQLite atomicity, cursor/proof
persistence, or the multilevel Host event schedule. Separate production
assertions check the event footprint (both headers, both proofs, both batches,
final cursor), its survival after reopen, all-or-none refusal, and unchanged
parent state. The existing ten storage-turn tests additionally exercise genuine
multilevel failure/crash schedules; those are production tests, not an extension
of the formal model.

Nine negative controls reject an altered weight, subtree work, missing durable
fact, wrong tip, lowered recovered revision, altered timestamp, failed-event
facts visible after reopen, partial proof persistence and partial cursor
advancement. The first seven fail named TLC invariants; the last two fail the
separate event-footprint assertions. The harness does not use a second Python
implementation of graph reduction or fork choice.

Reports are written to `target/formal-index-recovery-report.json` and
`target/formal-index-recovery-trace.json`. They record the formal pin/model hash,
the exact executed Rust test artifact hash, trace-file hash and adapter/exporter
input hashes. Before compilation the runner captures byte snapshots of the model,
TLC launcher and local source inputs, plus the formal revision/clean status. It
rejects any observed change after compilation, after execution or after model
checks. The test artifact is hashed before/after execution and again at completion.
`target/formal-index-recovery-inputs.json` records the local source manifest using
sibling-relative paths. Coverage includes every local Cargo dependency's `src`,
`tests`, `examples`, build script, and ancestor manifests/lockfiles. Target-specific
metadata uses the native host target so offline inspection does not fetch
unrelated platform dependencies. Missing/added files within these directories
also invalidate the run.

This records local-source stability at those checkpoints, not a hermetic build:
registry caches, toolchain binaries, build environment and assets outside the
named directories are not source-snapshotted. It does not detect an edit reverted
between checkpoints. The artifact hash identifies the exact compiled bytes;
no frozen whole-platform checkout is claimed. No private identity or machine
paths are included in the report. `python3.12 tools/test_formal_index_recovery.py`
checks provenance rejection for changed/added/removed sources and altered formal
revision/status/model/launcher snapshots in disposable fixtures.

Remaining work includes original `DerivedChildGrinds` index/proof scheduling
operators, newly mutating multilevel event projections, additional crash points,
and complete admission/scheduling composition. No full NodeCore proof is claimed.
