# Tip state retention differential

`tools/retention-swift-vectors.py` extracts the complete private `TipStateWalk`
actor from pinned `lattice-node@3df9e0a786c7b103a3da88d1c3881efc81706255` and
unchanged `NodeStorage.children`/`loadedNode` methods. They run against the full
pinned `Lattice@dda2d852429a27be6043599b63bde81530cd9514` and cashew dependencies
in `/tmp/lattice-genesis-oracle`. The only storage adapters supply `stateCount`
from an in-memory dictionary and `localFetcher` from `InMemoryContentSource`.
The actor and traversal methods are not rewritten or translated.

Nineteen actual Swift comparisons cover a shared empty dictionary referenced by
all four state fields, initial multi-branch/Unicode dictionaries, unchanged roots,
insertion, radix split/collapse, replacement, clearing, already-held roots and
subtrees, and absent root/dictionary bytes. Both runtimes receive unresolved
root headers so fetching and missing-byte behavior are exercised. Initial count
fixtures are computed by an independent materialized graph traversal counting
every edge while expanding each distinct CID once, rather than by the planner.

Each comparison checks the entire signed changes dictionary (including zero
entries) and sorted entering CID list. Source, dependency-lock, fixture and
harness hashes are recorded in `tests/retention-swift-evidence.json`.
Run `cargo test --locked --test retention-swift` for the stored corpus, or the
Python generator to reproduce the real Swift run.

This is planner evidence. It does not execute broker pinning, database count
transactions, crash ordering, physical eviction or runtime policy changes.
The source adapters return nonnegative counts without I/O failures. Rust's
planner explicitly rejects corrupt negative counts and arithmetic overflow;
Swift can trap on overflow. Swift's `letGo` swallows count-read errors, whereas
the Rust planner propagates a store error and emits no plan. Those failure
boundaries are not claimed equivalent by this valid-count corpus. Missing local
content does match: unseen nodes are not counted, and releasing a missing node
cannot discover or release its descendants.
