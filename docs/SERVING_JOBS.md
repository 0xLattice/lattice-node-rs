# Serving jobs and block bundles

Ivy storage jobs now run in a bounded blocking-worker set rather than inside
`Network::next`. The configured `servingMaxConcurrent` bounds physical workers;
Ivy's existing content concurrency and in-flight byte accounting still apply.
Queued cancellation produces one terminal result without reading. Running
cancellation keeps its physical slot and storage owner until the read finishes;
then the result is discarded if cancellation was observed. Shutdown discards
queued work and drains physical workers. A canceled poll does not remove jobs.
Storage errors return unavailable content rather than stopping the host.

Bundles are keyed by the requested **block root**, not by membership in an
admission batch. A spec, transaction, state or other non-block root bundles only
itself. Missing block-cache entries use local content only to reproduce pinned
Swift `BlockHeader.storeBlock`: root boundary, transaction Volumes, spec and
policy modules, selected pre-state and parent-receipt paths, and the empty state
for genesis. Parent blocks, child blocks, unrelated trie branches and post-state
are not selected. A failed walk advertises the root alone and is not cached.

`volume-bundles.db` is a disposable sidecar, never an admission/recovery
authority. Worker preservation records complete root sets while its snapshot and
acquired content are still available, before historical data can be swept.
Consequently a later bundle lookup can return those recorded roots even when
some member content has since been pruned, matching Swift's cache semantics.
Deleting the cache loses that optimization; an incomplete local walk then falls
back to the root alone. Cache open/read/write failure has the same fallback and
does not invalidate chain data. Malformed rows are ignored.

Validation includes 16 exact selected-root comparisons running the original
pinned Lattice `storeBlock` (revision
`dda2d852429a27be6043599b63bde81530cd9514`), including account/nonce, general,
deposit, receipt and parent receipt paths, policies, absent modules/state/spec,
size rejection and unresolved child blocks. The fixture records exact root sets;
it is not a proof of all Cashew graphs or of arbitrary concurrent schedules.

Tests additionally cover corrupt, missing and unopenable caches, delayed first
serve after worker preparation and pruning, reopen, queued/running cancellation,
physical-slot retention, archive byte boundaries, shutdown, and actual
localhost authenticated Ivy block-bundle/non-block retrieval. No public peer or
wallet is involved. Storage reads remain synchronous inside their blocking
worker; cancellation cannot interrupt an SQLite syscall already running.
