# Retention implementation and integration status

`storage/retention.rs` provides the pinned Swift scope names, state/body mode
types and a typed `plan_tip_state` walk. It counts repeated references, expands
outgoing edges only at zero crossings, holds the new state before releasing the
old state, and skips locally missing nodes. Nineteen actual Swift comparisons
cover the planner; see [retention-swift.md](retention-swift.md).

`NodeStorage` now exposes the storage executors:

- `retain_stored_content`: required body roots plus available read roots, under
  separate body/read or chain scopes.
- `retain_execution_content`: permanent constants, all-state retention, temporary
  tip execution retention, or replacement of the last nonempty execution set.
- `keep_tip_state`: pin entering roots, commit counts and tip atomically, then
  release leaving roots and the temporary execution scope.
- `release_block_reads`, `reconcile_read_scopes`, `release_old_bodies`: preserve
  pending content, genesis bodies and live tips according to the supplied mode.
- `replace_cached_volumes`: validate/store replacement content, then atomically
  replace a job/configuration scope under the same lock used by sweep.

Real SQLite/CAS tests in `retention_storage.rs` verify replacement refusal,
shared state references, sweep behavior and durable reopening. The planner
rejects count corruption/overflow instead of trapping; count-read errors propagate
instead of being swallowed during Swift's best-effort release walk.

## Daemon integration

CLI configuration accepts `state` (`all`, `tip`, `none`) and `blockBodies`
(`all`, `none`), both defaulting to `all`. `Host::start_with_retention` exposes
that policy to library callers; `Host::start` preserves the default.

Execution classifies block/transaction bodies, state outputs, branch specs and
policy modules under the same storage lock as CAS writes, pinning and journal
commit. Nested child candidates receive separate body/read scopes. Permanent
root genesis content remains retained. The host maintains executed tips and
releases finished read/body scopes after draining the queued node turn, and
before the authenticated `POST /sweep` operation. Opening a database outside
`tip` mode clears old state-count records.

`all` keeps execution history. `tip` tracks references reachable from each
hosted executed tip. `none` keeps the latest nonempty execution output set;
it does not promise a complete offline account read after sweep. A later block
that leaves an account nonce untouched can make that read return 404. Existing
permanent pins from an earlier `all` run remain conservative retention roots
when switching modes. Operator pins overlap safely with automatic retention.

The actual daemon harness passed all six policy combinations, signed state
changes, physical sweep observations, overlapping operator pins, bootstrap-free
restart and policy switches. See
[RETENTION_NODE_INTEGRATION.md](RETENTION_NODE_INTEGRATION.md) for evidence and
its explicit unavailable-read control.

## Limits

Retention maintenance failure does not manufacture an invalid-block verdict.
A stop between count commit and pin release can retain extra content; the Swift
same-tip fast path does not retry every interrupted cleanup. The current port
preserves that conservative behavior and does not claim exact reclamation after
every crash. SQLite fault injection verifies rollback of state counts and tip,
with both old and entering content retained for retry.

The host currently scans retention scopes after queued turns; this is not a
performance parity claim. The six-mode matrix covers root execution. Additional two-daemon runs cover
hosted-child signed transfers, gossip, sweep and bootstrap-free restart with
`tip/none` state and `none` bodies. A broader child reorganization/crash matrix
remains outstanding.
