# Shared acquisition and serving limits

The production network adapter uses one acquisition reservation queue for all
root and hosted-child downloads, transaction/index requests, and operator
retrievals. `maxAcquisitionBytes` defaults to 256 MiB and must fit at least one
64 MiB session. Each remote operation reserves the full 64 MiB allowance before
issuing its Ivy request. Saturation queues work without refusing it or waiting
inside the host actor. Local operator retrievals do not reserve remote capacity.

Ordering follows pinned Swift `AcquisitionBudget.swift` at node revision
`3df9e0a786c7b103a3da88d1c3881efc81706255`: shallow body depth first; at equal
depth, current next-to-connect bodies precede look-ahead; own work shares root
next-to-connect priority; ties preserve FIFO. The host refreshes the frontier
from each live core after draining effects. The first waiter must fit; smaller
later requests cannot bypass it. Root priority can delay descendants while the
parent catches up, as in Swift.

A grant is single-owner. Cancelling queued work removes it without a grant;
cancelling issued work retains a tombstone and its grant until Ivy's terminal
completion (including an already-queued completion racing cancellation).
Completed content carries the grant into the host event, through cache admission
and synchronous consumption, or operator response encoding. Dropping that event
wakes the queue. This is separate from storage retention leases: cached bodies,
worker snapshots, and pending connections use their existing storage ownership.

`servingMaxConcurrent` (default 64) and `servingMaxInFlightVolumeBytes` (default
Ivy's limit) configure the existing Ivy serving scheduler; both must be positive.
Serving keeps Ivy's peer-standing/virtual-time fairness, not acquisition priority.
The CLI validates and forwards all three settings to daemon JSON configuration.
The daemon also passes the configured serving byte budget into the public-read
coordinator. Its remote concurrency is `min(requested, max(1, bytes / 2 / 64 MiB))`,
with requested zero staying zero (local content only). Integer division rounds
down; the at-least-one exception permits a single read even below 128 MiB. This
matches pinned Swift `PublicVolumeFetches.limit` and `NodeRuntimeMode.swift`.
The prior fixed 128 MiB argument is removed. Public reads retain their documented
503/busy behavior at their own concurrency cap; internal acquisition saturation
continues to queue.

These reservations bound admitted remote archive sessions, not total process
memory. Metadata for queued requests, transient decoding/encoding copies,
returned operator JSON, disk caches, SQLite/WAL, and worker snapshots are outside
this accounting. Rust resolves execution/template content from its retained
local store; this adapter does not claim Swift's in-memory read-cache eviction
implementation or a new asynchronous missing-content walk for those workers.

Validation: queue tests cover depth/frontier/FIFO, head-of-line fairness and
cancellation. The real Network adapter test covers cancelled tombstones, queued
cancellation, local-only bypass, consumer handoff and wakeup. The authenticated
localhost TCP integration holds the first completed session's grant while
pumping both Ivy actors, observes the second request remain queued, then drops
the grant and verifies the second exact volume. CLI tests check forwarded values
and reject zero serving limits or acquisition capacity below one session.

Public-read boundary tests cover zero, below/at/above a two-read allowance,
configured-request ceilings and maximum integer input without multiplication
overflow. A gated coordinator test admits exactly two distinct reads at 256 MiB,
refuses a third without invoking the source, releases both, and then serves the
third; disabled mode never invokes the source.
