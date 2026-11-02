# Local hosted-child templates

`runtime/merged_template.rs` ports local child candidate construction and fitting
from 0xLattice/lattice-node at `3df9e0a786c7b103a3da88d1c3881efc81706255`,
`NodeRuntime+API.swift` and `MiningTemplateAssembly.swift`.

`build_merged_template` consumes a root job plus per-path `ChildTemplateContext`
snapshots. The host must select each child's extendable executed ancestor for
its carrier's entering state and fence completion against every affected level's
tip, epoch, pool and hosting configuration. Inputs are ordinary work data;
neither construction nor fitting grants admission or securing-work authority.
Mined children still need actual authenticated proofs and private validation.

The builder binds child parent state to the carrier's entering state, uses the
child schedule's committed target, filters policies at the candidate height and
time, and omits both pool and grandchildren when extending an older compatible
tip. A configured child without an executed tip gets a genesis only after
production genesis validation; a child with no genesis spec waits. A genesis
carries no grandchildren. Invalid or unavailable child candidates are omitted.

Fitting greedily bisects execution-conflicting transactions, filters commitments
into hosted slots, rotates supplied children by predecessor height, and fits
transaction and child counts against resolved logical content size. The shared
assembly performs the Swift withdrawal gate, including signature validation and
authenticated parent receipts, when any supplied transaction body is unresolved
or has withdrawals.

Minimum-work plans affect search thresholds and work identity only. Local
candidates carry no descendant scheduling witness, matching Swift's local path;
unresolved deeper filters conservatively cap search. Multiple thresholds are
advertised only when every supplied carried child has the canonical empty index.
Remote witness-based candidate negotiation is not implemented by this module.

`tests/merged_template.rs` checks root parity, configured genesis versus waiting,
parent-state binding, ancestor-fallback pool suppression, hosted-slot filtering,
and minimum-work caps without commitment mutation. Existing root template tests
remain required. `tools/merged-template-swift-vectors.py` and the associated
`merged-template-swift` test compare 24 actual unchanged Swift assembly cases,
including 12 transactions (numeric index order), byte budgets, height rotation,
and search thresholds. Evidence records corpus, harness and source hashes.

Adding children must preserve every other block commitment; rebuilding a
selected pool uses numeric transaction indices, never Merkle lexical order.
Status and work share `merged_template_digest`, including all child act-on tips,
best header tips and pools in Swift's depth-then-joined-path order, even when a
candidate waits or is omitted. Completion still requires epoch fencing.
Daemon configuration, scheduling and end-to-end child mining must be validated
at their integration boundary before claiming complete hosted-child support.

The `/mining/templates` actor path runs assembly in the same bounded blocking
pool as root/child transaction preflight (at most four workers, capped by available
parallelism). Execution effects drain first. The actor captures all hosted-level
revisions, executed/header tips, mining epochs/pool versions and template-relevant
configuration CIDs; workers read a committed WAL snapshot and cannot mutate facts.
Completion compares the entire basis before applying *any* policy refusals or
issuing work. A child-only change reissues the original request, preserving
coalesced waiters and their existing bounded retry counts. Exhaustion answers
`ContextChanged`; stale errors do not become current `BuildFailed` responses.
Runtime ingress caps all waiting template callers (including coalesced requests)
at the configured limit and returns busy before inserting an excess waiter.
The pure reducer retains the pinned Swift trace, whose coalescing branch does not
apply that bound. Cancelled waiters release queue slots, and jobs with no remaining waiters are
skipped before snapshot creation. A running blocking job keeps its snapshot and
storage lease until it exits even when its requester disconnects.

The actor still selects child contexts and reads their headers synchronously.
The separate `/child-blocks/build` authoring route still assembles synchronously;
this change does not claim a fully asynchronous runtime. Snapshot cache, WAL
lifetime and cancellation limits are the same as the preflight pool described in
`PORTING.md`. Unit tests exercise the actual single-thread host while a completed
assembly worker is held: status and child-hosting requests proceed, a changed
child basis discards the old result, and both waiting callers receive rebuilt
work. Reducer tests cover retry exhaustion and late completion generations;
additional actor tests check queued/running cancellation, coalesced ingress
saturation, and a stale policy-refusal map naming a live transaction CID without
removing it from the current pool before retry.
