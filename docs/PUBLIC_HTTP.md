# Public HTTP listener

The daemon accepts an optional `publicRead` socket address, for example
`"127.0.0.1:8081"` for local integration or an operator-selected public bind.
It is disabled when omitted. The ready event reports its actual address as
`publicRead`; the operator listener still requires a loopback address and cookie.
Both listeners stop on SIGINT/SIGTERM or host failure.

The public listener explicitly permits the implemented chain/block/transaction,
account, dictionary-witness, mempool, fee, peer and complete-volume reads. It does
not expose operator status, hosted configuration, pins, sweep, provider fetch,
serving-credit, mining, or child-author commands. Read responses allow cross-origin
access. Unknown routes and disallowed methods return 404; rate rejection returns
429 with CORS headers. Only `/health` accepts HEAD, matching Swift's explicit route.
Empty path components are normalized consistently for routing and rate billing.

`publicSubmit: true` additionally permits `POST /transactions` and POST CORS
preflight; it requires `publicRead`. This uses the ordinary `Submitted` admission
origin, capacity, validation and gossip. It does not journal the transaction as
local or grant local priority. No other mutation becomes public. Submit is off
by default. Public `/api/chain/info` reports this listener setting in
`acceptsSubmit`, including scoped child reads, rather than the private operator
backend's admission capability.

`publicReadRates` optionally overrides `general`, `expensive`, `listener`, and
`submit` rates in requests/second (defaults 25, 1, 200, 10). Zero disables that
ceiling. General/expensive banks hold 2/10 seconds; listener/submit banks hold
1/2 seconds, with at least one token. Expensive and submit requests share the
client's expensive bank; submits use a separate listener bank. Client rejection
never spends the listener bank. Client storage has Swift's derived ceiling and
fully-refilled eviction rule; overflow clients fall back to the listener bank.

Identity comes from the socket IP, never forwarding headers or source ports.
IPv6 clients share a /64 budget (Swift's public listener binds IPv4 only).
Nonfinite, negative, and overflowing rates are configuration errors; this avoids
Swift's out-of-range integer conversion trap.

Root health bypasses every bucket and coalesces backend loads into a three-second
cache whose age starts when loading completes. A cancelled HTTP waiter does not
cancel the shared load; a focused cancellation test checks this boundary. Failed
Rust backend snapshots are cached as 503 for the same TTL to bound retries. Other scoped health reads use the
bounded request path. Health currently requests the Rust host's status through
its serialized actor; unlike Swift's storage snapshot it is not yet independent
of consensus scheduling. Requests have existing body/response caps and deadlines;
64 concurrent noncached requests bound admission to the backend.

Successful CID-bound transaction, account-at-block, block, block-membership,
volume and genesis reads carry `public, max-age=31536000, immutable`. Latest/height
block selection, transaction inclusion, live accounts, chain info/spec/endpoints,
children, mempool, fees, peers and dictionary state carry `public, max-age=3`.
The authenticated JSON read surface shares this policy. Error responses and
submissions receive neither successful-read policy; in particular, missing
content and unavailable health snapshots are not advertised as cacheable success.
The server-side health failure TTL described above still bounds retries.

## Validation

`tools/public-rate-swift-vectors.py` extracts the unchanged rate configuration,
route classifier and limiter from pinned Swift node source, supplies deterministic
clocks, and records 4,760 observations. `tests/public_rate.rs` compares admission,
route class and tracked-client count, including disabled/tiny rates, source
churn, duplicate separators, submit/read separation and backwards clock samples.
The generator verifies the source against the pinned Git object; provenance is
stored alongside the corpus. This is finite differential coverage, not a formal
proof of the listener.

`tests/public_api.rs` uses real TCP HTTP to check operator route isolation,
CORS, forwarding-header non-authority, shared health loading, opt-in submit and
separate listener budgets, all public JSON cache classes, error/submit cache
exclusion, and per-listener submission capability. `runtime_cli` launches the actual binary with both
listeners, checks public reads without a cookie, mines fixture work via private
RPC, shuts down and restarts from durable state.

`tools/public-node-integration.py` builds and copies immutable binaries, checks
source fingerprints before/after, and runs an actual localhost daemon. A forged
public transaction is refused; a signed transfer is accepted, mined through
private RPC, read through public account routes, and checked after bootstrap-free
restart. Public operator routes remain inaccessible. The report is
`target/public-node-integration/report.json`; no fixture admission facts are
injected.

The public volume route first serves held archives, then retrieves from Ivy
providers without pinning or persisting the result. `publicReadFetches` defaults
to 4; the effective distinct-CID limit also reserves half the node's 128 MiB
content budget at the 64 MiB archive cap (currently one remote fetch). Zero
disables remote capacity. Concurrent requests for a CID share the task and one
archive buffer, including after a waiter disconnects. The coordinator has no
completed-result cache. Busy capacity/stopping returns 503 with `Retry-After: 1`;
unserved or invalid content returns 404. CID and volume validation precede
encoding. Fetch tasks have a 30-second wrapper deadline. The host cancels abandoned
operational fetches before draining queued work or admitting replacement commands;
Ivy receives cancellation before a replacement retrieval. Consensus work is never
cancelled by an HTTP disconnect. Canonical CID spelling and hosted chain scope
are checked before local lookup or network retrieval.

Explicit `publicReadUrl` advertisement and child endpoint discovery are now
implemented; see [READ_ENDPOINTS.md](READ_ENDPOINTS.md). The listener address is
never used to guess the advertised URL. An independent storage health snapshot
remains work.
No public deployment or Internet load test has been performed.
