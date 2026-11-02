# Root platform runtime

The Rust node has a runnable root and hosted-child actor, authenticated Ivy TCP,
a cookie-protected loopback HTTP listener, and a process CLI. Two actual daemon
processes have mined merged root/child genesis and successor blocks, synchronized
proof-backed children, checked balances, and recovered both levels after restart.
The full Swift explorer/content/operator API,
some public read cache/scheduling details and distribution remain incomplete.

## Configuration

Run `cargo run --locked --bin lattice-node -- --config node.json`:

```json
{
  "directory": "data",
  "identityKey": "identity.key",
  "genesisCid": "<verified root genesis CID>",
  "bootstrapFiles": ["genesis-volumes.json"],
  "listen": "127.0.0.1:7447",
  "rpc": "127.0.0.1:8080",
  "seeds": [],
  "allowedOrigins": [],
  "state": "all",
  "blockBodies": "all"
}
```

Paths are relative to the configuration file. A bootstrap file is an array of
Cashew `SerializedVolume` JSON objects (`root`, `entries` mapping CIDs to byte
arrays). The host checks content and executes actual root-genesis validation;
supplying a CID does not establish trust in arbitrary bytes. Existing databases
can restart with an empty `bootstrapFiles` list. The data directory is locked
against simultaneous writers.

The Ed25519 identity is created privately when missing, or read from 32-byte
hex / a Lattice key JSON object's `privateKey`. A seed is
`{"publicKey":"<64 hex>","host":"127.0.0.1","port":7447}`. The ready line
prints RPC/overlay addresses and the public key; it never prints secrets.
Port zero permits an ephemeral port for integration tests.

`minRelayFee` defaults to `1`, matching the Swift daemon; explicitly set `0`
for a zero-fee local fixture. The core library retains its separate default.

`state` accepts `all`, `tip`, or `none`; `blockBodies` accepts `all` or `none`.
Both default to `all`. Authenticated `POST /sweep` performs retention maintenance
and removes unretained volumes, returning `{"removed": n}`. With `state=none`,
swept data can make offline account reads unavailable. Mode changes preserve
existing permanent pins. See [RETENTION.md](RETENTION.md) for semantics and the
six-mode daemon validation record.

Optional `publicRead` binds a separate public read listener; `publicSubmit`
(default false) enables ordinary transaction submission there. `publicReadRates`
configures independent arrival budgets. See [PUBLIC_HTTP.md](PUBLIC_HTTP.md)
for exact routes, validation and remaining gaps. No public listener is opened
by default.

`peerSearchInterval`, `peerTarget`, `peersBeforeSeeds`, and `announceLifetime`
configure hosted discovery (defaults 600 seconds, 8, 2, and 1200 seconds).

The RPC address must be loopback. A new mode-0600 cookie is atomically written
at `data/.cookie` on startup (override with `cookieFile`). Clients send
`Authorization: Basic base64(file contents)` or `Bearer <token after colon>`.
Normal shutdown removes only this process's cookie, leaving a newer owner's
cookie intact. SIGINT and SIGTERM stop the HTTP listener and host.

## Current RPC surface

- `GET /health`: unauthenticated process health.
- `GET /status`: executed tip, height and current host status.
- `GET /hosted-chains`: hosted child paths in parent-first order.
- `POST /hosted-chains`: replace the hosted configuration using `hostChain`,
  `premineTo`, `genesisTransactions`, `chainPeer`, and `policyModule` arrays,
  matching the Swift flag-shaped request. Unhosting keeps durable facts.
  The same fields are accepted in CLI JSON; file paths there are relative to
  the configuration file. Operator POST paths are relative to the process.
  Configuration files are limited to 4 MiB each and arrays to 256 entries.
  Desired hosting is reapplied at startup, not implicitly persisted by the node.
- `POST /mining/templates`: `{ "recipients": [] }` or the supported root
  recipient/minimum-work plan. The search target is separate from the block's
  scheduled target.
- `POST /mining/work`: `{ "workID": "...", "nonce": 0 }`. Work must pass
  actual admission and durable execution before successful completion.
- `POST /transactions`: the platform signed-transaction payload.

- Root reads: `/api/chain/info`, `/api/chain/spec`, `/api/chain/genesis`,
  `/api/chain/children`, `/api/peers`, `/api/mempool`, `/api/fees`,
  `/api/block/latest`, `/api/block/:height-or-CID`, `/api/blocks`,
  `/api/block/:CID/transactions`, `/api/block/:CID/children`,
  `/api/transaction/:CID`, `/transactions/:CID`, `/accounts/:owner` and
  `/api/state/account/:owner`. Integer DTO fields follow Swift decimal strings.
- `/api/deposits`, `/api/deposit-state`, `/api/receipt-state`: targeted CAS
  dictionary witnesses anchored to the executed tip, including authenticated
  absence. Pagination resolves only the requested trie window.
- `GET/POST /pins`, `DELETE /pins/:CID`: durable operator retention, validated
  optional base64 entries and renewed provider advertisements.
- `GET /volumes/:CID`: bounded binary complete-volume archive;
  `GET /fetch/:CID`: Ivy provider retrieval with validated base64 entries.
- `GET /serving-credit/:peer`, `POST /serving-credit`: persistent Tally credit.

HEAD is supported for GET routes. Unknown routes return 404. Read routes retain
queries, including first-value duplicate query semantics. Bodies are capped at
1 MiB, except pin requests at 2 MiB. Binary volumes are capped at 64 MiB; JSON
responses at 4 MiB, except volume fetches and dictionary witnesses at 96 MiB
(to accommodate bounded base64 data). There is a 15-second body read timeout,
64 concurrent slots and a 30-second host-response deadline.
A timed-out request may already have reached the host: clients must inspect
state before retrying a mutation. The current host is serialized and does not
promise scheduling fairness for expensive validation jobs.

Origins are denied by default, including on health. `allowedOrigins` is an
exact allowlist: permitted origins receive CORS preflight/responses but still
need the cookie. No authentication challenge is emitted, matching the Swift
client compatibility rule. JSON parsing uses the existing Swift-compatible
wire decoder (including first-value duplicate-key semantics).

## Evidence and limits

`runtime_network` tests exercise real authenticated TCP/hello, a 200KB payload,
validated volume transfer and session fencing. `runtime_host` tests exercise
actual template construction, durable admission/execution, peer synchronization
and restart. `runtime_api` tests real HTTP authentication, origins, refusals,
size limits and cookie lifecycle. `runtime_cli` launches the binary, requests
and submits fixture work through HTTP, sends SIGTERM, and restarts from disk
without bootstrap volumes. These are local tests, not live network mining.

Template construction has differential coverage against the unchanged Swift
assembly. Node-generated absence/empty dictionary witnesses are checked by the
independent `lattice-light-client` verifier. The verifier separately has original
Swift differential coverage; byte-for-byte node proof generation parity is not
claimed.

`core::levels` provides proof-backed child imports, parent-before-child recovery,
parent continuity wakeups, parent-run rederivation and recursive unhosting.
Its bounded proof and pending-header schedulers authenticate real securing proofs,
repair missing retained proofs without crediting work twice, and fence index
responses by peer session. Recovery reloads all journaled genesis branch specs
from authenticated retained CAS. Focused tests include actual validation,
missing-parent parking, injected SQLite failure, proof repair and rehosting
without operator-supplied specs. Network hosting capability updates preserve root
sessions, and shutdown awaits Ivy transport teardown through an independent
signal. The actor now routes child stream/header/index/body work, runs actual proof jobs,
wakes children on executed parent facts, and assembles merged templates from
anchored executed snapshots. Scoped `/status`, `/accounts/:owner`, chain info,
genesis, transaction and block reads accept `?chainPath=Nexus/Alpha`. Child
transaction pools restore durable rows, preflight against executed child and
parent snapshots, gossip to hosting peers, and feed merged templates. An actual
signed child transfer was gossiped, mined and checked on both daemons after
restart; malformed signatures were rejected. Remote child-author build, push and commitment routes are implemented; see
[CHILD_AUTHOR_INTEGRATION.md](CHILD_AUTHOR_INTEGRATION.md) for the two-daemon
workflow and remaining derivation coverage. Local child work replies acknowledge only proofs
actually retained for the submitted root. See `docs/CHILD_NODE_INTEGRATION.md`
for the independent daemon harness evidence.
Public endpoint discovery and transient peer-backed volume retrieval are implemented;
see [READ_ENDPOINTS.md](READ_ENDPOINTS.md) and [PUBLIC_HTTP.md](PUBLIC_HTTP.md).
Canonical-height reads currently walk parent links; they do not yet use Swift's
optimized height index. No complete performance or API parity is claimed.
Existing lattice-formal bridges validate finite consensus/recovery traces;
they do not currently prove host scheduling, HTTP serving, or process lifecycle.
