# Remote child-author API

The cookie-authenticated operator API implements the pinned Swift
`NodeRuntime+API.swift` author workflow:

- `POST /child-blocks/build` accepts `chainPath` and optional `recipient`. It
  builds the same child candidate that merged mining would carry, stores its
  content, and returns `blockCID`, `parentState`, optional `previous`, and a
  `submit` payload. The candidate is not admitted or executed by this operation.
- `POST /child-blocks` accepts the returned `chainPath`, base64 DAG-CBOR `block`
  and `children`, and genesis `spec`. Decode/re-encode equality rejects
  noncanonical content. It feeds the ordinary untrusted header/proof pipeline;
  status is `tree`, `pending`, `awaiting`, or `dropped`. A tree status means a
  weighed header, not by itself completed execution.
- `GET /child-blocks/commitment?chainPath=Nexus/Alpha` reads `child/Alpha` from
  the immediate parent's executed state. It returns `parentState` and optional
  `value`. Missing state is an error, never a guessed absent commitment.

The author writes the independent parent action `child/Alpha = C@P`, using
`previous` as its old value. Once a parent block carries that commitment, the
same submission can be weighed. Local derivation builds securing proofs from
held parent carriers and saved upstream proofs; ordinary private proof
validation still gates admission. The endpoint has no verified-fact bypass.

## Actual daemon check

Run `python3.12 tools/child-author-integration.py`. It builds locked binaries,
copies immutable executables, and hashes production sources and fixture inputs
before and after the run. A changed input invalidates the result. Evidence and
logs are saved in `target/child-author-integration/report.json` and sibling files.

Two actual localhost daemons exercise child genesis and a successor. The parent
does not host Alpha: each child is built on the author and independently committed
by a signed parent transaction with the correct nonce and old commitment. A
push before commitment is dropped and does not move the child tip. The post-commit
push enters the ordinary proof pipeline and executes. The child's reward balance
is checked after each block and again after each bootstrap-free author restart.
Root proof-of-work hashes are independently checked by Python SHA-256.

Negative requests cover a root-only path, unhosted path, invalid recipient,
malformed block/index/genesis-spec bytes, and trailing CBOR bytes. These are
actual HTTP responses, not injected consensus facts. No public-network work or
real-wallet keys are involved.

## Coverage limits

The daemon test covers one child, one committing parent branch, and two author
restarts. It does not establish deep-child, concurrent reorganization, crash
injection, or scheduling/performance parity. Built but never committed candidate
content remains retained under pending body/read scopes as in the source
workflow; the harness is not an eviction-policy test.

The shared actor now schedules local owed-grind derivations on header arrival
and parent-revision changes, rather than only during an operator submit. Jobs
are keyed by carrier/child and dispatched in batches of at most 32 by default;
queued tail pairs remain available. Already credited or parent-attributed grinds
are omitted. Local composed proofs pass actual verification and the same durable
admission pathway, without the peer queue's per-header eight-proof truncation.

`tests/child_derivation.rs` checks 40 independent parent carriers (32+8 dispatch),
all 40 work contributions, idempotent repeated triggers, and a newly arrived
41st carrier. A nested Alpha/Beta scenario composes 12 saved upstream proofs and
credits all 12 at Beta. Corrupted upstream proofs and missing local content grant
no work; a later header arrival retries missing content. These fixtures execute
real root-genesis validation, header admission and proof generation/verification;
they intentionally do not claim full body execution of every constructed carrier.

The parent-revision scan revisits held child branches and the actor drains jobs
serially. This preserves work coverage but does not establish Swift's event-level
latency, bounded total turn time, parallel validation, or scheduling-performance
parity. Unhosting drops the in-memory derivation queue; restored facts and new
parent/arrival triggers rebuild necessary work from local authenticated content.
