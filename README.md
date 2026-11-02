# lattice-node (Rust)

A runnable Rust root-node host for the pinned 0xLattice platform: durable recovery,
authenticated admission, Ivy networking, transaction preflight and mining RPC.
Hosted-child orchestration and the full operator/read API remain incomplete.

Dependencies are sibling Rust repositories under `lattice-rust-research`.

```sh
cargo test
cargo clippy --all-targets -- -D warnings
tools/generate-fact-vectors.sh
python3 tools/formal-recovery-check.py ../lattice-formal
python3 tools/formal-continuity-check.py ../lattice-formal
```

Implemented: epoch-47 SQLite journal, node-wide fact transactions, conflict checks
and state-diff restatement, durable cursors, header evidence bytes, accepted block
indices, paging/revision floors, reference-counted state pins, mempool references,
and exclusive storage directory locks. Authenticated genesis stores complete
materialized volumes before execution becomes visible. Root and child header
admission use sealed proof/preparation capabilities and persist before graph
mutation. RootChainCore coordinates session-fenced streams, durable cursor turns,
header relaying, pending-header turns and body scheduling. Canonical sync wire,
transaction pool, mining event reducer, issued work and durable root preflight
are implemented with actual Swift differential corpora. Consensus replay lives in the shared
`lattice-rs/crates/lattice-block-tree` crate, not duplicated here.

See [PORTING.md](PORTING.md) for exact source pins, coverage, differences and
remaining work. See [docs/RUNTIME.md](docs/RUNTIME.md) for the runnable CLI,
bootstrap configuration, authentication and tested runtime limits.
