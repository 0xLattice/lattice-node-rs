# Hosted genesis premine precheck

`runtime::hosted_config::check_premine(spec, recipient, transactions, path,
modules, now)` is a pure preflight for hosted configuration. `modules` contains
raw WASM bytes, and `now` is the caller's UTC timestamp in milliseconds. The
helper performs no fetching, persistence, configuration mutation or admission.
`premine_transactions(spec, recipient, path)` constructs the optional unsigned
protocol premine transaction for reuse by callers.

Each explicit creator transaction must carry a materialized body, match the
requested chain path, have a nonempty set of valid normalized signatures whose
derived addresses equal its declared signer set, and contain no account,
deposit, receipt or withdrawal actions. This signature rule applies to creator
transactions, not the protocol-generated premine. Duplicate signer declarations
retain Swift's set semantics. Additional action/state validity is a later gate.

The optional premine is prepended and included in the transaction count. Its
recipient must be a canonical address and the amount must fit one signed account
action. A missing recipient or zero amount yields no premine transaction. Only
transaction-scoped policies execute here; supplied raw modules are indexed by
canonical typed WasmPolicyModule CID. The original full spec remains in each
policy context, with height zero and no action/index. Swift requires a matching
module even for an empty body list, but only evaluates ABI/exports when there
is a body. Full configured-module validation is a separate genesis gate.

`tools/hosted-config-swift-vectors.py` compiles exact extracted `premine`,
`checkPremine` and `PremineError` definitions from pinned
`lattice-node@3df9e0a786c7b103a3da88d1c3881efc81706255` against unchanged full
`Lattice@dda2d852429a27be6043599b63bde81530cd9514`. Only the ChainPath type alias
and executable harness are supplied. It reuses `/tmp/lattice-genesis-oracle`,
checks production source bytes and the dependency lock, and records source,
harness and corpus hashes in `tests/hosted-config-swift-evidence.json`.

Twenty-nine actual Swift cases match acceptance/refusal and exact premine CIDs.
They exercise signatures, path/body presence, all coin-action classes, amount
and count limits, policy acceptance/refusal, wrong/missing/duplicate modules,
action-scope exclusion and empty-body behavior. The finite policy fixtures do
not depend on time: unchanged Swift reads Date while Rust receives timestamp 1.
This does not claim identical error prose or arbitrary policy/runtime equivalence.
Run `cargo test --locked --test hosted-config-swift` for the stored regression,
or the Python generator to rebuild evidence with the pinned Swift cache present.
