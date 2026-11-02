# Local merged-template Swift differential

Run `python3 tools/merged-template-swift-vectors.py`, then
`cargo test --locked --test merged-template-swift` from `lattice-node-rs`.
The generator requires the same pinned `/tmp/lattice-genesis-oracle` full Lattice
cache as the root-template oracle. It checks original library source bytes and
the dependency-lock hash before compiling a separate `MergedTemplateSwiftOracle`
target. It does not change production Swift sources.

The oracle compiles the complete pinned node `MiningTemplateAssembly.swift`,
removing only imports supplied by extracted support definitions in this standalone
target. Production assembly, fit, target scheduling and leaf detection methods
are unchanged. Source, dependency lock, harness and corpus hashes are recorded
in `tests/merged-template-swift-evidence.json`.

Twenty-four cases compare exact block CIDs, transaction CIDs, carried direct
child directories and ordered target vectors. They include hosted commitments
with and without a supplied local child, transaction-count/byte caps, no-child,
one-child and all-child fits, height rotation selecting a/b/c, binding root and
child minimum work, unresolved descendant caps, absent direct filters, leaf
threshold ladders and nonleaf single-target results. A twelve-transaction case exercises numeric index order past index 9 and compares
the full block CID, preventing lexicographic 0,1,10,11,2 ordering from passing.
Negative cases cover an
impossible byte budget. The regression asserts branch-specific outcomes so a
corpus where everything fails cannot pass.

Direct child candidates intentionally have no remote scheduling witness. The
fixture controls time and supplies the previous header as the difficulty anchor;
this does not validate real ancestry, recursively executed state, child securing
proofs, or concurrent epoch fencing. The current Rust implementation matches all finite cases after correcting
numeric transaction extraction identified during review. Recursive construction and host lifecycle integration have separate tests.
