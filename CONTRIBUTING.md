# Contributing

Read [README.md](README.md), [PORTING.md](PORTING.md), and
[docs/RUNTIME.md](docs/RUNTIME.md) before changing behavior. Keep protocol
primitives and validation in the sibling `lattice-rs` workspace, generic content
and Merkle structures in `cashew-rs`, and host/runtime behavior in this repository.

Use the sibling layout documented in `lattice-rs/docs/PLATFORM_BUILD.md`. Run
`cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`,
and tests appropriate to the change. Protocol changes require unchanged Swift
comparisons and relevant formal bridges, with negative controls and limitations
recorded. A generated fixture is not an independent oracle unless its producer
and pinned inputs are documented.

Preserve private validation capabilities: disk, network, HTTP and worker inputs
must not manufacture verified facts. Keep resource bounds, cancellation and
durable-before-observation guarantees explicit. Update the relevant coverage
document when behavior or the scope of evidence changes.

Do not commit personal identifiers, private emails, machine paths, credentials,
cookies or real private keys. Use repository-relative provenance paths and
clearly identified public test vectors. Preserve required upstream license and
copyright notices; privacy edits must not falsify validation observations.
