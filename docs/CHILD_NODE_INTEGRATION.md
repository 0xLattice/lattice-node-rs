# Hosted-child daemon integration

`python3 tools/child-node-integration.py` builds and runs two actual Rust node
executables on disposable localhost listeners. The completed scenario passes:
merged root heights 1/2, child heights 0/1, native Ivy synchronization, signed
child transaction propagation and execution, and durable restart without
bootstrap files. The report records exact source, lockfile and binary hashes.
Missing features fail rather than supplying synthetic admission facts.

The scenario starts from canonical Nexus genesis and configures `Nexus/Alpha`
with a real ChainSpec and a public RFC 8032 test identity as premine recipient.
The observer starts with no child spec or child bootstrap content. Both nodes
must independently execute child genesis and report balance 10. A signed
nonce-0 transfer of 3 with fee 1 is submitted to the observer; the producer must
receive it through native Ivy gossip. The producer mines it in the next merged
root/child template. Both nodes must report sender balance 17, nonce 1, receiver
balance 3, nonce 0, and empty child pools. The sender receives the reward 10 and
fee 1 as the child block's mining recipient. Both accounts and executed tips
must survive clean restart without bootstrap files. Hosting configuration is
explicitly reapplied after restart; this does not claim it persists automatically.

An independent Python SHA-256 check verifies each nonce against every advertised
target before submission. Durable child-proof acknowledgements must match both
hosts' independently executed child tips. Both hosts reject altered signatures
with `invalid state`; a generic readiness error cannot satisfy that control.
Both hosts also return the child transaction DTO with the correct chain path,
canonical block hash and height, and a raw transaction with its body CID.
No live service or public-network peer is configured.

The HTTP contracts follow pinned Swift
`Sources/LatticeNodeDaemon/LatticeNodeCommand.swift`: `HostedChildren` reads
`hostChain` entries as `path=spec-file` and `premineTo` as `path=address`;
`HostedChainsRequest` replaces the hosted set; GET queries select a child via
`chainPath=Nexus%2FAlpha`. The disposable child spec uses premine 1 and initial
reward 10. The optional chain-path argument added to the existing
`lattice-validation` example `mixed_node_signer` preserves its default Nexus
behavior.

Reports are written to `target/child-node-integration/report.json`. Each new
invocation invalidates prior success before building. Binaries are copied to
an immutable temporary directory, hashed, and used for both initial runs and
restarts. Source and Cargo lockfile hashes are compared before final success.
Logs and intermediate round observations survive failure. Only a report with
`passed: true` records a completed scenario.

The harness also runs two authenticated Ivy peer controls from
`tests/child_adversarial_peer.rs`. A peer answers genuine stream/data requests
with a canonical wire envelope containing a wrong-directory securing proof.
The host must disconnect it and persist neither child execution nor the proof.
The matching valid-proof control executes the child over the same network path.
Its source fixture funds the parent state because empty-state parent anchors
intentionally cannot advance child continuity. Neither control inserts admission
facts into the receiving host. They can also run independently with
`cargo test --locked --test child_adversarial_peer`; scoped Clippy with warnings
denied passes. The report records `authenticated_ivy_proof_controls` separately
from the HTTP proof-injection route, which the pinned API does not expose.

This test exposed and verified a fix for a production actor bug: executing an
already-selected child header can advance the executed frontier without changing
the header graph revision. Child-pool initialization must run after execution
completion rather than depending only on that revision changing.

The final signed-child run completed at Unix time 1793587186.6983712 using node
binary SHA-256 `bab243b7f18fe8d63240e805c6c2da144475c7ba983aaa928a7fbd6511bc09dc`.
This is finite checkpoint evidence, not a proof covering all histories. Deeper
hierarchies, child reorganization, partition recovery and cross-language child
interoperability remain outside this scenario.

## Retention and dormant-child recovery

The same harness also passed with `--state tip --block-bodies none --sweep`
and `--state none --block-bodies none --sweep`, using distinct output directories
`target/child-retention-tip` and `target/child-retention-none`. Both actual daemons
mine/gossip the signed child transfer, sweep, restart without bootstrap volumes,
reapply hosting, sweep again, and read the expected sender/receiver balances.
The immutable binary hashes and source fingerprints are recorded in each report.

These checks exposed and fixed a startup cleanup error: root restoration runs
before desired child hosting is applied. Body cleanup must skip paths absent
from the active tip map, preserving dormant child tips for rehosting. Absence
from that map is not permission to release every executed body for that path.

With pruned state, transaction DTO location can be omitted when the historical
nonce state needed for canonical inclusion is unavailable. This matches pinned
Swift `ChainReads.canonicalInclusion`; the harness still requires a correct
location whenever present, and requires it in the original all-state run.
This is not a historical inclusion guarantee under pruning. These runs cover
one child and no reorganization or injected process crash.
