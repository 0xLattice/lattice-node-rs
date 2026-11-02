# Node-local finality windows

The daemon JSON accepts `finalityWindow` (default block distance) and
`finalityWindows` (chain-path to distance overrides). The CLI forwards the same
fields from `lattice.json`. Missing or zero default means no window. An explicit
zero override disables a nonzero default for that chain. Overrides apply to the
exact chain only: an override for `Nexus/A` is not inherited by `Nexus/A/B`.
Paths use the pinned Swift slash-component normalization; duplicate normalized
paths are rejected. Settings are process configuration; change them with a
controlled restart. Newly hosted/rehosted levels use the configured policy.

A header is past the window only when its height is strictly more than the
window below the current canonical header tip. The boundary remains admissible,
as does every future height. This is an operator's local acceptance policy, not
consensus finalization or proof that a reorg is impossible. It neither deletes
existing graph facts nor changes authenticated admission requirements.

Both root and child synchronization remember declined headers in bounded sets.
A declined item counts as completed for stream cursor progress; known old child
proof announcements also finish without redundant proof downloads. Cursor
advancement crosses the existing durable journal boundary. Pending child headers
that age out are removed and their cursors progress. When the canonical header
tip becomes shorter, the remembered declined set is cleared, matching Swift's
window movement; a taller off-canonical branch does not keep the window closed.
Restart discards the transient declined set and reconstructs durable facts/cursors.

`/api/chain/info` includes `finalityWindow` as a decimal string only when enabled,
for root and hosted paths. A hosted level awaiting genesis still reports its
chain and policy, omitting unavailable tip/genesis/height fields. The Rust SDK's
`ChainInfo.finality_window` preserves the full decimal width and rejects explicit
null, consistent with its other optional decimal fields.

## Evidence and scope

Reference: `0xLattice/lattice-node@3df9e0a786c7b103a3da88d1c3881efc81706255`:
`LatticeNodeCore/Chain/ChainCore.swift` (`FinalityWindows`),
`ChainSelection.swift` (`pastFinality(height:)`),
`LatticeNodeDaemon/LatticeNodeCommand.swift` (`finalityWindows` parser),
`LatticeNode/API/ChainReads.swift` (`explorerChainInfo`). The pinned Lattice
`Chain.swift` defines its highest-header height as its canonical tip height.

`python3.12 tools/finality-swift-check.py` extracts the original selector and
height decision methods verbatim, supplies a minimal tree/configuration adapter,
and generates 1,200 cases in `tests/finality-swift.json` with pinned source hashes.
The Rust unit test compares default/override/disabled behavior and strict height
boundaries, including u64 maximums. This oracle does not execute the full Swift
scheduler, parser or graph; zero normalization is supplied by the adapter as the
pinned daemon parser specifies.

Authenticated storage tests drive root and child header ingestion and durable
cursor progress after finality refusal, including a new proof announcement for
an already-held old child. A separate trusted-durable-graph test switches from a
tall branch to a shorter heavier tip and verifies the window and decline cache.
`tests/finality_host.rs` runs actual localhost hosts through override reads,
rehosting, new hosted paths, and storage restart. The CLI lifecycle harness checks
actual daemon JSON propagation and root/child read output. These are finite
parity/regression cases, not a complete fork/scheduler differential or a claim of
consensus finality.
