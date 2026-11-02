# Operator metrics

The authenticated loopback listener exposes `GET /metrics` and `HEAD /metrics`
using Prometheus text exposition 0.0.4. Use the same cookie authentication as
other operator routes. The public listener does not expose this endpoint.

The four metric families match pinned Swift node
`3df9e0a786c7b103a3da88d1c3881efc81706255`:

- `lattice_chain_tip_height`, with `validated` and `weighed` tiers;
- `lattice_overlay_peers`, counting authenticated overlay sessions;
- `lattice_mempool_transactions`;
- `process_start_time_seconds`, captured once at executable startup.

All samples carry the root `chain="Nexus"` label. The endpoint follows Swift's
root operator snapshot rather than introducing a per-child query interface.
Missing tip heights omit their samples. Weighed height comes from the canonical
header tip, not the maximum height of an arbitrary held fork. In-process hosted
links are not overlay peers.

Scrapes read published state without entering the actor mailbox. A scrape can
observe the previous completed actor turn while new work is in progress. The
peer count is published separately from the chain snapshot, as in Swift's
separate peer and metrics reads. No new validation or storage traversal runs
on the HTTP task.

`python3 tools/metrics-swift-check.py ../lattice-node` executes both real
renderers and compares three exact exposition samples, including absent heights,
label escaping, and `UInt64.max`. Transport tests cover authentication, methods,
content type, and public exclusion. The executable test mines a real fixture
block and verifies metric height advancement and a stable process start time.
These checks cover the metric surface, not complete node scheduling refinement.
