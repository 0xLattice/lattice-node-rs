# Declared public read endpoints

`publicReadUrl` is an optional explicit operator declaration. It is never inferred
from a bind address. The URL must satisfy the pinned Swift validator: at most 512
printable ASCII bytes, an absolute HTTP(S) URL with a host, and no credentials,
query, or fragment. A declaration can name an external reverse proxy; it does not
prove that the URL serves this node. `publicSubmit` is declared alongside it.

Every hosted child announces its read-endpoint rendezvous key:
`lattice.read-endpoint.v1:<Nexus genesis CID>:<absolute chain path>`.
Announcement lifetime, renewal and withdrawal use the existing discovery policy.
No URL means no read-endpoint announcement. Authenticated overlay requests are
answered only for levels the node currently hosts. A submit-capable host sends
the v2 declaration before its v1-compatible URL response.

`GET /api/chain/endpoints?chainPath=Nexus/Alpha` is available on the operator and
public read APIs. The immediate parent must be hosted locally; otherwise it is
404. It combines the node's own declaration when it hosts the requested child,
then distinct URLs returned by discovered hosts. `submitEndpoints` is the subset
that declares submission support. `committedBlock`, when available, is the newest
child CID found within 64 executed canonical parent blocks. It is informational:
lookup uses the child's rendezvous, never the committed block as a provider key.
A hosted/declared child needs no existing block to be discoverable.

## Bounds and trust

Lookups coalesce by chain path, with at most 16 distinct active lookups. At most
8 authenticated providers are asked, including at most 4 already connected
providers. Provider selection is shuffled; one responder contributes at most two
URLs. A response must match a request sent to that peer and its requested path.
Repeated v1 responses cannot remove a v2 submit declaration.

A shared five-second deadline includes membership lookup, DHT search and dialing.
After the first response the remaining work gets a 500 ms grace. The transport's
250 ms maintenance timer can add that polling granularity. Successful discoveries
cache for 30 seconds; empty refreshes cache for 5 seconds. A previously successful
source can stand in for an empty refresh for at most 600 seconds from the actual
successful discovery; empty refreshes do not extend that source age. Under lookup
pressure the directory returns retained results without starting or caching more
work. Temporary dial holds are released when no lookup uses them; operational
bootstrap, seed and peer-discovery connections are preserved.

URLs and submit flags remain unverified claims. The node does not fetch the
advertised HTTP URLs while discovering them, and discovery grants no consensus
or signature authority.

## Verification

- `tests/read_endpoints.rs`: canonical wire checks and 89 verdicts produced by
  compiling the unchanged pinned Swift `isValidPublicReadURL` implementation.
  Regenerate with `python3.12 tools/read-endpoint-url-swift.py`.
- `runtime::network::read_endpoints::tests`: lookup/coalescing and provider bounds,
  deadline cancellation, response binding, per-responder quota, v2/v1 merge,
  positive/empty caching and last-success expiry. Scripted provider commands do
  not dial external addresses.
- `python3.12 tools/read-endpoint-node-integration.py`: actual localhost daemons
  discover declarations for Alpha and Beta through their hosted parents, without
  child blocks; check own URL ordering, submit flags, invalid/unhosted paths,
  nonhosting declarations, cache reuse and bootstrap-free process restart.
  Binaries are built and copied immutably; source drift invalidates the report.
  Reports and logs are under `target/read-endpoint-node-integration`.

The daemon test rotates clients to respect Ivy's default two-direct-peers-per-
netgroup policy. It uses short configured announcement renewals because loopback
addresses are not globally routable DHT referrals. Actual remote-address dialing,
public Internet URL availability and every asynchronous dial-cancellation race
are not established by this local evidence. The existing child-author harness
separately exercises independently committed parent/child blocks; the read
endpoint harness does not assert that its declared URLs serve those blocks.
