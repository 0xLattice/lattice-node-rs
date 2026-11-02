# Published health reads

Both listeners answer GET/HEAD `/health` from immutable per-chain snapshots.
They never send an actor command, acquire ordinary RPC admission slots, replay
the journal, compute templates, or enumerate mempool transactions. The actor
publishes after draining completed work; startup publishes restored state and
initial hosted scopes before returning the running host. Readers hold a short
read lock only while cloning one status value.

This follows pinned Swift `ChainReads.readSnapshot`, `NodeRuntime.reads`, and
`ChainReadsByPath`: root and currently hosted child levels are available by exact
`chainPath`; a hosted child without execution reports `awaitingGenesis` without
inventing a tip or height. Unhosting removes its snapshot despite retained
storage facts. The act-on tip is the heaviest executed tip. `bestHeaderHeight`
is the canonical header tip's height, not the maximum height across forks.
Mempool count/bytes are published without a listing. Health omits templateDigest
and revision, matching Swift; no new waiting-reason inference is introduced.

The public listener retains its three-second root cache and cancellation-safe
shared load. Scoped health is uncached. Loopback reads the current published
value. Both expose the three-second HTTP cache policy for successful health
responses and explicit HEAD support; missing scopes are 404. Published state
can remain at the last completed turn while validation is busy, just as Swift's
published view does. This does not make other explorer endpoints independent
of the actor or make its mutation turns asynchronous.

Tests cover competing header/executed forks, a full unpolled command channel,
exact scope matching, awaiting genesis, native restored child execution,
HTTP backends that panic if health enters the request path, root-cache waiter
cancellation, and actual public/loopback daemon health after mining and restart.
