# Daemon retention integration

`python3 tools/retention-node-integration.py` passed all six retention-mode
combinations against the actual daemon. The run mined 23 blocks, executed 16
signed transactions, and verified sweep, bootstrap-free recovery, shared pins
and mode switches. No production source changes were needed by this harness.

The harness builds immutable executable copies, runs disposable localhost nodes
for all six `state = all/tip/none` × `blockBodies = all/none` configurations,
mines three actual blocks with signed payments changing balances and nonce, and
invokes authenticated `POST /sweep`. It compares current RPC accounts and
read-only SQLite volume inventories: old unpinned bodies remain only with
`blockBodies=all`, old unpinned state roots remain only with `state=all`, and
the touched working set and genesis remain available. Operator pins overlap actual block
and state scopes; they must survive sweep/restart, and removing one pin must
preserve roots still held by another scope. No database writes or fabricated
admission facts are used by the harness.

Every mode restarts without bootstrap files and checks identical tips, touched accounts,
and operator pins. A further sequence changes `none → tip → all → none → tip`
across restarts while mining new state at each step. The sequence checks current
state availability and continued operator-pin survival; switching to `all`
does not imply recreating historical content already deleted.

Reports initialize `passed: false` before any build and become successful only
after every case and a source/lockfile hash stability check. Intermediate
observations and logs are retained under `target/retention-node-integration`.
Missing configuration/API support is an explicit failure, not a skipped test.
The runner reuses the existing child daemon process/bootstrap helper and public
RFC test signer, with no public peers or external submissions.

This is finite daemon evidence. Abrupt crash injection, child-level mode
transitions and arbitrary competing-branch reclamation are outside its current
scope; storage transaction failure tests cover separate lower-level boundaries.

The first execution confirmed expected `none` semantics: after a reward-only
block, an earlier untouched nonce trie node can be swept, so an offline account
query returns 404. The completed harness preserves this explicit negative case
before and after restart. Positive working-set cases touch the account balances
and nonce each block. `none` is not a promise to retain the entire current state;
other omitted nodes require peer retrieval when read again.

The recorded successful run completed at Unix time 1793587180.7848032 using
node SHA-256 `bab243b7f18fe8d63240e805c6c2da144475c7ba983aaa928a7fbd6511bc09dc`.
The report includes immutable executable and transitive source/lockfile hashes.
Previously chain-pinned history can remain after switching from `all` to another
mode, matching Swift; the mode-switch assertions do not demand retrospective
removal of those older chain pins.
