# Atomic multi-level fact turns

The runtime now commits these synchronous multi-level operations through one
journal transaction before processing their queued effects:

- A mined root header and the carried headers/proofs of all hosted descendants.
- A root execution result, descendant wakes, and synchronous mining updates.
- A child execution result, descendant wakes, local proof completions, and
  synchronous mining updates.
- Each individual child `proofVerified` or `derived` completion, including every
  pending header it unblocks, its saved proofs/cursors, descendant run updates,
  and synchronous mining updates. Unrelated completions keep separate commits.

This follows Swift's `NodeCore.emit` persistence-before-effects ordering and
`NodeStorage.persistNodeBatch` all-level journal barrier. Existing authenticated
admission helpers retain their private verified capabilities. Their individual
journal transactions become nested savepoints while a node turn is active;
inside the turn, proof derivation can read newly staged parent headers/proofs.
A proof completion already inside a mined/connected event explicitly joins that
barrier. A failed constituent poisons the outer barrier even if its error is
accidentally caught, preventing a later partial commit.
Outside a turn, the original independent transaction behavior is preserved.

Owner-thread gates cover both the journal connection and content/retention
writes. Existing helpers reenter on the actor thread. Other users of the same
shared storage handle wait until the entire turn commits or rolls back, rather
than seeing provisional rows or accidentally appending writes to its transaction.
The guards cannot move across threads, and no turn closure awaits network work.
Parallel proof workers only read immutable CAS content.

Content remains committed and conservatively retained before journal writes.
Retention releases are delayed until the journal commits; a later retain in
the same scope cancels an earlier deferred release. Rollback leaves old content
owners intact and may leave extra retained content. This is deliberately not a
claim of atomic commit across the separate CAS and journal databases.

A failed outer turn may have changed provisional in-memory cores. These runtime
call sites propagate failure out of the owning host, discard those cores, and
release no queued reply, peer announcement, or published health update. Recovery
rebuilds from the previous committed journal. Child-author HTTP input refusals
remain ordinary 400 responses, but typed storage failures and every marked
proof-turn failure propagate out of the host (503 to the waiting request),
rather than reusing provisional core state. The marker also covers content or
other non-storage errors during descendant updates after provisional admission.
The internal turn helper must not
be used by a caller that catches an error and continues using provisional cores.

Validation exercises actual authenticated root/child header admission, an
injected second-level SQL failure, same-handle blocked readers and writers,
retention rollback/re-add ordering, and process exit before/after COMMIT followed
by restoration of both real chain trees and saved proofs. The actual daemon
harness additionally checks failed merged mining, follower non-advancement,
restart, successful merged mining, and recovery.

Scope remains narrower than Swift's complete `NodeCore.step`: other events
retain their existing per-core barriers, and body jobs are separate completion
turns. This change does not establish formal refinement of the full node reducer
or batch all CAS writes into a single volume-store transaction.

Additional proof-event checks authenticate a successor before its missing parent,
then inject failures at the second header, second saved proof, and second cursor
position when one proof result unlocks both headers. A composed nested derivation
checks two real root grinds and rolls both back if the later proof write fails.
The child-author daemon harness injects a proof-store failure after an independent
parent commitment, checks 503/fail-stop/no child journal, and recovers the root
commitment before retrying the child successfully.
