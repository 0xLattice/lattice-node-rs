# Child proof validation execution

The runtime evaluates each admitted proof/derivation batch using at most four
CPU workers (also capped by available parallelism). Workers run only the real
verifier and produce private completion values. The actor applies completions
in dispatch order, writes durable facts, and performs peer/session effects.
No worker can supply a public validity flag or mutate a chain. Batch tails are
preserved; worker count does not cap the number of credited carriers.

A missing held child block is a local availability outcome. It releases the
in-flight proof slot without blaming the sender and follows Swift's `skipped`
lookup retry: clear the awaiting header's lookup flag, or relookup a still-held
or pending header. A completion whose slot was already removed has no effect.
The runtime stops the current drain after a missing-block result so the retry
runs on a subsequent actor turn instead of spinning against the same missing
content. A malformed proof with its block available still follows normal peer
punishment.

Source references are the pinned Swift `NodeRuntime.swift` verifyProof and
 deriveProof effects, `ChainCore.swift` proofDropped event, and
`ChildProofs.swift` skipped helper. Rust keeps synchronous completion here:
scoped workers parallelize pure verification but the actor waits for the batch.
This is not the full Swift asynchronous execution queue, and does not introduce
atomic multi-level durable transactions.

Validation includes concurrent worker-bound/complete-tail/order checks, real
valid and corrupt child proofs, missing-local-block retry/late-completion
controls, and the existing nested/multicarrier derivation and actual daemon
child-author restart harnesses.
