# Security Policy

This is a development port, not a claim of production readiness. See [PORTING.md](PORTING.md) for implemented boundaries, validation evidence and remaining gaps.

## Reporting A Vulnerability

Do not publish exploit details for high-impact findings in a public issue.

Preferred reporting path:

1. Use GitHub private vulnerability reporting if it is enabled for the repo.
2. If private reporting is not available, open a minimal public issue asking for
   a private disclosure channel. Do not include exploit payloads, private keys,
   cookies, or live target details.

Useful reports include:

- Affected commit or version.
- Whether the issue is in `cashew`, `lattice`, or the root node crate.
- Minimal reproduction steps or malformed bytes.
- Expected impact: crash, memory exhaustion, consensus divergence, invalid
  admission, data loss, auth bypass, or information leak.
- Any suggested fix or failing test.


Use exact reviewed revisions. Tests and finite formal observations are evidence
for their documented scope, not proof of complete platform security.
