# AI audit challenge and qualification

An audit finding is a hypothesis until it survives an independent challenge pass.

The challenger is deliberately adversarial to the finding. Its job is to try to prove the original audit wrong before the organization spends engineering time or creates backlog items.

The challenge provider is the inner leaf worker already launched by the trusted
`ai-audit-challenge` process. It performs only the independent analysis and
returns the structured qualification. It must not invoke `ai-audit`,
`ai-audit-challenge`, `ai-audit-jira`, or any equivalent orchestration wrapper;
artifact and Jira publication remain owned by trusted outer processes.

## Statuses

- `CONFIRMED`: repository evidence establishes the failure path or violated invariant with high confidence. A concrete reproducer can reasonably be implemented from the supplied plan.
- `LIKELY`: evidence strongly supports the finding, but one material assumption still lacks executable or direct proof.
- `QUESTION`: correctness depends on an undocumented or ambiguous product/architecture invariant. Human input is required before treating it as a defect.
- `REJECTED`: the finding is contradicted by repository evidence, relies on an unreachable state, duplicates an existing protection, or otherwise fails challenge.

When evidence is insufficient, prefer `LIKELY` or `QUESTION`; do not manufacture certainty.

## Required contract

The challenge consumes one exact raw audit result at the same clean `HEAD`,
runs a separate read-only provider pass, and independently attempts to disprove
every finding. The qualified result must contain every original finding id
exactly once, no invented ids, and the original severity and title unchanged.
The challenge changes qualification, not finding identity.

## Challenge procedure

For every original finding:

1. preserve its stable finding id;
2. reconstruct the claimed failure path independently from current code;
3. search for guards, transactions, idempotency mechanisms, recovery paths, state-machine constraints, caller guarantees, tests, and documentation that invalidate the claim;
4. determine whether the violated invariant is actually documented or otherwise established by system semantics;
5. inspect whether the alleged state transition is reachable;
6. identify existing tests that exercise the behavior;
7. refine a reproduction plan that can become an executable regression test without changing product semantics.

Do not judge a finding true because the first model sounded confident. Do not reject it merely because no test currently demonstrates it.

## Repository target

Challenge runs are bound to the exact `head` recorded in the source audit report. The local repository must be clean and currently checked out at that exact commit. If the audit target and repository state differ, fail closed instead of silently challenging newer code.

The launcher hashes the source report before the provider runs and verifies the
same bytes remain afterward. It records that SHA-256 identity as
`sourceAuditDigest` in the published qualified report so downstream consumers
can verify the exact source artifact. The launcher supplies that trusted value
to the reasoning provider and rejects the result unless it copies the value
exactly; the provider does not determine its own provenance value.

## Mutation policy

The challenge pass is read-only. It must not edit code, add a reproducer, create issues, commit, push, comment, or resolve anything. Reproducer implementation is a separate later phase.

## Output

The qualified report must contain one challenge result for every original finding id and no invented finding ids. It may also retain unresolved audit questions. For each result record:

- original severity/title;
- qualification status;
- challenge summary;
- evidence for and against;
- invariant assessment;
- reachability assessment;
- relevant existing tests;
- refined reproduction plan;
- recommended next action.

A `CONFIRMED` result is suitable to become a backlog/Jira candidate after human or policy approval. `REJECTED` findings should remain in the audit history for deduplication rather than simply disappearing.

The qualified report is rejected unless every result reuses an original finding id exactly once and preserves that finding's severity and title. A challenge pass may change the status, not the priority or the subject of the original finding.

The launcher validates the trusted source digest in a temporary file under
`build/ai-audit/` and atomically renames the qualification to its final name.
Provider failure or validation failure publishes nothing. The source report and
tracked repository content are never written.
