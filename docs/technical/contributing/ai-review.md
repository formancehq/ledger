# AI review contract

AI review is a correctness and risk-reduction pass, not a second implementation pass. The reviewer should minimize human attention by reporting only actionable findings that are supported by the repository, the diff, or a concrete execution path.

Use this contract for reviews performed by Codex, Claude, NumaryBot, or another AI reviewer.

## Review objectives

A review should answer four questions:

1. Does the change satisfy the stated task without changing unrelated behavior?
2. Does it preserve repository invariants and subsystem contracts?
3. Are the tests and validation strong enough to catch the regression class being changed?
4. Is there any concrete risk that should block merge or be surfaced to a human?

For significant technical decisions, also consume `docs/technical/contributing/product-technical-traceability.md` as the canonical intent chain. Use the documented product/operational need and observable requirement to judge implementation correctness. Never invent missing motivation or rewrite the requirement to match the code; missing or conflicting intent is a human-decision question, not a rationale to manufacture.

Do not use review bandwidth for personal style preferences, speculative refactors, or restating what the diff already says.

## Severity and blocking status

Severity describes impact. Blocking status describes whether the PR should merge before the finding is addressed. Keep them separate.

| Severity | Meaning | Typical examples |
|---|---|---|
| **P0** | Catastrophic or immediately unsafe | data corruption, security boundary bypass, deterministic-replica divergence, unrecoverable persisted-state damage |
| **P1** | High-impact correctness failure | wrong business result, lost/duplicated committed work, broken compatibility contract, deadlock in a production path |
| **P2** | Bounded correctness/reliability gap | incomplete validation, weakened regression test, race window, misleading authoritative docs, failure in a narrower path |
| **P3** | Non-critical improvement | readability, small cleanup, optional refactor, documentation polish with no correctness impact |

A finding is **blocking** when merging with it would knowingly accept incorrect behavior, violate an invariant or documented contract, leave a material regression untested, or make authoritative guidance materially false.

P0/P1 findings are normally blocking. P2 findings may be blocking or non-blocking depending on the demonstrated impact. P3 findings are normally non-blocking.

## Evidence requirement

Every finding must identify a concrete failure mechanism. A valid finding should include:

- the affected file/symbol/path;
- what the code or documentation currently does;
- the execution path, invariant, test gap, or contract that makes this a problem;
- the user/system impact;
- what condition would make the finding resolved.

Prefer evidence from current code, tests, authoritative subsystem documentation, generated/API contracts, documented product/operational requirements, or reproducible command output.

Do not file a finding based only on:

- a generic best practice;
- a style preference not encoded by repository conventions;
- a hypothetical problem without a plausible path from the changed code;
- stale architecture assumptions contradicted by current code;
- a request to broaden the PR beyond its stated scope without a correctness reason.

If evidence is uncertain, say so and classify the item as a concern/question rather than presenting it as a confirmed blocker.

## Finding format

Use a compact structure:

```text
[P1][blocking] Short imperative title
Location: path/to/file.go:123 (symbol if useful)
Evidence: <specific execution path, invariant, test, or contract>
Impact: <what can go wrong>
Resolution: <observable condition that would resolve the finding>
```

Keep one root cause per finding. Do not split one issue into several comments merely because it appears at multiple call sites.

## What not to review

Do not report:

- formatting already enforced mechanically;
- generated-file drift already caught by canonical validation unless the check itself is bypassed;
- naming/style preferences that do not violate repository conventions;
- optional abstractions or refactors unrelated to correctness;
- unchanged pre-existing issues unless the PR makes them worse or the new change depends on them being correct.

If a pre-existing issue prevents the new change from being safe, explain that dependency explicitly.

## Test review

Do not accept a test merely because it passes. Check that the observation actually proves the behavior claimed by the test and, for significant decisions, the observable requirement recorded in the traceability chain.

For concurrency, timing, retry, and failure-path tests, prefer synchronization on an observable state transition over arbitrary sleeps. In particular, verify that a rewritten test still reaches the original regression state rather than only a nearby proxy state.

When a test is intentionally probabilistic or stress-based, state that clearly and verify that its failure signal corresponds to the invariant being tested.

## Re-review contract

A re-review starts from the current HEAD, not from the reviewer's memory of an older diff.

1. Re-evaluate every previous finding and classify it as **fixed**, **still valid**, or **outdated**.
2. Do not repeat fixed/outdated findings as new findings.
3. Inspect the fixes for regressions introduced while addressing review feedback.
4. Review newly changed lines and their directly affected execution paths.
5. Do not reopen the entire design discussion unless the fix changes the design or reveals a new correctness issue.

Structured reviewers must report those classifications in `previous_findings` using the stable finding `id`:

```json
{
  "previous_findings": [
    {
      "id": "preserve-commit-order",
      "status": "FIXED",
      "reason": "The acknowledgement now follows the durable commit."
    }
  ]
}
```

The array is required and empty on a first review. On re-review it must classify every finding from the immediately preceding result exactly once. `STILL_VALID` findings remain in the current `findings` array with the same id; `FIXED` and `OUTDATED` findings do not. Every classification includes a non-empty reason. This makes the re-review history machine-readable without treating an old review as authoritative instructions.

The re-review summary should state whether any prior findings remain and whether any genuinely new findings were discovered.

## Final decision

Use one of these outcomes:

### APPROVE

No unresolved blocking findings remain. Non-blocking concerns may be listed separately.

### REQUEST CHANGES

At least one unresolved blocking finding remains. List only the blockers needed to reach approval, followed by any materially useful non-blocking concerns.

### HUMAN DECISION REQUIRED

Use this only when repository evidence cannot decide the issue, for example a genuine product choice, conflicting authoritative sources, missing traceability for a significant technical decision, or a proposed change to a non-negotiable invariant.

## Review summary

End with a compressed summary:

```text
DECISION: APPROVE | REQUEST CHANGES | HUMAN DECISION REQUIRED
Head reviewed: <commit SHA>
Blocking findings: <count>
Non-blocking findings: <count>
Previous findings: <fixed / still valid / outdated counts, for re-review>
Validation considered: <relevant CI/checks/tests, or N/A>
Residual risk: LOW | MEDIUM | HIGH
```

The summary is a decision aid, not a transcript. Human reviewers should be able to understand why the PR is or is not mergeable without reconstructing the review process.
