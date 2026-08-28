# AI PR legitimacy triage

PR triage answers a different question from code review: **is this change worth spending review and maintenance capacity on?**

It runs before the technical review/fix loop and is advisory. It never closes a PR, merges code, posts comments, or invents product intent.

## Trust boundary

Launch the triage runner from a trusted checkout, never from the PR head being
evaluated. After GitHub resolves the exact target and head commits, the runner
creates two detached worktrees:

- the target commit is the trust root for `AGENTS.md`, agent context, this
  contract, the product-technical-traceability contract, the structured-output
  schema, and the Codex working directory;
- the head commit is exposed separately and only as untrusted evidence.

The committed PR delta starts at the immutable merge base of those commits, not
at the current target tip. This prevents later target-branch changes from being
attributed to the PR. A PR cannot supply or replace the policy used to decide
its own legitimacy. Consequently, a newly introduced triage policy becomes
available only after it reaches the target branch; its bootstrap review uses the
normal technical review workflow.

Both detached worktrees are workflow-owned child worktrees. The provider
process uses an explicit process cwd (`env -C`) in the trusted target worktree,
receives the expected PR/worktree/HEAD values, and is surrounded by the same
primary-checkout mutation detection described in
[AI PR worktree isolation](ai-worktree-isolation.md).

## Decisions

- `KEEP`: repository evidence establishes a concrete need and the change is proportionate enough to justify technical review.
- `QUESTION`: legitimacy depends on missing or ambiguous product/operational context. Human or author input is required before expensive review.
- `REJECT`: repository evidence shows that the proposed change is unnecessary, duplicative, obsolete, or materially disproportionate to the documented need. This is advisory only; a human decides what to do with the PR.

When uncertain between `KEEP` and another decision, use `QUESTION`. Never manufacture a rationale for a technical choice.

## Product motivation rule

Use `docs/technical/contributing/product-technical-traceability.md` as the canonical contract for significant technical decisions.

Every significant decision must trace through a concrete product or operational need, observable requirement/constraint, chosen technical decision, implementation, and validation evidence. Examples include a new abstraction or subsystem, persistence/cache/consistency strategy, API or semantic change, dependency, retry/idempotency mechanism, distributed-systems mechanism, compatibility strategy, or meaningful maintenance/complexity increase.

Mechanical changes such as typo fixes, dead-import removal, narrow test-helper maintenance, generated-file refreshes, or behavior-preserving renames do not require a separate product rationale.

A technical preference (`cleaner`, `more generic`, `future-proof`, `best practice`) is not by itself a product need.

If a significant decision is present and one or more links in the traceability chain are missing, prefer `QUESTION` over reconstructing a plausible rationale from implementation details.

## Evidence

Use repository evidence first: PR title/body, linked issue/spec when present in the PR text, code and tests, technical/product documentation, and relevant repository history that is locally available. Do not require a Jira/issue link when the need is already clearly documented in-repo.

Do not infer private roadmap priorities or undocumented stakeholder intent. Missing product context produces `QUESTION`, not an invented finding.

## Scope and proportionality

Evaluate whether the implementation scope is proportional to the established need. Look for duplicate mechanisms, speculative extensibility, premature abstraction, broad refactors attached to narrow needs, maintenance burden, and simpler existing alternatives.

Do not turn this gate into style review or correctness review. If a PR is legitimate but buggy, return `KEEP`; the technical review loop handles correctness.

## Output

The structured result records the exact PR head/base, decision, problem statement, documented needs/evidence, significant technical decisions and their motivation status, existing alternatives, cost/proportionality assessment, consequence of doing nothing, and concise questions for the author when required.
