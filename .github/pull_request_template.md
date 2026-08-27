## What changed

<!-- Describe the change in 2–5 lines. Focus on behavior and scope, not implementation history. -->

## Why

<!-- What problem does this solve? Link the issue/spec when relevant. -->

## Product / operational motivation

<!-- Required for significant technical decisions; otherwise write N/A (mechanical change).
Need: required user/customer/operator/reliability outcome.
Current limitation: why existing behavior is insufficient now.
Requirement / constraint: observable property that must hold.
Evidence: issue/spec/incident/test/doc link or repository path.
Durable repository evidence: committed documentation, contract, or code-comment path; the PR description alone is not sufficient. -->

N/A

## Technical decision

<!-- Required when Product / operational motivation is not N/A.
Decision: mechanism chosen.
Why now / why proportionate: why this complexity is justified now.
Alternatives considered: material alternatives, including do-nothing when relevant. -->

N/A

## Risk

<!-- Choose one and briefly justify MEDIUM/HIGH. -->

**LOW | MEDIUM | HIGH**

## Validation

<!-- Keep only checks that actually ran. Add targeted tests when relevant.
For significant decisions, state what observable evidence proves the requirement above. -->

- [ ] `bash scripts/agent-check`
- [ ] Targeted tests: N/A
- [ ] Full suite / broader validation: N/A

## Architecture / behavior impact

<!-- N/A if none. Otherwise call out persisted state, FSM/Raft, API, storage, compatibility, or operational impact. -->

N/A

## Review focus

<!-- Tell reviewers where judgment is most valuable. Avoid generic "please review everything". -->

## Known concerns

<!-- None, or list unresolved risks/tradeoffs explicitly. Do not hide a known concern behind passing CI. -->

None
