# Code quality audits

This workflow reviews maintainability and readability separately from the
[deep correctness audit](ai-audit.md). It does not use `scripts/ai-audit`, its
defect schema, or Jira publication. A quality observation is a proposal for a
future change, not proof of a product defect.

## Preparing a review

Choose a bounded subsystem and an exact clean commit. Read its architecture,
callers, tests and [code conventions](conventions.md). State a concrete
maintenance task that a contributor would need to perform, such as adding an
order type or changing a verification rule. Measure file size, call depth or
duplication only to select places to inspect. A threshold alone is not a
finding. Check current branches and PRs so the proposal does not duplicate
active work.

## Evidence for each proposal

Record:

1. The exact files, symbols, and dependency or data-flow boundary involved.
2. A realistic change scenario and the specific reading, edit, or test burden.
3. The smallest refactoring alternative, including what responsibility moves
   and which interface remains stable.
4. Behavior-preservation risks, relevant tests and an incremental validation
   plan. Say which checks ran and which are only proposed.
5. The repository convention that applies, or a clearly identified trade-off
   when no convention settles the choice.

Prefer a ranked list of a few independent proposals. Label uncertain ownership
or intended design as a question. Do not call code “unclean,” “DIY,” or
overengineered without identifying the maintenance consequence and a smaller
alternative. Do not recommend a new abstraction solely to reduce line count.
Challenge each proposal by checking existing encapsulation, performance and
determinism constraints, and whether the suggested split merely moves coupling.

## First pilot: checker and admission

The first review is scoped by
[checker and admission](../quality-audits/checker-admission.md). It may start
after this contract is reviewed and merged, against a later exact clean HEAD.
Report quality proposals separately from correctness findings. A confirmed
correctness defect discovered incidentally follows the owning correctness
domain and its qualification workflow; do not relabel it as a style issue.
