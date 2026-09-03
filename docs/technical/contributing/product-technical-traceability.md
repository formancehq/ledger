# Product to technical traceability

Every significant technical decision must be traceable to a concrete product or operational need. The purpose is not process for its own sake; it is to make architecture explainable, reviewable, and auditable without requiring reviewers to reconstruct intent from code or private context.

The canonical chain is:

```text
product / operational need
    -> requirement / constraint
    -> technical decision
    -> implementation
    -> validation
```

## When traceability is required

Traceability is required for decisions that materially change behavior, architecture, operational characteristics, or long-term maintenance cost. Typical examples include:

- API or semantic changes;
- persistence, storage, schema, snapshot, restore, replay, or migration decisions;
- cache, consistency, retry, idempotency, admission, ordering, or concurrency strategies;
- new abstractions, subsystems, services, dependencies, or distributed-system mechanisms;
- compatibility or rollout strategy changes;
- performance/availability tradeoffs that change system behavior;
- meaningful increases in implementation or operational complexity.

Mechanical maintenance does not require an artificial product rationale. Examples: typo fixes, dead import removal, behavior-preserving renames, generated-file refreshes, and narrow test-helper maintenance.

When classification is ambiguous, document the motivation rather than silently assuming it is exempt.

## Required chain

For every significant technical decision, repository evidence should answer:

### 1. Product or operational need

What user, customer, operator, reliability, compatibility, or business outcome is required?

A preference such as `cleaner`, `more generic`, `future-proof`, or `best practice` is not a need by itself.

### 2. Current limitation

What observable limitation, failure mode, missing capability, cost, or constraint makes the current behavior insufficient now?

Where practical, point to an issue, incident, specification, test, metric, API contract, support case, or authoritative repository documentation.

### 3. Requirement / constraint

Translate the need into an observable system requirement. State what must be true, not how to implement it.

Examples:

- an acknowledged command must survive leader change;
- restore must reproduce the same durable projections as live apply;
- a bulk operation must remain atomic within its documented scope;
- a request larger than the supported product limit must fail deterministically.

### 4. Technical decision

State the chosen mechanism and why it is proportionate to the requirement now.

Record materially credible alternatives when they existed, including the simplest option of doing nothing. Do not invent alternatives only to fill a template.

### 5. Implementation

Identify where the decision is implemented and where its durable intent is committed. For large decisions this may be an ADR/design document plus several PRs. Smaller decisions do not require a separate ADR: the same PR may add a concise explanation to the owning subsystem documentation, an API or behavior contract, or a relevant code comment.

### 6. Validation

Define how we know the original requirement is satisfied. Prefer tests, model checks, scenarios, metrics, compatibility checks, or other observable evidence over implementation assertions.

## Where the evidence lives

The repository is the durable source of technical intent. Jira, support cases, incidents, customer requests, or roadmap items may establish the need, but significant technical decisions must leave enough evidence in-repo for a future reviewer or auditor to understand the requirement and decision without private conversation history.

A PR description may satisfy its authoring role by linking an authoritative existing committed document instead of duplicating it. External issues, specifications, or incidents may establish the need, but the committed repository evidence must retain enough requirement and decision context to remain understandable without those private or mutable sources. Do not copy large product specifications into PR descriptions.

For a significant change with no existing committed evidence, add or update durable evidence in the same PR. This can be the owning subsystem documentation, a focused decision document, an API or behavior contract, or a relevant code comment. The PR description should summarize the chain and point to that committed path, but it is not the sole durable record.

Use this minimum PR-description summary:

```markdown
## Product / operational motivation

Need: <required outcome>
Current limitation: <why current behavior is insufficient>
Requirement / constraint: <observable property that must hold>
Evidence: <issue/spec/incident/test/doc links or repository paths>
Durable repository evidence: <committed documentation, contract, or code-comment path>

## Technical decision

Decision: <mechanism chosen>
Why now / why proportionate: <reason>
Alternatives considered: <material alternatives, including do-nothing when relevant>

## Validation

The requirement is satisfied when: <observable validation>
```

## AI behavior

AI reviewers and audit agents must never manufacture missing motivation.

- If the change is mechanical, record that traceability is not required.
- If a significant decision has a complete chain, use it as authoritative intent subject to consistency checks against code and other authoritative sources.
- If the need is evident but the chain is incomplete, ask for the missing link.
- If product/operational intent cannot be established from accessible evidence, return a human-decision/question outcome rather than rationalizing the implementation after the fact.
- If code contradicts the documented requirement, treat that as a correctness concern; do not silently redefine the requirement to match the implementation.

## Review and audit use

Technical review uses this contract to evaluate significant decisions against established intent.

Technical review uses it to distinguish implementation correctness from design preference and to evaluate whether tests validate the actual requirement.

Deep audits use it to distinguish latent defects from unusual-but-intentional behavior and to identify decisions whose original requirements are no longer documented.

Traceability is not a substitute for engineering judgment. It makes the inputs to that judgment explicit and durable.
