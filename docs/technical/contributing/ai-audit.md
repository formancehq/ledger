# AI deep-audit contract

This contract defines repository-wide audit campaigns. It is intentionally separate from the PR review loop:

- PR review asks whether a bounded change is safe to integrate;
- deep audit asks which latent defects may already exist in an immutable repository state.

An audit is read-only. It must never fix code, create commits, push branches, create issues, resolve review threads, or change GitHub metadata.

## Audit target

Every result is bound to:

- the exact repository `HEAD` SHA;
- one named audit domain manifest under `docs/technical/audits/`;
- the complete scope and invariants declared by that manifest.

The auditor must inspect the current code, tests, and relevant technical documentation. It must not infer that green CI or existing tests prove an invariant.

## Evidence standard

Audit findings are held to a higher evidence standard than ordinary review suggestions. Every finding must include:

- a stable id in the form `<audit-id>/<short-kebab-case-name>`, unique within the report;
- severity `P0`, `P1`, `P2`, or `P3`;
- an exact location when possible;
- the violated invariant or expected property;
- a concrete failure path;
- impact;
- evidence from the repository;
- a reproduction plan or executable test idea;
- the existing test gap that allowed the defect to survive, when identifiable;
- confidence `HIGH`, `MEDIUM`, or `LOW`.

`P0`, `P1`, and `P2` findings must not be speculative. They need a concrete code path, violated invariant, or reproducible state transition. If the intended behavior cannot be determined from repository evidence, report an audit question rather than inventing a defect.

## Audit questions

Questions are explicit uncertainties that need human/product/architecture input. They are not findings and must not be counted as defects.

## Duplicate and challenge pass

A future campaign orchestrator may run independent auditors and challenge findings. The first version deliberately does not auto-deduplicate or auto-fix. A finding becomes a confirmed engineering defect only after reproduction, an independent confirming review, or human validation.

## Output

`bash scripts/ai-audit <domain>` writes a structured JSON report. The report records the audited HEAD, manifest id, findings, questions, inspected areas, and residual risk.

Publication is atomic and anchored to the validated destination directory. A concurrent rename or symlink substitution of the output path must not redirect the final write into repository content.

The audit command exits non-zero when the provider or output contract fails. The presence of findings itself is not an execution failure.
