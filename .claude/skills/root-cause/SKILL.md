---
name: root-cause
description: Root-cause investigation discipline. Never patch the symptom — trace the causal chain to its origin, fix at the source, sweep for the same class of bug, and add prevention so it cannot recur. Use when the user says "investigate", "root cause", "RCA", "corrige à la source", "va au fond du problème", "pourquoi ça arrive", or when a bug smells like a symptom of something deeper (recurring bug, flaky behavior, "just add a nil check" temptation).
---

# Root Cause

A discipline for fixing problems at their source instead of where they explode. The output is not just a fix: it is a verified causal chain, a source-level fix, a class sweep, and a prevention measure.

**Companion skill:** when you need a reproduction or a feedback loop to test hypotheses, use the `diagnose` skill (Phase 1–4). This skill governs *how deep to go* and *what counts as done*; `diagnose` governs *how to find the mechanism*.

## The prime directive

The line where the error appears is almost never the line where the problem lives. Before writing any fix, you must be able to state:

> "The system's intent was X. Reality diverged from X at point P, because of decision/assumption D. Everything downstream of P is a symptom."

If you cannot fill in P and D with evidence, you are not done investigating.

### Forbidden moves (symptom patches)

These are red flags, not fixes. Each is only acceptable as a *labeled mitigation* (see below), never as the resolution:

- Adding a nil/undefined check where the crash happened, without knowing why the value was nil.
- Wrapping in try/catch (or `recover`, or `_ = err`) to make the error disappear.
- Adding a retry, a sleep, or bumping a timeout to make flakiness go away.
- Special-casing the failing input ("if id == 42, skip").
- Reordering statements until the test passes, without explaining why order mattered.
- Silencing the branch: `return nil` / `continue` on a path that "shouldn't happen". (In this repo that violates CLAUDE.md invariant 7 — surface it loudly with `fmt.Errorf("invariant: ...")` or `assert.Unreachable` instead; a silent no-op in the FSM apply path desyncs nodes.)
- Reverting a commit without understanding what in it was wrong. Reverting is a fine *mitigation*; it is not an *explanation*.

If you catch yourself doing one of these, stop and say so: "this would patch the symptom; the source is still unknown."

## Phase 1 — Establish the facts

Before any theory, collect what is actually known. Facts, not interpretations:

- **Exact symptom.** The literal error message, wrong value, or timing — copy it, don't paraphrase. "It crashes" is not a fact; the stack trace is.
- **Blast radius.** Who/what is affected? One user or all? One node or the cluster? One code path or several? The shape of the blast radius often identifies the layer of the cause.
- **Timeline.** When did it start? Was it ever correct? `git log` / `git blame` the area, check deploys, config changes, dependency bumps, data migrations around the onset. "It worked before X" is the single most valuable fact in any investigation.
- **What is *not* broken.** The sibling code path that works fine is a natural control group — the diff between working and broken narrows the search massively.

Write these down (a short facts block in your response or a scratch file). Every hypothesis in Phase 2 must be consistent with *all* the facts, not just the loudest one.

## Phase 2 — Trace the causal chain backwards

Walk from the symptom toward the origin, one verified link at a time. This is "5 Whys" with a hard rule attached:

**Every "because" must be verified with evidence before you ask the next "why".** Read the code, run the repro, inspect the state, check the log. A causal chain with one assumed link is worth nothing — the chain is only as strong as its weakest "I think".

```
Symptom:  API returns 500 on POST /transactions
  why? →  FSM apply returned ErrCacheMiss            [verified: log line]
  why? →  the key was never preloaded                [verified: preload.Needs lacks the key]
  why? →  the new proposal type declared no Needs    [verified: code review of the emitter]
  why? →  nothing forces an emitter to declare Needs [← root cause: missing enforcement]
```

Stop conditions — you have reached the root cause when the next "why" leaves the system you can change (e.g. "why does the OS do X"), or when the answer is a *decision or missing rule* rather than a code path. The root cause is usually one of:

- **A violated contract/invariant** — some code assumed a guarantee nobody actually provides.
- **A missing enforcement** — the rule exists in someone's head or in a doc, but nothing (compiler, linter, test, assert) makes it real.
- **A wrong design assumption** — the model of the world the code encodes stopped matching reality.
- **A process gap** — the change that introduced the bug could not have been caught by anything that exists.

"Developer made a typo" is never a root cause. Humans make typos at a constant rate; the root cause is whatever allowed the typo to reach production.

**Distinguish the three levels explicitly in your write-up:**
1. *Immediate cause* — the mechanism that produced the symptom (the nil, the race, the off-by-one).
2. *Contributing causes* — conditions that made it possible or hid it (dead test, swallowed error, misleading name).
3. *Root cause* — the origin decision/gap. This is the one the fix must target.

## Phase 3 — Fix at the source

The fix goes at point P — where reality first diverged from intent — not where the stack trace ends.

- If the source fix is small: do it. Delete the downstream defensive scar tissue the symptom accumulated (the old nil-checks and retries that were masking it), so the code tells the truth again.
- If the source fix is large or risky: a **mitigation + debt** split is acceptable, but only explicitly. The mitigation must be labeled in code (`// MITIGATION: masks <root cause>, tracked in <ticket>`) and the follow-up must be real (ticket created, or clearly handed to the user). A silent mitigation is a symptom patch with better marketing.
- If the fix and the root cause are at different layers (bug fixed in code, but root cause is "nothing enforces this"), the fix is not complete until Phase 5 addresses the enforcement gap.

Verify against the original symptom from Phase 1 — the exact one, not a nearby one. Re-run the repro/feedback loop if one exists (see `diagnose`).

## Phase 4 — Sweep for the class

A root cause almost never has exactly one instance. Before closing:

- **Search for the same pattern elsewhere.** Grep/LSP for the same misused API, the same violated assumption, the same copy-pasted block. If the bug was "caller X forgot to declare Y", audit every caller.
- **Check the neighbors in time.** If the bug came in with commit C, review the rest of C and its siblings — the same author under the same misunderstanding usually made the same mistake more than once.
- Fix (or list) every instance found. "I fixed the one that was reported" is symptom-level thinking applied at the codebase scale.

## Phase 5 — Prevention: make the class impossible or loud

Choose the strongest feasible rung on this ladder — each rung down is weaker:

1. **Eliminate** — change types/APIs so the wrong state cannot be expressed (compiler-enforced; e.g. a write-only session type that has no `Get`).
2. **Enforce** — a linter rule (`forbidigo`, custom analyzer), a CI check, a runtime `assert.Unreachable` that fails loudly.
3. **Detect** — a regression test that exercises the real bug pattern at a correct seam (see `diagnose` Phase 5 on seams), a checker/verifier pass, an alert.
4. **Document** — an invariant written where the next person will actually read it (CLAUDE.md, an ADR, a comment stating *why* the impossible case is impossible).

Documentation alone is a last resort — if you land on rung 4, say why rungs 1–3 were not feasible. If the honest answer is "the architecture has no seam for this", surface that finding to the user as proposed follow-up work — it is more valuable than the bug fix itself.

## Phase 6 — Deliverable: the RCA summary

Close with a short write-up (in the PR/commit message, or to the user). Required sections:

- **Symptom** — what was observed (verbatim).
- **Causal chain** — the verified why-chain, each link with its evidence.
- **Root cause** — the origin decision/gap, stated in one sentence.
- **Fix** — what changed and why it targets the source; any labeled mitigations + their follow-ups.
- **Class sweep** — what was searched, what else was found, what was done about it.
- **Prevention** — which rung of the ladder was added, and what would now catch a recurrence.

If any section is empty, the investigation is not finished — say which one and why.

## Calibration

Not every bug deserves the full ceremony. A typo in a log message needs a one-line fix, not an RCA. Scale by *recurrence risk*: the more likely the same class of mistake is to happen again (shared pattern, many call sites, invariant-adjacent, already happened twice), the further down the phases you must go. When in doubt, at minimum do Phase 2 (verified chain) and Phase 4 (class sweep) — they are cheap and catch most of the value.
