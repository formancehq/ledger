---
name: antithesis-run
description: Trigger an Antithesis cloud run for ledger-v3-poc by building and pushing per-branch tagged images to ECR and POSTing to the Antithesis launch API. Default mode is k8s (`just k8s-run`, duration in hours, exercises the operator); compose mode (`just run`, duration in minutes) is available on request. Use this skill when the user wants to "lance un run antithesis", "trigger antithesis", "fire antithesis", "test sur antithesis", or any phrasing about kicking off a cloud Antithesis test for this repo.
---

# /antithesis-run — Launch an Antithesis cloud run

## Purpose

Antithesis runs are long, expensive, and produce a triage report sent by email. The user fires one when they want their current branch exercised under fault injection. The skill wraps `tests/antithesis/Justfile`'s `k8s-run` / `run` recipes, computes a per-branch image tag so concurrent runs do not clobber each other on ECR, and surfaces the launch result.

## What this skill does NOT do

- Read or parse the resulting triage report (Antithesis emails it; that path is owned by `antithesis-debug` and `antithesis-query-logs`).
- Re-implement the build / push pipeline — it shells out to the existing justfile recipes.
- Run the local model checker — that is `just test-model` / `just test-model-cluster`, not an Antithesis cloud run.

## Workflow

### 1. Preconditions

Run from the repo root (`git rev-parse --show-toplevel`). Verify:

- `tests/antithesis/Justfile` exists. If not, abort — wrong repo.
- These environment variables are set in the shell: `ANTITHESIS_PASSWORD`, `ANTITHESIS_REPOSITORY`, `ANTITHESIS_REPORT_RECIPIENT`. The `tests/antithesis/Justfile` does not load `.env`, so they must come from the user's shell (typically via direnv or an exported profile). If any are missing, list which and abort.
- `docker info` succeeds (daemon reachable). If not, abort.

Note: images are pushed to `$ANTITHESIS_REPOSITORY` (the registry Antithesis provides for this tenant) — there is no AWS / ECR step. Auth to that registry is set up once by the user via `docker login`; this skill does not re-do it.

### 2. Detect git state

- `git rev-parse --abbrev-ref HEAD` → branch name. Compute a slug (lowercase, replace `/` and any non-alphanumeric with `-`, truncate to 40 chars).
- `git rev-parse --short=7 HEAD` → SHA7.
- `git status --porcelain` → if non-empty, **the working tree is dirty and the image will not contain the uncommitted changes**. Surface this loud and ask the user to confirm or commit first. Do NOT proceed silently.

### 3. Compute the tag

```
tag := antithesis-<slug>-<sha7>
```

The shared `antithesis` tag (default in `tests/antithesis/Justfile`) is deliberately avoided so concurrent runs do not race on ECR.

### 4. Choose mode

Default to **k8s**. Only switch to compose if the user explicitly asked ("compose mode", "legacy mode", "without operator"). Do not ask routinely — the default covers >99% of runs.

### 5. Ask duration and description

- **Duration**:
  - k8s mode: hours, default `2`. Accept integers or decimals (`1.5`). Validate >0.
  - compose mode: minutes, default `30`. Validate >0.
- **Description**: default = `<branch> — <last commit subject>` (use `git log -1 --format=%s HEAD`). Allow the user to override. This string goes into the email report header — keep it meaningful.

Use a single AskUserQuestion with two questions where possible. If the user said the duration/description in the original request ("lance un run de 4h"), skip the question for that field.

### 6. Launch

From the repo root, change into `tests/antithesis/`:

```bash
cd tests/antithesis
just tag=<computed-tag> k8s-run <duration_hours> "<description>"
# or, in compose mode:
just tag=<computed-tag> run <duration_minutes> "<description>"
```

The recipe builds, pushes, then POSTs. Stream the output. Build + push is typically 5–15 min; the API POST itself is sub-second.

If the curl fails (non-2xx), surface the response body — the API returns useful error JSON on auth / parameter issues.

### 7. Report

Print a structured summary:

```
Antithesis run lancé
  Mode        : k8s (basic_k8s_test) | compose (basic_test)
  Tag         : antithesis-<slug>-<sha7>
  Branche     : <branch> @ <sha7> (dirty: yes/no)
  Durée       : <N> heures | minutes
  Description : <description>
  Rapport     : email à <ANTITHESIS_REPORT_RECIPIENT> (variable lue depuis le shell ; le rapport arrive typiquement sous quelques heures)
```

The launch API does not return a run URL in the response, so do not invent one. The user knows their inbox.

## Invariants

- **Never modify `tag := "antithesis"` in `tests/antithesis/Justfile`.** Pass the tag override via the CLI (`just tag=...`) — never edit the recipe file.
- **Never run silently on a dirty working tree.** The image is built from HEAD; uncommitted work is invisible to Antithesis. Always surface and confirm.
- **No retries.** A failed launch (auth, parameter, image push) is the user's to diagnose. Do not loop.
- **English in scripts, replies in the user's language** (typically French for this repo).

## Failure modes worth surfacing

- `401` from the launch API → `ANTITHESIS_PASSWORD` is wrong.
- `400` with `unknown image` → image push silently failed earlier (check the build/push step output).
- `docker push` fails with `no basic auth credentials` / `denied` → not logged in to `$ANTITHESIS_REPOSITORY`; user needs to `docker login <registry>` with the credentials Antithesis provided.
- Build fails with a Go toolchain mismatch → retry with `GOROOT=` in the environment, same as the `develop` skill.

## Out of scope

- Triggering from CI (none exists today; this is a manual operation).
- Listing past runs (not exposed by the launch API; that lives in the Antithesis web UI).
- Selecting a workload driver (today the workload image bundles all drivers; the test config in `tests/antithesis/config/` and `tests/antithesis/k8s/` selects what runs).
