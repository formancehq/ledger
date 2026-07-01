---
name: antithesis-run
description: Trigger an Antithesis cloud run for ledger by building/pushing per-branch tagged SUT images and delegating the launch to `snouty launch` (the official Antithesis CLI). Default mode is k8s (`basic_k8s_test`, duration in hours, exercises the operator); compose mode (`basic_test`, duration in minutes) is available on request. Use this skill when the user wants to "lance un run antithesis", "trigger antithesis", "fire antithesis", "test sur antithesis", or any phrasing about kicking off a cloud Antithesis test for this repo.
---

# /antithesis-run — Launch an Antithesis cloud run

## Purpose

Antithesis runs are long, expensive, and produce a triage report sent by email. The user fires one when they want their current branch exercised under fault injection.

This skill orchestrates the repo-specific pieces (per-branch tag, dirty-tree guard, manifest substitution, SUT image build/push) and then delegates the actual launch to `snouty launch`. Snouty handles the config-image build/push and the POST to the Antithesis launch API.

## What this skill does NOT do

- Read or parse the resulting triage report — that path is owned by the `antithesis-triage`, `antithesis-debug`, and `antithesis-query-logs` skills (all installed globally via `npx skills add antithesishq/antithesis-skills`).
- Run the local model checker — that is `just test-model` / `just test-model-cluster`, not an Antithesis cloud run.
- Re-implement the launch protocol — that is owned by `snouty`. The skill is a thin orchestrator on top.

## Why `snouty` and not the legacy `just k8s-run` / `just run`

The `tests/antithesis/Justfile` recipes (`k8s-run`, `run`) embed a hand-rolled `curl` against `formance.antithesis.com/api/v1/launch/...`. They predate `snouty`. `snouty launch` is the supported interface for agents (see Antithesis "Using Antithesis with AI" docs) and handles:

- Building and pushing the config image from a `--config <dir>` (containing `docker-compose.yaml` or `manifests/`) automatically.
- Hour→minute conversion (via the `--duration` flag, no `bc` shell-out).
- Auth via `ANTITHESIS_TENANT` + `ANTITHESIS_API_KEY` (preferred) or `ANTITHESIS_USERNAME` + `ANTITHESIS_PASSWORD`.
- Surfacing API errors in a structured form.

The legacy recipes are kept in-tree as a fallback while `snouty` is still <1.0, but every new run should go through `snouty`.

## Workflow

### 1. Preconditions

Run from the repo root (`git rev-parse --show-toplevel`). Verify:

- `tests/antithesis/k8s/` (k8s mode) or `tests/antithesis/config/` (compose mode) exists. If not, abort — wrong repo.
- `snouty --version` succeeds. If not, install per `https://github.com/antithesishq/snouty` and abort.
- These environment variables are set in the shell:
  - `ANTITHESIS_TENANT` (the tenant slug — `formance` for this repo; embedded in the launch URL `formance.antithesis.com`)
  - `ANTITHESIS_API_KEY` — **required** for triage / analysis (`snouty runs …`); the launcher accepts basic auth as a fallback, but every downstream skill (`antithesis-triage`, `antithesis-query-logs`, etc.) needs the API key.
  - `ANTITHESIS_USERNAME` + `ANTITHESIS_PASSWORD` — accepted by `snouty launch`/`snouty debug` only.
  - `ANTITHESIS_REPOSITORY` (registry for SUT images)
  - `ANTITHESIS_REPORT_RECIPIENT` (used as `--recipients`)
  If any are missing, list which and abort. Run `snouty doctor` to surface what's missing in one shot.
- `docker info` succeeds (daemon reachable). If not, abort.

Auth to `$ANTITHESIS_REPOSITORY` is set up once by the user via `docker login`; this skill does not re-do it.

### 2. Detect git state

- `git rev-parse --abbrev-ref HEAD` → branch name. Compute a slug (lowercase, replace `/` and any non-alphanumeric with `-`, truncate to 40 chars).
- `git rev-parse --short=7 HEAD` → SHA7.
- `git status --porcelain` → if non-empty, **the working tree is dirty and those uncommitted changes WILL end up in the image**: `docker build` reads its context from the working tree, not from git HEAD. The per-branch tag still uses the HEAD SHA, so the on-registry artifact will be tagged after a commit it does not actually contain. Surface this loud and ask the user to confirm or commit first. Do NOT proceed silently.

### 3. Compute the tag

```
tag := antithesis-<slug>-<sha7>
```

The shared `antithesis` tag (default in `tests/antithesis/*/Justfile`) is deliberately avoided so concurrent runs do not race on the registry.

### 4. Choose mode

Default to **k8s**. Only switch to compose if the user explicitly asked ("compose mode", "legacy mode", "without operator"). Do not ask routinely — the default covers >99% of runs.

### 5. Ask duration and description

- **Duration**:
  - k8s mode: hours, default `2`. Accept integers or decimals (`1.5`). Validate >0. Convert to minutes before passing to `snouty --duration`.
  - compose mode: minutes, default `30`. Validate >0.
- **Description**: default = `<branch> — <last commit subject>` (use `git log -1 --format=%s HEAD`). Allow the user to override. This string goes into the email report header — keep it meaningful.

  The description is passed verbatim to `snouty --description "<text>"`. Snouty escapes it correctly for the API payload, but the value still ends up in shell argv. **Sanitise** before passing: collapse the value to `[A-Za-z0-9 ._-]` (replace every other byte with `-`) and trim leading/trailing whitespace. If the sanitised string is empty, fall back to the branch slug.

Use a single AskUserQuestion with two questions where possible. If the user said the duration/description in the original request ("lance un run de 4h"), skip the question for that field.

### 6. Build & push SUT images

Snouty's `--config` handles only the config image. The SUT images (ledger, operator, workload) are pushed by us because they reference the registry-specific name resolved at template time.

**K8s mode** — from repo root:

```bash
export TAG=antithesis-<slug>-<sha7>

# 1. Generate manifests with the per-branch tag substituted in.
( cd tests/antithesis/k8s && ./generate-manifests.sh "$TAG" )

# 2. Build & push ledger.
docker build --platform linux/amd64 \
  -f Dockerfile.antithesis \
  -t "$ANTITHESIS_REPOSITORY/${LEDGER_IMAGE_NAME:-ledger}:$TAG" .
docker push "$ANTITHESIS_REPOSITORY/${LEDGER_IMAGE_NAME:-ledger}:$TAG"

# 3. Build & push operator.
docker build --platform linux/amd64 \
  -t "$ANTITHESIS_REPOSITORY/ledger-operator:$TAG" misc/operator
docker push "$ANTITHESIS_REPOSITORY/ledger-operator:$TAG"

# 4. Build & push workload.
( cd tests/antithesis/workload && \
  docker build --platform linux/amd64 \
    --build-arg GOARCH=amd64 --build-arg GOOS=linux \
    -f Dockerfile \
    -t "$ANTITHESIS_REPOSITORY/workload-ledger-v3:$TAG" ../../.. && \
  docker push "$ANTITHESIS_REPOSITORY/workload-ledger-v3:$TAG" )
```

**Compose mode** — same image build/push, plus the etcd retag:

```bash
docker pull --platform linux/amd64 quay.io/coreos/etcd:v3.5.9
docker tag quay.io/coreos/etcd:v3.5.9 "$ANTITHESIS_REPOSITORY/etcd:v3.5.9"
docker push "$ANTITHESIS_REPOSITORY/etcd:v3.5.9"

# Substitute placeholders in docker-compose.yaml into a tmpdir snouty can consume.
TMPCFG=$(mktemp -d)
sed "s/__TAG__/$TAG/g; s/__IMAGE_NAME__/${LEDGER_IMAGE_NAME:-ledger}/g" \
  tests/antithesis/config/docker-compose.yaml > "$TMPCFG/docker-compose.yaml"
```

These steps replace the legacy `just k8s-push-images` / `just push-images`. Build + push is typically 5–15 min on a cold cache.

### 7. Launch with `snouty`

**K8s mode**:

```bash
# Force amd64 for the config image snouty builds internally (see note below).
export DOCKER_DEFAULT_PLATFORM=linux/amd64

snouty launch \
  --webhook basic_k8s_test \
  --config tests/antithesis/k8s \
  --description "<sanitised description>" \
  --duration "<minutes>" \
  --recipients "$ANTITHESIS_REPORT_RECIPIENT" \
  --source "<branch-slug>"
```

Notes:
- **`export DOCKER_DEFAULT_PLATFORM=linux/amd64` before invoking snouty on Apple Silicon.** When you pass `--config <dir>`, snouty builds the config image locally without forcing `--platform linux/amd64`; on an M-series Mac this yields an arm64 image, which Antithesis rejects at boot with `antithesis_error code=4005 "Unsupported container type"` — the run finishes as `Incomplete` at `input_hash=0 vtime=0` about two minutes in. Setting the env var is the smallest fix. Fallback if it does not stick: pre-build the config image manually (`docker build --platform linux/amd64 -f tests/antithesis/k8s/Dockerfile.config -t <ref>:$TAG tests/antithesis/k8s && docker push <ref>:$TAG`) and pass `--config-image <ref>:$TAG` to snouty instead of `--config`.
- **Do NOT pass `--param antithesis.images=...`** — snouty 0.5.x rejects it (`do not specify antithesis.images as --param, use api webhook instead`). In k8s mode the platform reads image refs straight out of the manifests, so the param is unnecessary anyway.
- **Always pass `--source <branch-slug>`** so the run is *not* marked ephemeral; ephemeral runs (`is_ephemeral=true`) do not appear in property history reports. The slug is the same per-branch slug used in the tag.

**Compose mode**:

```bash
snouty launch \
  --webhook basic_test \
  --config "$TMPCFG" \
  --description "<sanitised description>" \
  --duration "<minutes>" \
  --recipients "$ANTITHESIS_REPORT_RECIPIENT" \
  --param custom.duration="<minutes>" \
  --param custom.containers_to_exclude_from_network_faults="etcd-0 etcd-1 etcd-2 workload"
```

Snouty streams its progress to stderr; the resulting POST body (and run identifier, if any) lands on stdout. Pass `--verbose` if the launch errors out and you need to see the raw API request/response.

### 8. Report

Print a structured summary:

```
Antithesis run lancé
  Mode        : k8s (basic_k8s_test) | compose (basic_test)
  Tag         : antithesis-<slug>-<sha7>
  Branche     : <branch> @ <sha7> (dirty: yes/no)
  Durée       : <N> heures | minutes
  Description : <description>
  Rapport     : email à <ANTITHESIS_REPORT_RECIPIENT>
  Snouty      : <stdout from snouty launch>
```

The launch API does not return a stable run URL, so do not invent one. Snouty may print the session ID in its response — surface it as-is if present, it is the handle the user gives to `antithesis-triage`/`antithesis-debug`/`antithesis-query-logs`.

## Invariants

- **Always use `snouty launch`, never re-issue the raw curl.** The legacy `just k8s-run` / `just run` recipes are kept only as a documented fallback; new runs go through `snouty`.
- **Never run silently on a dirty working tree.** `docker build` packs the working tree, so uncommitted edits silently land in the image while the image tag still pins to the HEAD SHA. Always surface and confirm before launching.
- **Never modify `tag := "antithesis"` in `tests/antithesis/*/Justfile`.** The per-branch tag is computed by the skill and threaded through env / args — the in-repo default is only for one-off manual invocations.
- **No retries.** A failed launch (auth, parameter, image push) is the user's to diagnose. Do not loop.
- **English in scripts, replies in the user's language** (typically French for this repo).

## Failure modes worth surfacing

- `snouty doctor` flags `ANTITHESIS_TENANT not set` → add `export ANTITHESIS_TENANT=formance` to the shell profile (e.g. `~/.zshrc` next to the existing `ANTITHESIS_*` vars). After editing, either open a new shell or `source ~/.zshrc` — already-running bash sessions (including any Claude Code agent that started before the edit) do not pick the export up automatically.
- `snouty launch` `401` → `ANTITHESIS_API_KEY` or `ANTITHESIS_PASSWORD` is wrong.
- `snouty launch` `400` with `unknown image` → image push silently failed earlier (re-check the build/push step output).
- `docker push` fails with `no basic auth credentials` / `denied` → not logged in to `$ANTITHESIS_REPOSITORY`; user needs to `docker login <registry>` with the credentials Antithesis provided.
- `helm dependency update` in `generate-manifests.sh` fails → helm is missing or offline; the script will continue with the cached deps if any.
- Build fails with a Go toolchain mismatch → retry with `GOROOT=` in the environment, same as the `develop` skill.

## Out of scope

- Triggering from CI (none exists today; this is a manual operation).
- Listing past runs (snouty does not expose this in 0.5.x; the Antithesis web UI is the source of truth).
- Selecting a workload driver (the workload image bundles all drivers; the manifests in `tests/antithesis/k8s/` and the compose file in `tests/antithesis/config/` select what runs).
