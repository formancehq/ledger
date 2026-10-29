#!/usr/bin/env python3
"""Submit an isolated Pulumi run, or request UID-checked cancellation."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = "ledger-v3"
PROFILES = ("smoke-5m", "smoke-10m", "soak-24h")


def kubectl(*args, capture=False):
    command = ["kubectl", "--context", os.environ.get("PERF_KUBE_CONTEXT", "eks-acme-dev-euw1-01")]
    return subprocess.run(command + list(args), check=True, text=True, capture_output=capture)


def cancel(name, uid, resource_version=None):
    if not re.fullmatch(r"ledger-[a-f0-9]{12}-[1-9][0-9]*-[1-9][0-9]*", name):
        raise ValueError("not a CI benchmark name")
    current = json.loads(kubectl("-n", NAMESPACE, "get", "benchmark", name, "-o", "json", capture=True).stdout)
    metadata = current["metadata"]
    if metadata["uid"] != uid or metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != "ledger-perf":
        raise ValueError("benchmark UID or ownership does not match")
    if resource_version is not None and metadata["resourceVersion"] != resource_version:
        return False
    # The API server evaluates the UID test atomically with the cancellation.
    patch = [{"op": "test", "path": "/metadata/uid", "value": uid},
             {"op": "add", "path": "/spec/cancel", "value": True}]
    if resource_version is not None:
        patch.insert(1, {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version})
    try:
        kubectl("-n", NAMESPACE, "patch", "benchmark", name, "--type=json", "-p", json.dumps(patch))
    except subprocess.CalledProcessError:
        if resource_version is None:
            raise
        latest = json.loads(kubectl("-n", NAMESPACE, "get", "benchmark", name, "-o", "json", capture=True).stdout)
        if latest["metadata"]["uid"] == uid and latest["metadata"]["resourceVersion"] != resource_version:
            return False  # Reconciled since observation. Preserve it, including a run that just started.
        raise
    return True


def preflight():
    crd = json.loads(kubectl("get", "crd", "benchmarks.benchmark.formance.com", "-o", "json", capture=True).stdout)
    version = next(v for v in crd["spec"]["versions"] if v["name"] == "v1alpha1" and v["served"])
    fields = version["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
    required = {"executionGroup", "activeDeadlineSeconds", "whenBusy", "queueDeadlineSeconds", "cancel"}
    if not required.issubset(fields):
        raise ValueError("benchmark-operator lifecycle CRD must be upgraded before CI submission")


def supersede_pending(pr, run_id, attempt):
    if pr == "0":
        return
    selector = f"app.kubernetes.io/managed-by=ledger-perf,benchmark.formance.com/pr={pr}"
    items = json.loads(kubectl("-n", NAMESPACE, "get", "benchmarks", "-l", selector, "-o", "json", capture=True).stdout)["items"]
    for item in items:
        status = item.get("status", {})
        if status.get("phase", "") not in ("", "Pending", "Queued") or status.get("executionStartedAt"):
            continue
        annotations = item["metadata"].get("annotations", {})
        old_run = annotations.get("benchmark.formance.com/run-id", "")
        old_attempt = annotations.get("benchmark.formance.com/run-attempt", "")
        if old_run.isdecimal() and old_attempt.isdecimal() and (int(old_run), int(old_attempt)) < (int(run_id), int(attempt)):
            cancel(item["metadata"]["name"], item["metadata"]["uid"], item["metadata"]["resourceVersion"])


def submit(profile, sha, image, run_id, attempt, pr):
    if profile not in PROFILES or not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("invalid profile or commit SHA")
    if not all(re.fullmatch(r"[1-9][0-9]*", n) for n in (run_id, attempt)):
        raise ValueError("run ID and attempt must be positive integers")
    if not re.fullmatch(r"0|[1-9][0-9]*", pr):
        raise ValueError("invalid PR number")
    if not re.fullmatch(r"[a-z0-9./_-]+:[A-Za-z0-9_.-]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("image must include a tag and immutable digest")
    if "-rc" in image.split("@", 1)[0].rsplit(":", 1)[1]:
        raise ValueError("CI image tags must not use the RC upgrade contract")
    name = f"ledger-{sha[:12]}-{run_id}-{attempt}"
    if len(name) > 48:
        raise ValueError("run identity is too long")
    when_busy = os.environ.get("PERF_WHEN_BUSY", "Skip" if profile == "soak-24h" else "Queue")
    if when_busy not in ("Queue", "Skip"):
        raise ValueError("PERF_WHEN_BUSY must be Queue or Skip")
    workload_commit = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    if not re.fullmatch(r"[a-f0-9]{40}", workload_commit):
        raise ValueError("invalid workload checkout SHA")
    preflight()
    supersede_pending(pr, run_id, attempt)

    # Copy only program inputs. Never copy or overwrite user stack configs.
    run_dir = ROOT / "build" / "perf" / "runs" / name
    project = run_dir / "project"
    project.mkdir(parents=True, exist_ok=False)
    source = ROOT / "tests" / "perf"
    for path in source.glob("*.go"):
        if not path.name.endswith("_test.go"):
            shutil.copy2(path, project / path.name)
    for filename in ("go.mod", "go.sum"):
        shutil.copy2(source / filename, project / filename)
    for directory in ("scripts", "profiles"):
        shutil.copytree(source / directory, project / directory)
    (project / "Pulumi.yaml").write_text(json.dumps({
        "name": "perf", "runtime": {"name": "go", "options": {"buildTarget": "../pulumi"}}
    }))
    state_base = Path(os.environ.get("PERF_STATE_DIR", os.environ.get("RUNNER_TEMP", str(run_dir)))).resolve()
    state = state_base / "pulumi-state" / name
    state.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PULUMI_HOME"] = str(state / "home")
    env["PULUMI_BACKEND_URL"] = state.as_uri()
    env["PULUMI_CONFIG_PASSPHRASE"] = secrets.token_urlsafe(32)
    env.pop("PULUMI_CONFIG_PASSPHRASE_FILE", None)
    env["PULUMI_SKIP_UPDATE_CHECK"] = "true"
    # No credentials are recorded in this run's Pulumi configuration.
    def pulumi(*args):
        subprocess.run(["pulumi", *args, "--non-interactive"], cwd=project, env=env, check=True)

    pulumi("login", env["PULUMI_BACKEND_URL"])
    pulumi("stack", "init", name, "--secrets-provider", "passphrase")
    values = {"profile": profile, "commit": sha, "image": image, "run-id": run_id,
              "run-attempt": attempt, "pr": pr, "when-busy": when_busy, "workload-commit": workload_commit,
              "k8s-context": os.environ.get("PERF_KUBE_CONTEXT", "eks-acme-dev-euw1-01")}
    for key, value in values.items():
        pulumi("config", "set", f"perf:{key}", value, "--plaintext")
    pulumi("up", "--yes", "--skip-preview")
    print(f"Submitted Benchmark {NAMESPACE}/{name}; execution and snapshots are owned by Kubernetes.")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"\n### Benchmark submitted\n\n- Benchmark: `{NAMESPACE}/{name}`\n"
                      f"- Commit: `{sha}`\n- Profile: `{profile}`\n- Image: `{image}`\n"
                      "\nThis job confirms submission only. Consult Grafana for snapshots.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    run = actions.add_parser("submit")
    for name in ("profile", "sha", "image", "run_id", "attempt", "pr"):
        run.add_argument(name)
    stop = actions.add_parser("cancel")
    stop.add_argument("name")
    stop.add_argument("uid")
    args = vars(parser.parse_args())
    action = args.pop("action")
    try:
        (submit if action == "submit" else cancel)(**args)
    except (ValueError, KeyError, StopIteration, subprocess.CalledProcessError) as error:
        parser.exit(1, f"perf: {error}\n")


if __name__ == "__main__":
    main()
