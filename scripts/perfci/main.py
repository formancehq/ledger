#!/usr/bin/env python3
"""Trusted CI admission for asynchronous acme-dev benchmarks.

An image artifact is data, never executable input. All cluster operations use
the default branch's Pulumi program and profiles after this admission succeeds.
"""

import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile


PROFILES = {"perf-5m": "smoke-5m", "perf-10m": "smoke-10m"}
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
REPOSITORY = re.compile(r"ghcr\.io/[a-z0-9][a-z0-9_.-]*(?:/[a-z0-9][a-z0-9_.-]*)+\Z")
MAX_JSON = 1024 * 1024
MAX_ARTIFACT = 64 * 1024


class AdmissionError(ValueError):
    """The request cannot safely be submitted."""


def require(condition, message):
    if not condition:
        raise AdmissionError(message)


def positive(value, name, allow_zero=False):
    require(re.fullmatch(r"[0-9]+", str(value)) is not None, f"invalid {name}")
    number = int(value)
    require(number >= (0 if allow_zero else 1), f"invalid {name}")
    return number


def image_repository():
    repository = os.environ.get("IMAGE_REPOSITORY", "ghcr.io/formancehq/ledger")
    require(REPOSITORY.fullmatch(repository) is not None, "invalid trusted image repository")
    return repository


def outputs(**values):
    # No caller may introduce another GitHub output through embedded newlines.
    for value in values.values():
        require("\n" not in str(value) and "\r" not in str(value), "multiline CI output")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        redirected = super().redirect_request(request, fp, code, message, headers, new_url)
        if redirected is not None:
            # Artifact downloads redirect to a signed storage URL. Never send
            # the GitHub token to that other host.
            redirected.remove_header("Authorization")
        return redirected


def bounded_read(response, maximum):
    content = response.read(maximum + 1)
    require(len(content) <= maximum, "GitHub response exceeds size limit")
    return content


class GitHub:
    def __init__(self):
        self.repository = os.environ["GITHUB_REPOSITORY"]
        self.api = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
        self.opener = urllib.request.build_opener(SafeRedirect())

    def request(self, suffix, maximum=MAX_JSON):
        request = urllib.request.Request(
            f"{self.api}/repos/{self.repository}/{suffix}",
            headers={
                "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with self.opener.open(request, timeout=30) as response:
            return bounded_read(response, maximum)

    def json(self, suffix):
        return json.loads(self.request(suffix))


def metadata_from_zip(content):
    require(len(content) <= MAX_ARTIFACT, "image artifact exceeds size limit")
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        entries = archive.infolist()
        require(len(entries) == 1, "image artifact must contain exactly one file")
        entry = entries[0]
        require(entry.filename == "image-metadata.json", "unexpected image artifact file")
        require(not stat.S_ISLNK(entry.external_attr >> 16), "image artifact cannot be a symlink")
        require(entry.file_size <= 4096, "image metadata exceeds size limit")
        require(entry.compress_size <= MAX_ARTIFACT, "compressed image metadata exceeds size limit")
        # Read the one entry in memory. No archive-controlled path reaches disk.
        return json.loads(archive.read(entry))


def validate_metadata(metadata, run, repository, allowed_image_repository):
    require(isinstance(metadata, dict), "image metadata must be an object")
    sha = metadata.get("sha", "")
    require(isinstance(sha, str) and SHA.fullmatch(sha) is not None, "invalid image commit")
    pr = positive(metadata.get("pr", ""), "PR number")
    require(metadata.get("repository") == repository, "image repository provenance mismatch")
    require(metadata.get("runId") == run["id"], "image workflow run mismatch")
    require(metadata.get("runAttempt") == run["run_attempt"], "image workflow attempt mismatch")
    require(sha == run["head_sha"], "image workflow commit mismatch")
    require(any(item.get("number") == pr for item in run.get("pull_requests", [])),
            "image workflow is not associated with this PR")
    expected_image = f"{allowed_image_repository}:pr-{pr}-{sha[:7]}"
    require(metadata.get("image") == expected_image, "image reference is not the expected PR tag")
    digest = metadata.get("digest", "")
    require(isinstance(digest, str) and DIGEST.fullmatch(digest) is not None, "invalid image digest")
    return {"sha": sha, "pr": pr, "image": f"{expected_image}@{digest}"}


def pr_profile(pr, repository, sha):
    if pr.get("state") != "open" or pr.get("head", {}).get("repo", {}).get("full_name") != repository:
        return None
    if pr.get("base", {}).get("repo", {}).get("full_name") != repository or pr["head"]["sha"] != sha:
        return None
    profiles = [PROFILES[label["name"]] for label in pr.get("labels", []) if label["name"] in PROFILES]
    require(len(profiles) <= 1, "choose exactly one of perf-5m and perf-10m")
    return profiles[0] if profiles else None


def prepare_pr():
    github = GitHub()
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    source_id = positive(event["workflow_run"]["id"], "source workflow run")
    run = github.json(f"actions/runs/{source_id}")
    require(run.get("repository", {}).get("full_name") == github.repository,
            "source workflow repository mismatch")
    require(run.get("head_repository", {}).get("full_name") == github.repository,
            "fork image workflows cannot submit benchmarks")
    require(run.get("path") == ".github/workflows/pr-images.yml" and run.get("name") == "PR Images",
            "unexpected source workflow")
    require(run.get("event") == "pull_request", "source workflow must be a PR build")
    require(run.get("status") == "completed" and run.get("conclusion") == "success",
            "source image workflow did not succeed")
    artifacts = github.json(f"actions/runs/{source_id}/artifacts?per_page=100")["artifacts"]
    artifact_name = f"perf-image-{positive(run['run_attempt'], 'source workflow attempt')}"
    matches = [artifact for artifact in artifacts if artifact["name"] == artifact_name and not artifact["expired"]]
    if not matches:
        # A filtered PR Images job can finish successfully without building.
        outputs(enabled="false")
        return
    require(len(matches) == 1, "ambiguous image artifacts")
    artifact = matches[0]
    require(artifact["size_in_bytes"] <= MAX_ARTIFACT, "image artifact exceeds size limit")
    content = github.request(f"actions/artifacts/{positive(artifact['id'], 'artifact id')}/zip", MAX_ARTIFACT)
    admission = validate_metadata(metadata_from_zip(content), run, github.repository, image_repository())
    pr = github.json(f"pulls/{admission['pr']}")
    profile = pr_profile(pr, github.repository, admission["sha"])
    if profile is None:
        outputs(enabled="false")
        return
    outputs(enabled="true", profile=profile, **admission)


def prepare_main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    profile = "soak-24h" if os.environ["GITHUB_EVENT_NAME"] == "schedule" else event["inputs"]["profile"]
    require(profile in {*PROFILES.values(), "soak-24h"}, "invalid benchmark profile")
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    require(SHA.fullmatch(sha) is not None, "invalid default branch commit")
    # PR number zero is reserved for trusted default-branch benchmark builds.
    outputs(sha=sha, image=f"{image_repository()}:pr-0-{sha[:7]}", pr=0, profile=profile)


def image_metadata():
    sha = os.environ["PERF_SHA"]
    require(SHA.fullmatch(sha) is not None, "invalid image commit")
    pr = positive(os.environ["PERF_PR"], "PR number", allow_zero=True)
    image = f"{image_repository()}:pr-{pr}-{sha[:7]}"
    require(os.environ["PERF_IMAGE"] == image, "image tag does not match commit")
    digest = os.environ["PERF_DIGEST"]
    require(DIGEST.fullmatch(digest) is not None, "invalid image digest")
    metadata = {
        "repository": os.environ["GITHUB_REPOSITORY"], "sha": sha, "pr": pr,
        "runId": positive(os.environ["GITHUB_RUN_ID"], "workflow run"),
        "runAttempt": positive(os.environ["GITHUB_RUN_ATTEMPT"], "workflow attempt"),
        "image": image, "digest": digest,
    }
    target = Path("build/perf-image/image-metadata.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(metadata, indent=2) + "\n")


def capture_digest():
    sha = os.environ["PERF_SHA"]
    require(SHA.fullmatch(sha) is not None, "invalid image commit")
    image = f"{image_repository()}:pr-0-{sha[:7]}"
    require(os.environ["PERF_IMAGE"] == image, "invalid baseline image reference")
    digest = subprocess.check_output(
        ["docker", "buildx", "imagetools", "inspect", image, "--format", "{{ .Manifest.Digest }}"], text=True,
    ).strip()
    require(DIGEST.fullmatch(digest) is not None, "invalid registry digest")
    outputs(image=f"{image}@{digest}")


def image_labels(value):
    require(isinstance(value, dict) and bool(value), "registry returned incomplete image configuration")
    if "config" in value or "Config" in value:
        config = value.get("config", value.get("Config"))
        require(isinstance(config, dict) and isinstance(config.get("Labels"), dict),
                "registry returned incomplete image configuration")
        return [config["Labels"]]
    # Buildx returns a platform map for an image index. Every platform must
    # provide its own complete configuration; never skip an empty platform.
    return [labels for child in value.values() for labels in image_labels(child)]


def validate_image_config(config, sha, repository):
    labels = image_labels(config)
    require(bool(labels), "registry returned no image configuration")
    for item in labels:
        require(item.get("org.opencontainers.image.revision") == sha, "registry image commit mismatch")
        require(item.get("org.opencontainers.image.source") == f"https://github.com/{repository}",
                "registry image source mismatch")


def verify_image():
    sha = os.environ["PERF_SHA"]
    pr = positive(os.environ["PERF_PR"], "PR number", allow_zero=True)
    require(SHA.fullmatch(sha) is not None, "invalid image commit")
    image = os.environ["PERF_IMAGE"]
    prefix = f"{image_repository()}:pr-{pr}-{sha[:7]}@"
    require(image.startswith(prefix) and DIGEST.fullmatch(image[len(prefix):]) is not None,
            "image is not a digest-pinned expected commit")
    config = subprocess.check_output(
        ["docker", "buildx", "imagetools", "inspect", image, "--format", "{{json .Image}}"], text=True,
    )
    require(len(config) <= MAX_JSON, "registry image configuration exceeds size limit")
    validate_image_config(json.loads(config), sha, os.environ["GITHUB_REPOSITORY"])


def recheck_pr():
    pr = positive(os.environ["PERF_PR"], "PR number", allow_zero=True)
    if pr == 0:
        outputs(enabled="true")
        return
    github = GitHub()
    profile = pr_profile(github.json(f"pulls/{pr}"), github.repository, os.environ["PERF_SHA"])
    outputs(enabled="true" if profile == os.environ["PERF_PROFILE"] else "false")


def check_access():
    require(re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", os.environ.get("PERF_AWS_ROLE_ARN", ""))
            is not None, "configure PERF_AWS_ROLE_ARN before enabling acme-perf")
    require(bool(os.environ.get("PERF_TS_OAUTH_CLIENT_ID")), "configure PERF_TS_OAUTH_CLIENT_ID")
    require(bool(os.environ.get("PERF_TS_OAUTH_SECRET")), "configure secret PERF_TS_OAUTH_SECRET")
    require(re.fullmatch(r"tag:[a-zA-Z0-9_-]+(?:,tag:[a-zA-Z0-9_-]+)*", os.environ["PERF_TS_TAGS"])
            is not None, "invalid PERF_TS_TAGS")


def kubeconfig():
    # Fixed target: a workflow payload cannot select another cluster/account.
    target = Path(os.environ["RUNNER_TEMP"]) / "perf-kubeconfig"
    region = os.environ.get("PERF_AWS_REGION", "eu-west-1")
    require(re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]", region) is not None, "invalid AWS region")
    subprocess.run([
        "aws", "eks", "update-kubeconfig", "--name", "eks-acme-dev-euw1-01", "--region", region,
        "--alias", "eks-acme-dev-euw1-01", "--kubeconfig", str(target),
    ], check=True)
    target.chmod(0o600)
    require("\n" not in str(target) and "\r" not in str(target), "invalid runner temporary directory")
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as environment:
        environment.write(f"KUBECONFIG={target}\n")


def check_cancel():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    name = event["inputs"]["benchmark_name"]
    uid = event["inputs"]["benchmark_uid"]
    require(re.fullmatch(r"ledger-[a-f0-9]{12}-[1-9][0-9]*-[1-9][0-9]*", name) is not None,
            "invalid benchmark name")
    require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", uid) is not None,
            "invalid benchmark UID")
    outputs(name=name, uid=uid)


COMMANDS = {
    "prepare-pr": prepare_pr, "prepare-main": prepare_main, "image-metadata": image_metadata,
    "capture-digest": capture_digest, "verify-image": verify_image, "recheck-pr": recheck_pr,
    "check-access": check_access, "kubeconfig": kubeconfig, "check-cancel": check_cancel,
}


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 2 and sys.argv[1] in COMMANDS, "expected a perf-ci subcommand")
        COMMANDS[sys.argv[1]]()
    except (AdmissionError, KeyError, TypeError, json.JSONDecodeError, zipfile.BadZipFile,
            urllib.error.URLError, subprocess.CalledProcessError) as error:
        print(f"perf-ci: {error}", file=sys.stderr)
        sys.exit(1)
