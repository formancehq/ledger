import copy
import importlib.util
import io
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock
import zipfile


spec = importlib.util.spec_from_file_location("perfci", Path(__file__).with_name("main.py"))
perfci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perfci)


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.sha = "a" * 40
        self.repo = "formancehq/ledger"
        self.run = {"id": 123, "run_attempt": 2, "head_sha": self.sha, "pull_requests": [{"number": 42}]}
        self.metadata = {
            "repository": self.repo, "sha": self.sha, "pr": 42, "runId": 123, "runAttempt": 2,
            "image": "ghcr.io/formancehq/ledger:pr-42-aaaaaaa", "digest": "sha256:" + "b" * 64,
        }
        self.pr = {
            "state": "open", "head": {"sha": self.sha, "repo": {"full_name": self.repo}},
            "base": {"repo": {"full_name": self.repo}}, "labels": [{"name": "perf-5m"}],
        }

    def validate(self, metadata=None, run=None):
        return perfci.validate_metadata(metadata or self.metadata, run or self.run, self.repo, "ghcr.io/formancehq/ledger")

    def test_exact_image_run_and_commit_admitted(self):
        result = self.validate()
        self.assertEqual(result["sha"], self.sha)
        self.assertEqual(result["image"], self.metadata["image"] + "@" + self.metadata["digest"])

    def test_reject_cross_repository_commit_attempt_tag_digest_and_run(self):
        for field, value in [
            ("repository", "attacker/repo"), ("sha", "c" * 40), ("runAttempt", 1),
            ("image", "ghcr.io/attacker/ledger:pr-42-aaaaaaa"), ("digest", "sha256:nope"),
            ("runId", 999), ("pr", 13), ("image", "$(touch /tmp/unsafe)"),
        ]:
            with self.subTest(field=field, value=value):
                metadata = self.metadata | {field: value}
                with self.assertRaises(perfci.AdmissionError):
                    self.validate(metadata)

    def test_pr_profiles_reject_ambiguous_labels(self):
        self.assertEqual(perfci.pr_profile(self.pr, self.repo, self.sha), "smoke-5m")
        self.pr["labels"] = [{"name": "perf-10m"}, {"name": "bug"}]
        self.assertEqual(perfci.pr_profile(self.pr, self.repo, self.sha), "smoke-10m")
        self.pr["labels"].append({"name": "perf-5m"})
        with self.assertRaisesRegex(perfci.AdmissionError, "exactly one"):
            perfci.pr_profile(self.pr, self.repo, self.sha)

    def test_stale_closed_fork_or_unlabelled_pr_skipped(self):
        for kind in ["closed", "fork", "stale", "unlabelled", "foreign_base"]:
            with self.subTest(kind=kind):
                pr = copy.deepcopy(self.pr)
                if kind == "closed":
                    pr["state"] = "closed"
                elif kind == "fork":
                    pr["head"]["repo"]["full_name"] = "attacker/ledger"
                elif kind == "stale":
                    pr["head"]["sha"] = "c" * 40
                elif kind == "foreign_base":
                    pr["base"]["repo"]["full_name"] = "other/ledger"
                else:
                    pr["labels"] = []
                self.assertIsNone(perfci.pr_profile(pr, self.repo, self.sha))

    def zip(self, files):
        result = io.BytesIO()
        with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, value in files:
                archive.writestr(name, value)
        return result.getvalue()

    def test_zip_reads_only_one_bounded_json_without_extracting(self):
        content = self.zip([("image-metadata.json", json.dumps(self.metadata))])
        self.assertEqual(perfci.metadata_from_zip(content), self.metadata)
        for files in [
            [("../image-metadata.json", "{}")],
            [("image-metadata.json", "{}"), ("payload.sh", "echo unsafe")],
            [("image-metadata.json", "x" * 4097)],
        ]:
            with self.subTest(files=[name for name, _ in files]):
                with self.assertRaises(perfci.AdmissionError):
                    perfci.metadata_from_zip(self.zip(files))

    def test_zip_symlink_rejected(self):
        entry = zipfile.ZipInfo("image-metadata.json")
        entry.external_attr = (stat.S_IFLNK | 0o777) << 16
        with self.assertRaises(perfci.AdmissionError):
            perfci.metadata_from_zip(self.zip([(entry, "{}")]))

    def test_registry_revision_source_and_every_platform_required(self):
        labels = {
            "org.opencontainers.image.revision": self.sha,
            "org.opencontainers.image.source": "https://github.com/" + self.repo,
        }
        image = {"config": {"Labels": labels}}
        perfci.validate_image_config(image, self.sha, self.repo)
        perfci.validate_image_config({"linux/amd64": image, "linux/arm64": image}, self.sha, self.repo)
        for config in [
            {}, {"config": {"Labels": {}}},
            {"linux/amd64": image, "linux/arm64": {"config": {"Labels": labels | {"org.opencontainers.image.revision": "c" * 40}}}},
            {"config": {"Labels": labels | {"org.opencontainers.image.source": "https://github.com/attacker/ledger"}}},
        ]:
            with self.subTest(config=config):
                with self.assertRaises(perfci.AdmissionError):
                    perfci.validate_image_config(config, self.sha, self.repo)

    def test_artifact_redirect_strips_token(self):
        request = perfci.urllib.request.Request("https://api.github.com/repos/x/y/actions/artifacts/1/zip",
                                                headers={"Authorization": "Bearer secret"})
        redirected = perfci.SafeRedirect().redirect_request(request, None, 302, "found", {}, "https://storage.example/file")
        self.assertFalse(redirected.has_header("Authorization"))

    def test_registry_rejects_each_platform_without_labels(self):
        valid = {"config": {"Labels": {
            "org.opencontainers.image.revision": self.sha,
            "org.opencontainers.image.source": "https://github.com/" + self.repo,
        }}}
        for incomplete in [{}, {"config": {}}, {"config": {"Labels": None}}, None, []]:
            for platform in ["linux/amd64", "linux/arm64"]:
                with self.subTest(platform=platform, incomplete=incomplete):
                    config = {"linux/amd64": valid, "linux/arm64": valid} | {platform: incomplete}
                    with self.assertRaisesRegex(perfci.AdmissionError, "incomplete image configuration"):
                        perfci.validate_image_config(config, self.sha, self.repo)

    def prepare_fixture(self, change_run=None, change_pr=None, artifacts=None):
        run = self.run | {
            "repository": {"full_name": self.repo}, "head_repository": {"full_name": self.repo},
            "path": ".github/workflows/pr-images.yml", "name": "PR Images",
            "event": "pull_request", "status": "completed", "conclusion": "success",
        } | (change_run or {})
        pr = copy.deepcopy(self.pr)
        pr.update(change_pr or {})
        content = self.zip([("image-metadata.json", json.dumps(self.metadata))])
        responses = {
            "actions/runs/123": run, "pulls/42": pr,
            "actions/runs/123/artifacts?per_page=100": {"artifacts": artifacts if artifacts is not None else [
                {"id": 11, "name": "perf-image-1", "expired": False, "size_in_bytes": 100},
                {"id": 12, "name": "perf-image-2", "expired": False, "size_in_bytes": len(content)},
            ]},
        }
        client = mock.Mock(repository=self.repo)
        client.json.side_effect = responses.__getitem__
        client.request.side_effect = lambda route, limit: content if route == "actions/artifacts/12/zip" else self.fail(route)
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / "event.json"
            event.write_text(json.dumps({"workflow_run": {"id": 123}}))
            output = Path(directory) / "outputs"
            with mock.patch.dict(perfci.os.environ, {
                "GITHUB_EVENT_PATH": str(event), "GITHUB_OUTPUT": str(output),
                "IMAGE_REPOSITORY": "ghcr.io/formancehq/ledger",
            }), mock.patch.object(perfci, "GitHub", return_value=client):
                perfci.prepare_pr()
            return dict(line.split("=", 1) for line in output.read_text().splitlines()), client

    def test_prepare_pr_uses_only_current_attempt_artifact_and_current_head(self):
        result, client = self.prepare_fixture()
        self.assertEqual(result["enabled"], "true")
        self.assertEqual(result["profile"], "smoke-5m")
        self.assertEqual(result["pr"], "42")
        client.request.assert_called_once_with("actions/artifacts/12/zip", perfci.MAX_ARTIFACT)

    def test_prepare_pr_missing_current_attempt_or_removed_label_skipped(self):
        result, _ = self.prepare_fixture(artifacts=[
            {"id": 11, "name": "perf-image-1", "expired": False, "size_in_bytes": 100},
        ])
        self.assertEqual(result, {"enabled": "false"})
        result, _ = self.prepare_fixture(change_pr={"labels": []})
        self.assertEqual(result, {"enabled": "false"})

    def test_prepare_pr_rejects_wrong_source_before_artifact_download(self):
        for change in [
            {"repository": {"full_name": "attacker/ledger"}},
            {"head_repository": {"full_name": "attacker/ledger"}},
            {"path": ".github/workflows/pwn.yml"}, {"name": "other"},
            {"event": "pull_request_target"}, {"conclusion": "failure"}, {"status": "in_progress"},
        ]:
            with self.subTest(change=change):
                with self.assertRaises(perfci.AdmissionError):
                    self.prepare_fixture(change_run=change)

    def test_cancel_requires_exact_benchmark_name_and_uid(self):
        uid = "01234567-89ab-cdef-0123-456789abcdef"
        for name, candidate_uid, valid in [
            ("ledger-aaaaaaaaaaaa-123-1", uid, True),
            ("other-resource", uid, False), ("ledger-../../other", uid, False),
            ("ledger-aaaaaaaaaaaa-123-1", "", False),
            ("ledger-aaaaaaaaaaaa-123-1", "$(whoami)", False),
        ]:
            with self.subTest(name=name, uid=candidate_uid), tempfile.TemporaryDirectory() as directory:
                event = Path(directory) / "event.json"
                output = Path(directory) / "output"
                event.write_text(json.dumps({"inputs": {"benchmark_name": name, "benchmark_uid": candidate_uid}}))
                with mock.patch.dict(perfci.os.environ, {"GITHUB_EVENT_PATH": str(event), "GITHUB_OUTPUT": str(output)}):
                    if valid:
                        perfci.check_cancel()
                        self.assertEqual(output.read_text(), f"name={name}\nuid={uid}\n")
                    else:
                        with self.assertRaises(perfci.AdmissionError):
                            perfci.check_cancel()

    def test_recheck_pr_does_not_submit_after_label_or_sha_changed(self):
        client = mock.Mock(repository=self.repo)
        for profile, sha, enabled in [
            ("smoke-5m", self.sha, "true"), ("smoke-10m", self.sha, "false"),
            ("smoke-5m", "b" * 40, "false"),
        ]:
            with self.subTest(profile=profile, sha=sha), tempfile.TemporaryDirectory() as directory:
                client.json.return_value = self.pr
                output = Path(directory) / "output"
                with mock.patch.dict(perfci.os.environ, {
                    "PERF_PR": "42", "PERF_SHA": sha, "PERF_PROFILE": profile, "GITHUB_OUTPUT": str(output),
                }), mock.patch.object(perfci, "GitHub", return_value=client):
                    perfci.recheck_pr()
                self.assertEqual(output.read_text(), f"enabled={enabled}\n")


if __name__ == "__main__":
    unittest.main()
