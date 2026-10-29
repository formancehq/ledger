import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import submit


class SubmitTests(unittest.TestCase):
    def test_cancel_checks_uid_ownership_and_atomic_patch(self):
        name = "ledger-aaaaaaaaaaaa-10-1"
        resource = {"metadata": {"uid": "original", "labels": {"app.kubernetes.io/managed-by": "ledger-perf"}}}
        with patch.object(submit, "kubectl") as kubectl:
            kubectl.return_value.stdout = json.dumps(resource)
            with self.assertRaisesRegex(ValueError, "UID"):
                submit.cancel(name, "replacement")
            self.assertEqual(kubectl.call_count, 1)
            kubectl.reset_mock()
            submit.cancel(name, "original")
            args = kubectl.call_args.args
            payload = json.loads(args[-1])
            self.assertEqual(payload, [{"op": "test", "path": "/metadata/uid", "value": "original"},
                                       {"op": "add", "path": "/spec/cancel", "value": True}])

    def test_only_older_pending_requests_are_superseded(self):
        items = []
        for run, phase in ((10, "Queued"), (11, "Running"), (12, "Pending"), (14, "Queued")):
            items.append({"metadata": {"name": f"ledger-aaaaaaaaaaaa-{run}-1", "uid": str(run), "resourceVersion": "1",
                                      "annotations": {"benchmark.formance.com/run-id": str(run),
                                                      "benchmark.formance.com/run-attempt": "1"}},
                          "status": {"phase": phase}})
        with patch.object(submit, "kubectl") as kubectl, patch.object(submit, "cancel") as cancel:
            kubectl.return_value.stdout = json.dumps({"items": items})
            submit.supersede_pending("42", "13", "1")
            self.assertEqual([call.args[1] for call in cancel.call_args_list], ["10", "12"])

    def test_same_workflow_retry_supersedes_only_older_pending_attempt(self):
        items = [{"metadata": {"name": f"ledger-aaaaaaaaaaaa-10-{attempt}", "uid": str(attempt),
                              "resourceVersion": "1", "annotations": {
                                  "benchmark.formance.com/run-id": "10",
                                  "benchmark.formance.com/run-attempt": str(attempt)}},
                  "status": {"phase": "Queued"}} for attempt in (1, 3)]
        with patch.object(submit, "kubectl") as kubectl, patch.object(submit, "cancel") as cancel:
            kubectl.return_value.stdout = json.dumps({"items": items})
            submit.supersede_pending("42", "10", "2")
            cancel.assert_called_once_with("ledger-aaaaaaaaaaaa-10-1", "1", "1")

    def test_supersession_preserves_request_started_since_list(self):
        name = "ledger-aaaaaaaaaaaa-10-1"
        current = {"metadata": {"uid": "original", "resourceVersion": "2",
                               "labels": {"app.kubernetes.io/managed-by": "ledger-perf"}},
                   "status": {"phase": "Running"}}
        with patch.object(submit, "kubectl") as kubectl:
            kubectl.return_value.stdout = json.dumps(current)
            self.assertFalse(submit.cancel(name, "original", "1"))
            self.assertEqual(kubectl.call_count, 1)

    def test_supersession_checks_resource_version_atomically(self):
        name = "ledger-aaaaaaaaaaaa-10-1"
        def response(version):
            return subprocess.CompletedProcess([], 0, json.dumps({"metadata": {
                "uid": "original", "resourceVersion": version,
                "labels": {"app.kubernetes.io/managed-by": "ledger-perf"}}}))
        with patch.object(submit, "kubectl", side_effect=[response("1"),
                subprocess.CalledProcessError(1, "kubectl"), response("2")]) as kubectl:
            self.assertFalse(submit.cancel(name, "original", "1"))
            patch_body = json.loads(kubectl.call_args_list[1].args[-1])
            self.assertEqual(patch_body[1], {"op": "test", "path": "/metadata/resourceVersion", "value": "1"})

    def test_preflight_rejects_old_operator(self):
        document = {"spec": {"versions": [{"name": "v1alpha1", "served": True, "schema": {
            "openAPIV3Schema": {"properties": {"spec": {"properties": {}}}}}}]}}
        with patch.object(submit, "kubectl") as kubectl:
            kubectl.return_value.stdout = json.dumps(document)
            with self.assertRaisesRegex(ValueError, "upgraded"):
                submit.preflight()

    def test_submission_isolated_from_user_stacks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tests" / "perf"
            source.mkdir(parents=True)
            for filename in ("main.go", "ci.go", "main_test.go", "go.mod", "go.sum", "Pulumi.maxence.yaml"):
                (source / filename).write_text("original")
            for folder in ("scripts", "profiles"):
                (source / folder).mkdir()
                (source / folder / "input").write_text("input")
            sha = "a" * 40
            image = "ghcr.io/formancehq/ledger:pr-42-aaaaaaa@sha256:" + "b" * 64
            with patch.object(submit, "ROOT", root), patch.object(submit, "preflight"), \
                 patch.object(submit, "supersede_pending"), patch.object(submit.subprocess, "run") as run, \
                 patch.object(submit.subprocess, "check_output", return_value="c" * 40), \
                 patch.dict(os.environ, {"PERF_STATE_DIR": str(root / "state"),
                                         "PULUMI_BACKEND_URL": "https://api.pulumi.com"}, clear=True):
                submit.submit("smoke-5m", sha, image, "123", "1", "42")
            project = root / "build" / "perf" / "runs" / "ledger-aaaaaaaaaaaa-123-1" / "project"
            self.assertFalse((project / "Pulumi.maxence.yaml").exists())
            self.assertFalse((project / "main_test.go").exists())
            self.assertEqual((source / "Pulumi.maxence.yaml").read_text(), "original")
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(commands[0][0:2], ["pulumi", "login"])
            self.assertTrue(commands[0][2].startswith((root / "state").resolve().as_uri()))
            self.assertEqual(commands[-1], ["pulumi", "up", "--yes", "--skip-preview", "--non-interactive"])
            config = {command[3]: command[4] for command in commands if command[1:3] == ["config", "set"]}
            self.assertEqual(config["perf:commit"], sha)
            self.assertEqual(config["perf:image"], image)
            self.assertEqual(config["perf:when-busy"], "Queue")
            self.assertEqual(config["perf:workload-commit"], "c" * 40)
            self.assertTrue(all(call.kwargs["cwd"] == project for call in run.call_args_list))
            self.assertTrue(all(call.kwargs["env"]["PULUMI_BACKEND_URL"] == commands[0][2]
                                for call in run.call_args_list))

    def test_invalid_input_never_contacts_cluster(self):
        with patch.object(submit, "kubectl") as kubectl:
            with self.assertRaisesRegex(ValueError, "SHA"):
                submit.submit("smoke-5m", "../", "image", "1", "1", "0")
            kubectl.assert_not_called()

    def test_pulumi_failure_is_not_reported_as_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tests" / "perf"
            source.mkdir(parents=True)
            for filename in ("main.go", "go.mod", "go.sum"):
                (source / filename).write_text("input")
            for folder in ("scripts", "profiles"):
                (source / folder).mkdir()
            with patch.object(submit, "ROOT", root), patch.object(submit, "preflight"), \
                 patch.object(submit.subprocess, "check_output", return_value="c" * 40), \
                 patch.object(submit.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "pulumi")), \
                 patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(subprocess.CalledProcessError):
                    submit.submit("soak-24h", "a" * 40, "ghcr.io/f/l:pr-0-aaaaaaa@sha256:" + "b" * 64, "1", "1", "0")


if __name__ == "__main__":
    unittest.main()
