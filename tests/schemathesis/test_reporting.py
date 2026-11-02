#!/usr/bin/env python3

import contextlib
import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_api import _report_events, run_suite


def scenario(*, method="GET", path="/v3/test", checks=("success",), status="success"):
    return {"ScenarioFinished": {
        "status": status,
        "recorder": {
            "label": f"{method} {path}",
            "checks": {"case": [{"name": "not_a_server_error", "status": s} for s in checks]},
        },
    }}


def error(*, method="GET", path="/v3/test", message="ConnectionRefusedError: refused"):
    return {"NonFatalError": {"label": f"{method} {path}", "value": message}}


def finished(**kwargs):
    return {"EngineFinished": {"stop_reason": "completed", **kwargs}}


class ReportingTest(unittest.TestCase):
    def report(self, events):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            exit_code = _report_events(events)
        return exit_code, output.getvalue()

    def test_transport_error_followed_by_success_fails(self):
        # Preserve each original trigger: another path, another method, and the
        # very same operation. Success cannot erase a previous execution error.
        for method, path in (("GET", "/v3/other"), ("POST", "/v3/test"), ("GET", "/v3/test")):
            with self.subTest(method=method, path=path):
                code, output = self.report([error(), scenario(method=method, path=path), finished()])
                self.assertEqual(1, code)
                self.assertIn("ERROR: GET /v3/test:", output)
                self.assertIn("ConnectionRefusedError: refused", output)
                self.assertIn("Execution errors: 1", output)
                self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_transport_error_without_recovery_fails(self):
        code, output = self.report([error(), finished()])
        self.assertEqual(1, code)
        self.assertIn("ConnectionRefusedError: refused", output)

    def test_success_before_transport_error_does_not_prove_recovery(self):
        code, output = self.report([scenario(), error(), finished()])
        self.assertEqual(1, code)
        self.assertIn("Execution errors: 1", output)

    def test_non_network_error_followed_by_success_fails(self):
        code, output = self.report([error(message="InvalidSchema: invalid response"), scenario(), finished()])
        self.assertEqual(1, code)
        self.assertIn("InvalidSchema: invalid response", output)

    def test_zero_executions_fail(self):
        code, output = self.report([finished()])
        self.assertEqual(1, code)
        self.assertIn("no successful check execution was observed", output)

    def test_successful_checks_without_errors_pass(self):
        code, output = self.report([scenario(), finished()])
        self.assertEqual(0, code)
        self.assertIn("RESULT: ALL CHECKS PASSED", output)

    def test_success_followed_by_final_only_failure_fails(self):
        code, output = self.report([scenario(), finished(failures=[{"title": "after_run failure"}])])
        self.assertEqual(1, code)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_success_followed_by_final_only_error_fails(self):
        code, output = self.report([scenario(), {"FatalError": {"exception": "invalid shared parameter"}}])
        self.assertEqual(1, code)
        self.assertIn("invalid shared parameter", output)

    def test_success_without_finished_event_fails(self):
        code, output = self.report([scenario()])
        self.assertEqual(1, code)
        self.assertIn("runner ended without an EngineFinished event", output)

    def test_execution_without_checks_does_not_prove_success(self):
        code, output = self.report([scenario(checks=()), finished()])
        self.assertEqual(1, code)
        self.assertIn("no successful check execution was observed", output)

    def test_skipped_check_does_not_prove_success(self):
        code, output = self.report([scenario(checks=("skip",)), finished()])
        self.assertEqual(1, code)
        self.assertIn("no successful check execution was observed", output)

    def test_failure_cannot_be_erased_by_later_success(self):
        for event in (scenario(checks=("failure",)), scenario(status="error"),
                      {"SuiteFinished": {"status": "error"}},
                      {"Interrupted": {}}, {"PhaseFinished": {"status": "failure"}}):
            with self.subTest(event=event):
                code, _ = self.report([event, scenario(), finished()])
                self.assertEqual(1, code)

    def test_early_stop_is_not_completion(self):
        for reason in ("interrupted", "failure_limit", "max_time", "server_unavailable"):
            with self.subTest(reason=reason):
                code, _ = self.report([scenario(), finished(stop_reason=reason)])
                self.assertEqual(1, code)

    def test_events_after_completion_are_invalid(self):
        with self.assertRaisesRegex(ValueError, "events after EngineFinished"):
            self.report([scenario(), finished(), error()])

    def test_cli_status_and_report_are_both_required(self):
        args = SimpleNamespace(base_url="http://localhost", max_examples=1, workers=1, shrink=False)
        for cli_status, report_status, expected in ((1, 0, 1), (0, 1, 1), (0, 0, 0)):
            with self.subTest(cli_status=cli_status, report_status=report_status):
                def run(command, **kwargs):
                    Path(command[command.index("--report-ndjson-path") + 1]).write_text("")
                    return SimpleNamespace(returncode=cli_status)
                with patch("test_api.subprocess.run", side_effect=run), \
                     patch("test_api._report_events", return_value=report_status):
                    self.assertEqual(expected, run_suite(args))

    def test_cli_error_cannot_print_success_for_a_green_report(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            code = _report_events([scenario(), finished()], cli_status=1)
        self.assertEqual(1, code)
        self.assertIn("RESULT: FAILURES DETECTED", output.getvalue())
        self.assertNotIn("RESULT: ALL CHECKS PASSED", output.getvalue())

    def test_missing_or_malformed_report_fails(self):
        args = SimpleNamespace(base_url="http://localhost", max_examples=1, workers=1, shrink=False)
        for report in (None, "not json", '{}', '{"ScenarioFinished": {}}'):
            with self.subTest(report=report):
                def run(command, **kwargs):
                    if report is not None:
                        Path(command[command.index("--report-ndjson-path") + 1]).write_text(report)
                    return SimpleNamespace(returncode=0)
                with patch("test_api.subprocess.run", side_effect=run):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(1, run_suite(args))


if __name__ == "__main__":
    unittest.main()
