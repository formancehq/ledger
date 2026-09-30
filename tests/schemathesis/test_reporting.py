#!/usr/bin/env python3

import contextlib
import io
import unittest
from types import SimpleNamespace

from schemathesis.models import Status
from schemathesis.runner.events import AfterExecution, Finished

from test_api import _report_events


def after_execution(*, method="GET", path="/v3/test", checks=(), errors=()):
    return AfterExecution(
        method=method,
        path=path,
        relative_path=path,
        verbose_name=f"{method} {path}",
        status="error" if errors else "success",
        data_generation_method=[],
        result=SimpleNamespace(
            has_failures=False,
            has_errors=bool(errors),
            checks=list(checks),
            errors=list(errors),
        ),
        elapsed_time=0,
        correlation_id="test",
    )


def finished(*, passed=0, errored=0):
    return Finished(
        passed_count=passed,
        skipped_count=0,
        failed_count=0,
        errored_count=errored,
        has_failures=False,
        has_errors=bool(errored),
        has_logs=False,
        is_empty=passed == 0 and errored == 0,
        generic_errors=[],
        warnings=[],
        total={},
        running_time=0,
    )


class ReportingTest(unittest.TestCase):
    def report(self, events):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            exit_code = _report_events(events)
        return exit_code, output.getvalue()

    def test_transport_error_followed_by_different_path_success_fails(self):
        exit_code, output = self.report(
            [
                after_execution(errors=["ConnectionRefusedError: refused"]),
                after_execution(
                    path="/v3/other",
                    checks=[SimpleNamespace(value=Status.success, message=None)],
                ),
                finished(passed=1, errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("ERROR: GET /v3/test: ConnectionRefusedError: refused", output)
        self.assertIn("Passed: 1 | Failed: 0 | Errored: 1", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_transport_error_without_recovery_fails(self):
        exit_code, output = self.report(
            [
                after_execution(errors=["ConnectionRefusedError: refused"]),
                finished(errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("ERROR: GET /v3/test: ConnectionRefusedError: refused", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_transport_error_followed_by_different_method_success_fails(self):
        exit_code, output = self.report(
            [
                after_execution(errors=["ConnectionRefusedError: refused"]),
                after_execution(
                    method="POST",
                    checks=[SimpleNamespace(value=Status.success, message=None)],
                ),
                finished(passed=1, errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("ERROR: GET /v3/test: ConnectionRefusedError: refused", output)
        self.assertIn("Passed: 1 | Failed: 0 | Errored: 1", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_transport_error_followed_by_same_operation_success_fails(self):
        exit_code, output = self.report(
            [
                after_execution(errors=["ConnectionRefusedError: refused"]),
                after_execution(
                    checks=[SimpleNamespace(value=Status.success, message=None)]
                ),
                finished(passed=1, errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("Passed: 1 | Failed: 0 | Errored: 1", output)
        self.assertIn("ERROR: GET /v3/test: ConnectionRefusedError: refused", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_success_before_transport_error_does_not_prove_recovery(self):
        exit_code, output = self.report(
            [
                after_execution(
                    checks=[SimpleNamespace(value=Status.success, message=None)]
                ),
                after_execution(errors=["ConnectionRefusedError: refused"]),
                finished(passed=1, errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("ERROR: GET /v3/test: ConnectionRefusedError: refused", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_zero_executions_fail(self):
        exit_code, output = self.report([finished()])

        self.assertEqual(1, exit_code)
        self.assertIn("no successful check execution was observed", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_successful_checks_without_errors_pass(self):
        exit_code, output = self.report(
            [
                after_execution(
                    checks=[SimpleNamespace(value=Status.success, message=None)]
                ),
                finished(passed=1),
            ]
        )

        self.assertEqual(0, exit_code)
        self.assertIn("Passed: 1 | Failed: 0 | Errored: 0", output)
        self.assertIn("RESULT: ALL CHECKS PASSED", output)

    def test_non_network_error_followed_by_success_fails(self):
        exit_code, output = self.report(
            [
                after_execution(errors=["InvalidSchema: invalid response"]),
                after_execution(
                    checks=[SimpleNamespace(value=Status.success, message=None)]
                ),
                finished(passed=1, errored=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("ERROR: GET /v3/test: InvalidSchema: invalid response", output)
        self.assertIn("Passed: 1 | Failed: 0 | Errored: 1", output)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_success_without_finished_event_fails(self):
        exit_code, output = self.report(
            [after_execution(checks=[SimpleNamespace(value=Status.success, message=None)])]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("runner ended without a Finished event", output)

    def test_execution_without_checks_does_not_prove_success(self):
        exit_code, output = self.report(
            [after_execution(), finished(passed=1)]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("no successful check execution was observed", output)

    def test_skipped_check_does_not_prove_success(self):
        exit_code, output = self.report(
            [
                after_execution(
                    checks=[SimpleNamespace(value=Status.skip, message=None)]
                ),
                finished(passed=1),
            ]
        )

        self.assertEqual(1, exit_code)
        self.assertIn("no successful check execution was observed", output)


if __name__ == "__main__":
    unittest.main()
