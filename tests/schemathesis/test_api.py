#!/usr/bin/env python3
"""Schemathesis API testing for Ledger V3.

Runs OpenAPI conformity checks and fuzzing against all documented endpoints.
Uses stateful link traversal for dependent request chaining.

Usage:
    python test_api.py [--base-url URL] [--max-examples N]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import schemathesis

OPENAPI_PATH = Path(
    os.environ.get(
        "SCHEMATHESIS_OPENAPI_PATH",
        Path(__file__).resolve().parents[2] / "openapi.yml",
    )
)

VALID_LEDGER_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{2,20}$")

# Fixture ledger name every coerced/invalid ledgerName path parameter resolves
# to. Seeded by run.sh with two transactions and a revert so
# response_schema_conformance validates real rows instead of an empty [].
FIXTURE_LEDGER_NAME = "test-ledger"
# Sacrificial ledger name for DELETE /v3/{ledgerName} specifically — see the
# before_call hook below for why it needs its own name.
DELETE_LEDGER_SACRIFICE_NAME = "delete-me-ledger"

SAMPLE_NUMSCRIPTS = [
    'send [USD/2 100] (\n  source = @world\n  destination = @user:001\n)',
    'send [EUR/2 50] (\n  source = @users:alice\n  destination = @users:bob\n)',
]

_numscript_idx = 0


# --- Link validation + domain hooks (applied to actual test cases) ---
@schemathesis.hook
def before_call(context, case, kwargs):
    """Validate link resolution and adjust domain-specific constraints."""
    global _numscript_idx

    # Link validation: warn if any parameter resolved to None
    for key, value in (case.path_parameters or {}).items():
        if value is None:
            print(
                f"WARNING: Link resolved '{key}' to None for "
                f"{case.method.upper()} {case.formatted_path}",
                file=sys.stderr,
            )

    # DELETE /v3/{ledgerName} is itself a fuzzed operation, and the coercion
    # below would point it at the seeded fixture ledger. With workers=1 the
    # operations run in openapi.yml declaration order, and this one is
    # declared before the transaction endpoints — so the fuzzer deleted the
    # fixture within seconds of the run starting, and every later request
    # returned "ledger has been deleted", validating against an error schema
    # instead of TransactionResponse. That made response_schema_conformance
    # vacuous for every seeded route. Send ledger-level deletes to a
    # sacrificial ledger so the fixture survives the whole run.
    if case.path == "/v3/{ledgerName}" and case.method == "DELETE":
        if case.path_parameters:
            case.path_parameters["ledgerName"] = DELETE_LEDGER_SACRIFICE_NAME
    # Ledger names: lowercase alphanumeric with hyphens
    elif case.path_parameters and "ledgerName" in case.path_parameters:
        name = case.path_parameters["ledgerName"]
        if not isinstance(name, str) or not VALID_LEDGER_NAME_RE.match(name):
            case.path_parameters["ledgerName"] = FIXTURE_LEDGER_NAME

    if case.body and isinstance(case.body, dict):
        # Transaction: postings/script mutual exclusion
        if "postings" in case.body and "script" in case.body:
            del case.body["script"]

        # Provide valid postings structure when present
        if "postings" in case.body and isinstance(case.body["postings"], list):
            for posting in case.body["postings"]:
                if isinstance(posting, dict):
                    posting.setdefault("source", "world")
                    posting.setdefault("destination", "user:001")
                    posting.setdefault("asset", "USD/2")
                    if "amount" not in posting or not isinstance(
                        posting.get("amount"), (int, str)
                    ):
                        posting["amount"] = 100

        # Provide valid numscript content
        if "content" in case.body and "numscript" in (case.path or "").lower():
            case.body["content"] = SAMPLE_NUMSCRIPTS[
                _numscript_idx % len(SAMPLE_NUMSCRIPTS)
            ]
            _numscript_idx += 1

        # Metadata: ensure it's a valid object
        if "metadata" in case.body and not isinstance(case.body["metadata"], dict):
            case.body["metadata"] = {}


@schemathesis.hook
def after_call(context, case, response):
    """Log details of server errors and connection failures for debugging."""
    if response is not None and response.status_code >= 500:
        print(
            f"  >> SERVER ERROR {response.status_code} on "
            f"{case.method.upper()} {case.formatted_path}: "
            f"body={case.body!r:.200} resp={response.text[:200]}",
            file=sys.stderr,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Schemathesis API conformity and fuzzing tests for Ledger V3"
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:9099",
        help="Base URL of the running server (default: http://localhost:9099)",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=50,
        help="Max examples per endpoint (default: 50)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Number of concurrent workers (default: 1). Keep at 1 for the CI "
            "gate: >1 reintroduces nondeterminism (thread interleaving over the "
            "stateful-link sequences) that defeats deterministic "
            "reproducibility below. The suite is fast at 1 worker since the "
            "cost was generation, not request throughput; raise this only for "
            "ad-hoc exploratory fuzzing, not the reproducible gate."
        ),
    )
    parser.add_argument(
        "--shrink",
        action="store_true",
        help=(
            "Enable Hypothesis shrinking (minimizes failing examples). "
            "Off by default: shrinking multiplies request volume by ~7x when "
            "endpoints fail, without changing which failures are detected. "
            "Enable locally when you need a minimal reproduction."
        ),
    )
    args = parser.parse_args()

    print(f"Loading schema from {OPENAPI_PATH}")
    print(f"Target server: {args.base_url}")
    print(f"Max examples per endpoint: {args.max_examples}")
    print(f"Workers: {args.workers}")
    print("=" * 60)

    sys.exit(run_suite(args))


def run_suite(args):
    """Use the public v4 CLI and require evidence of completed checks.

    The NDJSON report replaces the removed v3 Python runner API. Keep the
    subprocess status authoritative as well as our stricter accounting guard.
    """
    with tempfile.TemporaryDirectory(prefix="ledger-schemathesis-") as directory:
        report = Path(directory) / "events.ndjson"
        # An explicit empty config prevents a contributor's discovered config
        # from changing this gate's checks, filters, phases, or failure policy.
        config = Path(directory) / "schemathesis.toml"
        config.write_text("")
        command = [
            sys.executable, "-m", "schemathesis.cli",
            "--config-file", str(config), "--no-color", "run", str(OPENAPI_PATH.resolve()),
            "--url", args.base_url,
            "--max-examples", str(args.max_examples),
            "--workers", str(args.workers),
            "--checks", "not_a_server_error,status_code_conformance,response_schema_conformance",
            "--phases", "examples,fuzzing,stateful",
            "--mode", "positive",
            # Deterministic mode also disables the Hypothesis database.
            "--generation-deterministic",
            # The disposable server has auth disabled. Fuzzed Bearer headers
            # otherwise exercise net/http rejection rather than the API.
            "--generation-with-security-parameters", "false",
            "--suppress-health-check", "filter_too_much",
            "--report", "ndjson", "--report-ndjson-path", str(report),
        ]
        if not args.shrink:
            command.append("--no-shrink")
        env = dict(os.environ, SCHEMATHESIS_HOOKS="test_api", PYTHONDONTWRITEBYTECODE="1")
        # Resolve this module even when called from another working directory.
        env["PYTHONPATH"] = os.pathsep.join(
            [str(Path(__file__).resolve().parent), env.get("PYTHONPATH", "")]
        )
        result = subprocess.run(command, env=env, cwd=directory, check=False)
        try:
            with report.open() as stream:
                report_status = _report_events(
                    (json.loads(line) for line in stream), cli_status=result.returncode
                )
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            print(f"ERROR: invalid or missing Schemathesis report: {error}", file=sys.stderr)
            report_status = 1
        return result.returncode or report_status


def _report_events(events, *, cli_status=0):
    """Account for the pinned CLI's NDJSON events without importing its engine.

    Errors remain fatal even if a later scenario for the same operation passes.
    A successful scenario alone is insufficient: a check must actually pass.
    """
    failed = cli_status != 0
    if failed:
        print(f"ERROR: Schemathesis CLI exited with status {cli_status}", file=sys.stderr)
    finished = False
    successful_checks = 0
    errored_count = 0
    for event in events:
        if finished:
            raise ValueError("events after EngineFinished")
        if not isinstance(event, dict) or len(event) != 1:
            raise ValueError("expected one NDJSON event")
        name, data = next(iter(event.items()))
        if name in ("NonFatalError", "FatalError", "Interrupted"):
            failed = True
            errored_count += 1
            print(f"ERROR: {data.get('label', name)}: {data}", file=sys.stderr)
        if data.get("status") in ("error", "failure", "interrupted"):
            failed = True
        if name == "ScenarioFinished":
            recorder = data["recorder"]
            for checks in recorder.get("checks", {}).values():
                for check in checks:
                    if check["status"] == "success":
                        successful_checks += 1
                    elif check["status"] in ("failure", "error"):
                        failed = True
        elif name == "EngineFinished":
            finished = True
            if data["stop_reason"] != "completed" or data.get("failures"):
                failed = True
    if not finished:
        print("ERROR: Schemathesis runner ended without an EngineFinished event", file=sys.stderr)
        failed = True
    if successful_checks == 0:
        print("ERROR: no successful check execution was observed", file=sys.stderr)
        failed = True
    print(f"Successful checks: {successful_checks} | Execution errors: {errored_count}")
    print("RESULT: FAILURES DETECTED" if failed else "RESULT: ALL CHECKS PASSED")
    return int(failed)


if __name__ == "__main__":
    main()
