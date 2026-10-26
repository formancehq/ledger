#!/usr/bin/env python3
"""Exercise the pinned public CLI and its real NDJSON report."""

import contextlib
import io
import json
import os
import subprocess
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import test_api


def response_schema(schema, **extra):
    return {
        "description": "Test response",
        "content": {"application/json": {"schema": schema}},
        **extra,
    }


@contextlib.contextmanager
def server(*, invalid=False):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            requests.append((self.command, self.path))
            payload = {"id": "invalid" if invalid else 424242}
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{httpd.server_port}", requests
        finally:
            httpd.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("fixture server did not stop")


ITEM = {"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}


class RunnerIntegrationTest(unittest.TestCase):
    def run_suite(self, url, paths, *, relative_path=False):
        with TemporaryDirectory(prefix="ledger-schema-test-") as directory:
            schema = Path(directory) / "openapi.json"
            schema.write_text(json.dumps({
                "openapi": "3.0.3",
                "info": {"title": "Runner regression", "version": "1"},
                "paths": paths,
            }))
            output = io.StringIO()
            cli_output = []
            actual_run = subprocess.run

            def run_cli(*args, **kwargs):
                result = actual_run(
                    *args, **kwargs, timeout=60, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True,
                )
                cli_output.append(result.stdout)
                return result

            if relative_path:
                # The CLI changes into its own temporary directory. Resolve a
                # caller-relative schema before crossing that process boundary.
                schema = Path(os.path.relpath(schema))
            with patch.object(test_api, "OPENAPI_PATH", schema):
                # Keep the actual CLI process, but bound a broken integration
                # so this regression cannot hang the test runner indefinitely.
                with patch.object(test_api.subprocess, "run", run_cli), \
                        contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                    status = test_api.run_suite(SimpleNamespace(
                        base_url=url, max_examples=1, workers=1, shrink=False,
                    ))
            return status, output.getvalue() + "\n" + "\n".join(cli_output)

    def test_successful_checks_from_real_cli_pass(self):
        with server() as (url, requests):
            status, output = self.run_suite(url, {
                "/items": {"get": {"responses": {"200": response_schema(ITEM)}}},
            })
        self.assertEqual(0, status, output)
        self.assertTrue(requests)
        self.assertIn("RESULT: ALL CHECKS PASSED", output)
        self.assertNotIn("Successful checks: 0", output)

    def test_real_response_schema_failure_fails(self):
        with server(invalid=True) as (url, requests):
            status, output = self.run_suite(url, {
                "/items": {"get": {"responses": {"200": response_schema(ITEM)}}},
            })
        self.assertNotEqual(0, status, output)
        self.assertTrue(requests)
        self.assertIn("RESULT: FAILURES DETECTED", output)

    def test_caller_relative_schema_survives_cli_directory_change(self):
        with server() as (url, requests):
            status, output = self.run_suite(url, {
                "/items": {"get": {"responses": {"200": response_schema(ITEM)}}},
            }, relative_path=True)
        self.assertEqual(0, status, output)
        self.assertIn(("GET", "/items"), requests)

    def test_unreachable_server_fails(self):
        # Reserve an ephemeral port, then close its listener before the CLI
        # starts. No HTTP requests are accepted by this fixture.
        with ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler) as httpd:
            url = f"http://127.0.0.1:{httpd.server_port}"
        status, output = self.run_suite(url, {
            "/items": {"get": {"responses": {"200": response_schema(ITEM)}}},
        })
        self.assertNotEqual(0, status, output)
        self.assertIn("RESULT: FAILURES DETECTED", output)
        self.assertIn("no successful check execution was observed", output)

    def test_zero_operations_fail(self):
        with server() as (url, requests):
            status, output = self.run_suite(url, {})
        self.assertNotEqual(0, status, output)
        # The CLI still probes transport capabilities without any operations.
        self.assertFalse(any(path.startswith("/items") for _, path in requests))
        self.assertIn("no successful check execution was observed", output)

    def test_stateful_links_reach_response_derived_identifier(self):
        with server() as (url, requests):
            status, output = self.run_suite(url, {
                "/items": {"post": {
                    "operationId": "createItem",
                    "responses": {"200": response_schema(ITEM, links={
                        "ReadCreatedItem": {
                            "operationId": "getItem",
                            "parameters": {"id": "$response.body#/id"},
                        },
                    })},
                }},
                "/items/{id}": {"get": {
                    "operationId": "getItem",
                    # Ordinary generation can only produce 1. A request for
                    # 424242 proves the response-derived link was traversed.
                    "parameters": [{"name": "id", "in": "path", "required": True,
                                    "schema": {"type": "integer", "minimum": 1, "maximum": 1}}],
                    "responses": {"200": response_schema(ITEM)},
                }},
            })
        self.assertEqual(0, status, output)
        self.assertIn(("GET", "/items/424242"), requests)


if __name__ == "__main__":
    unittest.main()
