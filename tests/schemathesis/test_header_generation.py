#!/usr/bin/env python3

import unittest

import schemathesis
from hypothesis import given, settings
from hypothesis import strategies as st

from test_api import OPENAPI_PATH, before_generate_headers


def valid_field_value(value):
    # RFC field values exclude CTLs except HTAB; obs-text is Latin-1.
    forbidden = set(range(32)) - {9} | {127}
    try:
        encoded = value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return forbidden.isdisjoint(encoded)


class HeaderGenerationTest(unittest.TestCase):
    def test_control_bytes_are_filtered(self):
        for codepoint in range(256):
            value = "key" + chr(codepoint) + "value"
            headers = {"Idempotency-Key": value}
            # Include a valid alternative so filtering always has a result.
            strategy = before_generate_headers(
                None, st.sampled_from([headers, {"Idempotency-Key": "valid"}])
            )

            @given(strategy)
            @settings(max_examples=2, derandomize=True, database=None)
            def check(generated):
                self.assertTrue(valid_field_value(generated["Idempotency-Key"]))

            check()

    def test_valid_values_are_preserved(self):
        for value in ("", "visible !~", "key\tvalue", "\x80\x9f\xff"):
            headers = {"Idempotency-Key": value, "X-Consistency": "strong"}

            @given(before_generate_headers(None, st.just(headers)))
            @settings(max_examples=1, derandomize=True, database=None)
            def check(generated):
                self.assertEqual(headers, generated)

            with self.subTest(value=value):
                check()

    def test_absent_headers_are_preserved(self):
        for headers in (None, {}):
            @given(before_generate_headers(None, st.just(headers)))
            @settings(max_examples=1, derandomize=True, database=None)
            def check(generated):
                self.assertEqual(headers, generated)

            with self.subTest(headers=headers):
                check()

    def test_generated_operation_headers_are_valid_http(self):
        schema = schemathesis.openapi.from_path(str(OPENAPI_PATH))
        schema.config.generation.with_security_parameters = False
        # The index operation has an unconstrained inline Idempotency-Key;
        # transactions use the shared parameter with maxLength: 256.
        for path, method in (
            ("/health", "GET"),
            ("/v3/{ledgerName}/indexes", "POST"),
            ("/v3/{ledgerName}/transactions", "POST"),
        ):
            operation = schema[path][method]

            @given(operation.as_strategy())
            @settings(max_examples=30, derandomize=True, database=None, deadline=None)
            def check(case):
                for value in (case.headers or {}).values():
                    self.assertTrue(valid_field_value(value), repr(case.headers))

            with self.subTest(path=path):
                check()


if __name__ == "__main__":
    unittest.main()
