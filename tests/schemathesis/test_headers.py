#!/usr/bin/env python3

import re
import unittest

from hypothesis import given, settings
from requests.utils import check_header_validity
from schemathesis.hooks import GLOBAL_HOOK_DISPATCHER
from schemathesis.specs.openapi.formats import header_values

from test_api import map_headers, sanitize_header_value

# Field-value bytes Go's net/http accepts: horizontal tab, visible ASCII, space
# and obs-text (0x80-0xFF).
GO_VALID_HEADER_VALUE_RE = re.compile(r"[\t\x20-\x7e\x80-\xff]*")


def assert_reaches_handler(test, value):
    test.assertIsNotNone(GO_VALID_HEADER_VALUE_RE.fullmatch(value), repr(value))
    check_header_validity(("Idempotency-Key", value))


class HeaderSanitizationTest(unittest.TestCase):
    def test_map_headers_is_registered(self):
        self.assertIn(map_headers, GLOBAL_HOOK_DISPATCHER.get_all_by_name("map_headers"))

    def test_strips_bytes_go_rejects(self):
        for raw, expected in (
            ("sq?\x02I", "sq?I"),
            ("\x03lÛT", "lÛT"),
            ("a\x7fb", "ab"),
            ("\x01 key", "key"),
            ("a\tb", "a\tb"),
            ("", ""),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(sanitize_header_value(raw), expected)

    def test_map_headers_keeps_names_and_absent_headers(self):
        self.assertIsNone(map_headers(None, None))
        self.assertEqual(
            map_headers(None, {"Idempotency-Key": "a\x02b", "X-Query-Profile": "1"}),
            {"Idempotency-Key": "ab", "X-Query-Profile": "1"},
        )

    @settings(derandomize=True, database=None, max_examples=500)
    @given(header_values(blacklist_characters="\n\r"))
    def test_generated_values_reach_handler(self, value):
        assert_reaches_handler(self, sanitize_header_value(value))


if __name__ == "__main__":
    unittest.main()
