#!/usr/bin/env python3

import unittest
from pathlib import Path

import yaml
from jsonschema import RefResolver
from openapi_schema_validator import OAS30Validator


class PreparedQuerySchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        openapi_path = Path(__file__).resolve().parents[2] / "openapi.yml"
        cls.schema = yaml.safe_load(openapi_path.read_text())

    def test_create_and_update_validate_filter_instances(self):
        schemas = self.schema["components"]["schemas"]
        for request_name, required_fields in (
            ("CreatePreparedQueryRequest", {"name": "all-accounts", "target": "ACCOUNTS"}),
            ("UpdatePreparedQueryRequest", {}),
        ):
            validator = OAS30Validator(
                schemas[request_name],
                resolver=RefResolver.from_schema(self.schema),
            )
            for filter_value in (None, {"$match": {"address": "world"}}, 'address == "world"'):
                with self.subTest(request=request_name, filter=filter_value):
                    validator.validate({**required_fields, "filter": filter_value})
            with self.subTest(request=request_name, filter="omitted"):
                validator.validate(required_fields)
            for filter_value in ({}, 42, True, []):
                with self.subTest(request=request_name, invalid_filter=filter_value):
                    self.assertTrue(list(validator.iter_errors({**required_fields, "filter": filter_value})))

    def test_execute_parameters_match_handler_accepted_values(self):
        # Mirrors jsonToParameterValue: strings, booleans and exact integers in
        # [int64 min, uint64 max] are forwarded; null, fractions and
        # out-of-range integers are rejected with 400 INVALID_PARAMETERS.
        validator = OAS30Validator(
            self.schema["components"]["schemas"]["ExecutePreparedQueryRequest"],
            resolver=RefResolver.from_schema(self.schema),
        )
        for value in (
            "world",
            "",
            True,
            False,
            0,
            9007199254740993,
            -9223372036854775808,
            18446744073709551615,
        ):
            with self.subTest(valid=value):
                validator.validate({"parameters": {"p": value}})
        for value in (
            None,
            1.5,
            -9223372036854775809,
            18446744073709551616,
            [],
            {},
        ):
            with self.subTest(invalid=value):
                self.assertTrue(list(validator.iter_errors({"parameters": {"p": value}})))


if __name__ == "__main__":
    unittest.main()
