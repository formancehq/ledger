#!/usr/bin/env python3

import unittest
from pathlib import Path

import yaml


class PreparedQuerySchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        openapi_path = Path(__file__).resolve().parents[2] / "openapi.yml"
        cls.schema = yaml.safe_load(openapi_path.read_text())

    def test_filter_input_models_null_with_an_explicit_local_type(self):
        filter_input = self.schema["components"]["schemas"]["PreparedQueryFilterInput"]
        nullable_branch = filter_input["oneOf"][0]

        # OpenAPI 3.0 only applies `nullable` when `type` is declared in the
        # same Schema Object. Keeping both keys here ensures generated clients
        # and request validators admit the documented JSON null contract.
        self.assertEqual("object", nullable_branch["type"])
        self.assertIs(True, nullable_branch["nullable"])
        self.assertEqual(
            "#/components/schemas/QueryFilter",
            nullable_branch["allOf"][0]["$ref"],
        )

    def test_create_and_update_use_the_nullable_filter_input(self):
        expected_ref = "#/components/schemas/PreparedQueryFilterInput"
        schemas = self.schema["components"]["schemas"]

        self.assertEqual(expected_ref, schemas["CreatePreparedQueryRequest"]["properties"]["filter"]["$ref"])
        self.assertEqual(expected_ref, schemas["UpdatePreparedQueryRequest"]["properties"]["filter"]["$ref"])


if __name__ == "__main__":
    unittest.main()
