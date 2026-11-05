#!/usr/bin/env python3
"""Validate analysis event envelopes with the pinned SSE response validator."""

import json
import unittest
from pathlib import Path

import requests
import schemathesis
import yaml
from jsonschema import RefResolver
from openapi_schema_validator import OAS30Validator
from schemathesis.core.transport import Response
from schemathesis.openapi.checks import JsonSchemaError


class AnalysisStreamSchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[2] / "openapi.yml"
        cls.document = yaml.safe_load(path.read_text())
        cls.schema = schemathesis.openapi.from_path(str(path))

    def test_analysis_events_conform(self):
        for resource, component, result in (
            ("accounts", "AccountAnalysisEventStream", {"data": {"patterns": [], "totalAccounts": 0}}),
            ("transactions", "TransactionAnalysisEventStream", {"data": {"flowPatterns": [], "totalTransactions": 0, "totalReverted": 0}}),
        ):
            operation = self.schema[f"/v3/{{ledgerName}}/analyze-{resource}"]["GET"]
            payloads = {
                "started": {},
                "progress": {"processed": 0, "total": 0, "phase": "scanning" if resource == "accounts" else ""},
                "result": result,
                "error": {"errorCode": "INTERNAL", "errorMessage": "Internal error", "status": 500},
            }
            for event, payload in payloads.items():
                with self.subTest(resource=resource, event=event):
                    data = json.dumps(payload)
                    self.validate_frame(operation, f"event: {event}\ndata: {data}\n\n")
                    # OpenAPI 3.0 has no contentSchema keyword: payload contracts
                    # remain named separately from the string-valued SSE data.
                    payload_schema = self.document["components"]["schemas"][component]["x-sse-events"][event]
                    OAS30Validator(payload_schema, resolver=RefResolver.from_schema(self.document)).validate(payload)

    def test_unknown_event_is_rejected(self):
        for resource in ("accounts", "transactions"):
            operation = self.schema[f"/v3/{{ledgerName}}/analyze-{resource}"]["GET"]
            with self.subTest(resource=resource), self.assertRaisesRegex(JsonSchemaError, "not one of"):
                self.validate_frame(operation, 'event: unknown\ndata: {}\n\n')

    @staticmethod
    def validate_frame(operation, frame):
        response = Response(
            status_code=200,
            headers={"content-type": ["text/event-stream"]},
            content=frame.encode(),
            request=requests.Request("GET", "http://localhost/v3/test-ledger/analyze-accounts").prepare(),
            elapsed=0,
            verify=False,
        )
        operation.validate_response(response)


if __name__ == "__main__":
    unittest.main()
