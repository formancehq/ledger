from types import SimpleNamespace

import test_api


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def send(self, case, base_url):
        self.calls.append((case, base_url))
        return self.responses.pop(0)


def historical_case(responses):
    transport = FakeTransport(responses)
    schema = SimpleNamespace(
        base_url="http://ledger.test",
        transport=transport,
    )
    case = SimpleNamespace(
        path="/v3/{ledgerName}/volumes",
        method="GET",
        query={"at": "2026-01-15T12:30:45Z"},
        operation=SimpleNamespace(schema=schema),
    )
    return case, transport


def test_wait_for_historical_read_retries_only_transient_states(monkeypatch):
    monkeypatch.setattr(test_api.time, "sleep", lambda _: None)
    case, transport = historical_case(
        [
            FakeResponse(503, {"errorCode": "HISTORY_BUILDING"}),
            FakeResponse(503, {"errorCode": "HISTORY_BEHIND"}),
            FakeResponse(200, {}),
        ]
    )

    test_api._wait_for_historical_read(case)

    assert len(transport.calls) == 3
    assert all(base_url == "http://ledger.test" for _, base_url in transport.calls)


def test_wait_for_historical_read_keeps_other_server_errors_blocking(monkeypatch):
    monkeypatch.setattr(test_api.time, "sleep", lambda _: None)
    case, transport = historical_case(
        [FakeResponse(503, {"errorCode": "ANOTHER_UNAVAILABLE_STATE"})]
    )

    test_api._wait_for_historical_read(case)

    assert len(transport.calls) == 1


def test_wait_for_historical_read_is_bounded(monkeypatch):
    monkeypatch.setattr(test_api, "HISTORICAL_READ_RETRY_ATTEMPTS", 3)
    monkeypatch.setattr(test_api.time, "sleep", lambda _: None)
    case, transport = historical_case(
        [
            FakeResponse(503, {"errorCode": "HISTORY_BUILDING"}),
            FakeResponse(503, {"errorCode": "HISTORY_BUILDING"}),
            FakeResponse(503, {"errorCode": "HISTORY_BUILDING"}),
        ]
    )

    test_api._wait_for_historical_read(case)

    assert len(transport.calls) == 3


def test_wait_for_historical_read_ignores_non_historical_cases():
    case, transport = historical_case([])
    case.query = {}

    test_api._wait_for_historical_read(case)

    assert transport.calls == []
