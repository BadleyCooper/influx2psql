from datetime import timezone

from transform import parse_rfc3339, parse_numeric, transform_row


def test_parse_rfc3339_zulu() -> None:
    dt = parse_rfc3339("2026-04-01T12:34:56Z")
    assert dt.tzinfo is not None
    assert dt.astimezone(timezone.utc).isoformat() == "2026-04-01T12:34:56+00:00"


def test_parse_numeric_ok() -> None:
    assert parse_numeric("123") == 123.0
    assert parse_numeric("12.5") == 12.5


def test_parse_numeric_fail() -> None:
    assert parse_numeric("abc") is None
    assert parse_numeric("") is None


def test_transform_row_numeric() -> None:
    row = {
        "result": "",
        "table": "0",
        "_start": "2026-04-01T00:00:00Z",
        "_stop": "2026-04-02T00:00:00Z",
        "_time": "2026-04-01T12:00:00Z",
        "_value": "42.5",
        "_field": "usage_idle",
        "_measurement": "cpu",
        "host": "srv-1",
        "region": "eu-west",
    }

    metric = transform_row(row)

    assert metric.measurement == "cpu"
    assert metric.field == "usage_idle"
    assert metric.host == "srv-1"
    assert metric.value_num == 42.5
    assert metric.value_text is None
    assert metric.tags == {"host": "srv-1", "region": "eu-west"}


def test_transform_row_text() -> None:
    row = {
        "_time": "2026-04-01T12:00:00Z",
        "_value": "ok",
        "_field": "status",
        "_measurement": "service",
        "host": "srv-2",
    }

    metric = transform_row(row)

    assert metric.value_num is None
    assert metric.value_text == "ok"
    assert metric.host == "srv-2"
