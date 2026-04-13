from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

SYSTEM_COLUMNS = {
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_value",
    "_field",
    "_measurement",
}

@dataclass(frozen=True)
class MetricRow:
    time: datetime
    measurement: str
    field: str
    host: str
    tags: dict[str, str] | None
    value_num: float | None
    value_text: str | None


def parse_rfc3339(value: str) -> datetime:
    """
    Парсит RFC3339 timestamp в timezone-aware datetime.
    Influx обычно отдает время в UTC с суффиксом Z.
    """
    value = value.strip()
    if not value:
        raise ValueError("Empty timestamp")

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def parse_numeric(value: str) -> float | None:
    """
    Пытается распарсить значение как число.
    Если не получается, возвращает None.
    """
    value = value.strip()
    if value == "":
        return None

    try:
        return float(value)
    except ValueError:
        return None


def extract_tags(row: dict[str, str]) -> dict[str, str]:
    """
    Забирает все не-системные колонки как tags.
    Например: host, region, env, instance и т.д.
    """
    tags: dict[str, str] = {}

    for key, value in row.items():
        if key in SYSTEM_COLUMNS:
            continue
        if value is None:
            continue

        value = value.strip()
        if value == "":
            continue

        tags[key] = value

    return tags


def transform_row(row: dict[str, str]) -> MetricRow:
    """
    Превращает одну строку annotated CSV в нормализованный объект.
    """
    time_raw = row.get("_time", "").strip()
    measurement = row.get("_measurement", "").strip()
    field = row.get("_field", "").strip()
    value_raw = row.get("_value", "")

    if not time_raw:
        raise ValueError("Row is missing _time")

    if not measurement:
        raise ValueError("Row is missing _measurement")

    if not field:
        raise ValueError("Row is missing _field")

    time = parse_rfc3339(time_raw)
    tags = extract_tags(row)
    host = tags.get("host", "")

    value_num = parse_numeric(value_raw)
    value_text = None

    if value_num is None:
        text = value_raw.strip()
        value_text = text if text != "" else None

    return MetricRow(
        time=time,
        measurement=measurement,
        field=field,
        host=host,
        tags=tags or None,
        value_num=value_num,
        value_text=value_text,
    )
