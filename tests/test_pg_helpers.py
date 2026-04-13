from datetime import datetime, timezone

from pg import PostgresStore


def test_compute_query_start(monkeypatch) -> None:
    store = PostgresStore(
        dsn="postgresql://x",
        target_table="metrics_influx",
        watermark_table="etl_watermarks",
        job_name="job1",
    )

    fake_ts = datetime(2026, 4, 2, 0, 0, 0, tzinfo=timezone.utc)

    def fake_get_watermark(initial_start_ts: str):
        return fake_ts

    monkeypatch.setattr(store, "get_watermark", fake_get_watermark)

    result = store.compute_query_start(
        initial_start_ts="2026-01-01T00:00:00Z",
        overlap_seconds=300,
    )

    assert result == datetime(2026, 4, 1, 23, 55, 0, tzinfo=timezone.utc)
