from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

from transform import MetricRow


def metric_identity(row: MetricRow) -> tuple:
    return (row.time, row.measurement, row.field, row.series_key)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresStore:
    def __init__(
        self,
        dsn: str,
        target_table: str,
        watermark_table: str,
        job_name: str,
    ) -> None:
        self.dsn = dsn
        self.target_table = target_table
        self.watermark_table = watermark_table
        self.job_name = job_name

    def connect(self):
        return psycopg2.connect(self.dsn)

    def get_watermark(self, initial_start_ts: str) -> datetime:
        query = f"""
            SELECT last_ts
            FROM {self.watermark_table}
            WHERE job_name = %s
        """

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.job_name,))
                row = cur.fetchone()

        if row is None:
            from transform import parse_rfc3339
            return parse_rfc3339(initial_start_ts)

        value = row[0]
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    def upsert_metrics(self, metrics: Iterable[MetricRow], page_size: int = 1000) -> int:
    	rows = list(metrics)
    	if not rows:
    		return 0

    	deduped: dict[tuple, MetricRow] = {}
    	for row in rows:
            	deduped[metric_identity(row)] = row

    	rows = list(deduped.values())

    	query = f"""
            INSERT INTO {self.target_table} (
                time,
                measurement,
                field,
                host,
		series_key,
                tags,
                value_num,
                value_text
            )
            VALUES %s
            ON CONFLICT (time, measurement, field, series_key)
            DO UPDATE SET
                tags = EXCLUDED.tags,
                value_num = EXCLUDED.value_num,
                value_text = EXCLUDED.value_text,
                ingested_at = now()
        """

    	values = [
    	(
                row.time,
                row.measurement,
                row.field,
                row.host,
		row.series_key,
                Json(row.tags) if row.tags is not None else None,
                row.value_num,
                row.value_text,
            )
            for row in rows
        ]

    	with self.connect() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, values, page_size=page_size)
            conn.commit()

    	return len(rows)

    def save_watermark(self, last_ts: datetime) -> None:
        query = f"""
            INSERT INTO {self.watermark_table} (
                job_name,
                last_ts,
                updated_at
            )
            VALUES (%s, %s, now())
            ON CONFLICT (job_name)
            DO UPDATE SET
                last_ts = EXCLUDED.last_ts,
                updated_at = now()
        """

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.job_name, last_ts))
            conn.commit()

    def compute_query_start(
        self,
        initial_start_ts: str,
        overlap_seconds: int,
    ) -> datetime:
        watermark = self.get_watermark(initial_start_ts)
        return watermark - timedelta(seconds=overlap_seconds)
