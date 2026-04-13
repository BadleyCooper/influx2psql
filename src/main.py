from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

from config import load_settings
from influx import FluxWindow, InfluxClient, build_flux_query
from pg import PostgresStore
from retry import build_retry_session
from transform import MetricRow, transform_row


logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def chunked(iterable: Iterable[MetricRow], size: int):
    batch: list[MetricRow] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def run() -> int:
    settings = load_settings()
    setup_logging(settings.log_level)

    logger.info("ETL started")
    logger.info(
        "Settings: measurement=%s batch_size=%s overlap_seconds=%s dry_run=%s job_name=%s",
        settings.influx_measurement,
        settings.batch_size,
        settings.overlap_seconds,
        settings.dry_run,
        settings.pg_job_name,
    )

    store = PostgresStore(
        dsn=settings.pg_dsn,
        target_table=settings.pg_target_table,
        watermark_table=settings.pg_watermark_table,
        job_name=settings.pg_job_name,
    )

    query_start = store.compute_query_start(
        initial_start_ts=settings.initial_start_ts,
        overlap_seconds=settings.overlap_seconds,
    )
    query_stop = datetime.now(timezone.utc)

    logger.info(
        "Computed query window: start=%s stop=%s",
        query_start.isoformat(),
        query_stop.isoformat(),
    )

    flux = build_flux_query(
        bucket=settings.influx_bucket,
        measurement=settings.influx_measurement,
        window=FluxWindow(start=query_start, stop=query_stop),
    )

    logger.info("Flux query built")
    logger.debug("Flux query:\n%s", flux)

    session = build_retry_session(
        retries=settings.http_retries,
        backoff_factor=settings.http_backoff_factor,
    )

    client = InfluxClient(
        base_url=settings.influx_url,
        org=settings.influx_org,
        token=settings.influx_token,
        session=session,
    )

    raw_rows = client.iter_csv_rows(
        flux=flux,
        timeout_seconds=settings.http_timeout_seconds,
    )

    total_input_rows = 0
    skipped_rows = 0
    processed_rows = 0
    max_seen_ts: datetime | None = None

    def safe_transform_rows():
        nonlocal total_input_rows, skipped_rows
        for row in raw_rows:
            total_input_rows += 1
            try:
                yield transform_row(row)
            except Exception:
                skipped_rows += 1
                logger.exception("Failed to transform row: %s", row)

    transformed_iter = safe_transform_rows()

    for batch in chunked(transformed_iter, settings.batch_size):
        if not batch:
            continue

        batch_max_ts = max(item.time for item in batch)

        if settings.dry_run:
            written = len(batch)
            logger.info(
                "Dry run batch: batch_size=%s batch_max_ts=%s",
                len(batch),
                batch_max_ts.isoformat(),
            )
        else:
            written = store.upsert_metrics(batch, page_size=settings.batch_size)
            logger.info(
                "Batch processed: batch_size=%s written=%s batch_max_ts=%s",
                len(batch),
                written,
                batch_max_ts.isoformat(),
            )

        processed_rows += written

        if max_seen_ts is None or batch_max_ts > max_seen_ts:
            max_seen_ts = batch_max_ts

    if max_seen_ts is not None:
        if settings.dry_run:
            logger.info(
                "Dry run mode: watermark would be updated to %s",
                max_seen_ts.isoformat(),
            )
        else:
            store.save_watermark(max_seen_ts)
            logger.info("Watermark updated: last_ts=%s", max_seen_ts.isoformat())
    else:
        logger.info("No data returned; watermark not changed")

    logger.info(
        "ETL finished: total_input_rows=%s skipped_rows=%s processed_rows=%s max_seen_ts=%s",
        total_input_rows,
        skipped_rows,
        processed_rows,
        max_seen_ts.isoformat() if max_seen_ts else None,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
