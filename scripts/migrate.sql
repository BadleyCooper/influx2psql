
BEGIN;

DROP TABLE IF EXISTS etl_watermarks;
DROP TABLE IF EXISTS metrics_influx;

-- datatable fron influxdb
CREATE TABLE IF NOT EXISTS metrics_influx (
	time		timestamptz		NOT NULL,
	measurement	text			NOT NULL,
	field		text			NOT NULL,
	host		text			NOT NULL DEFAULT '',
	tags		jsonb			NULL,
	value_num	double precision	NULL,
	value_text	text			NULL,
	ingested_at	timestamptz		NOT NULL DEFAULT now()
);

-- UNIQUE index for point
CREATE UNIQUE INDEX IF NOT EXISTS ux_metrics_influx_point
	ON metrics_influx (time, measurement, field, host);

--time indexes
CREATE INDEX IF NOT EXISTS ix_metrics_influx_time
	ON metrics_influx (time);

CREATE INDEX IF NOT EXISTS ix_metrics_influx_host_time
	ON metrics_influx (host, time);

--watermark tablee
CREATE TABLE IF NOT EXISTS etl_watermarks (
	job_name		text		PRIMARY KEY,
	last_ts			timestamptz 	NOT NULL,
	updated_at		timestamptz	NULL DEFAULT now()
);

COMMIT;
