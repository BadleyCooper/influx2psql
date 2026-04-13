import os

from dotenv import load_dotenv
from dataclasses import dataclass

def _parse_bool(value: str | None, default: bool = False) -> bool:
	if value is None:
		return default
	return value.strip().lower() in {"1", "true", "yes", "on"}

def _require_env(name: str) -> str:
	value = os.getenv(name)
	if value is None or value.strip() == "":
		raise ValueError(f"Missing required env var: {name}")
	return value.strip()

@dataclass(frozen=True)
class Settings:
	# InfluxDB
	influx_url: str
	influx_org: str
	influx_bucket: str
	influx_token: str
	influx_measurement: str

	#PGSQL
	pg_dsn: str
	pg_target_table: str
	pg_watermark_table: str
	pg_job_name: str

	#ETL
	initial_start_ts: str
	overlap_seconds: int
	batch_size: int
	dry_run: bool

	#HTTP
	http_timeout_seconds: int
	http_retries: int
	http_backoff_factor: float

	#Logging
	log_level: str
	structured_logs: bool
	prometheus_enabled: bool
	prometheus_port: int

def load_settings(env_file: str | None = None) -> Settings:
	if env_file:
		load_dotenv(env_file)
	else:
		load_dotenv()

	settings = Settings(
		#InflxDB
		influx_url=_require_env("INFLUX_URL").rstrip("/"),
		influx_org=_require_env("INFLUX_ORG"),
		influx_bucket=_require_env("INFLUX_BUCKET"),
		influx_token=_require_env("INFLUX_TOKEN"),
		influx_measurement=_require_env("INFLUX_MEASUREMENT"),

		#PGSQL
		pg_dsn=_require_env("PG_DSN"),
		pg_target_table=os.getenv("PG_TARGET_TABLE", "metrics_influx").strip(),
	        pg_watermark_table=os.getenv("PG_WATERMARK_TABLE", "etl_watermarks").strip(),
       		pg_job_name=os.getenv("PG_JOB_NAME", "influx2pg").strip(),

		
     		#ETL
        	initial_start_ts=os.getenv("INITIAL_START_TS", "1970-01-01T00:00:00Z").strip(),
        	overlap_seconds=int(os.getenv("OVERLAP_SECONDS", "300")),
        	batch_size=int(os.getenv("BATCH_SIZE", "5000")),
		dry_run=_parse_bool(os.getenv("DRY_RUN", default=False)),

        	#HTTP
        	http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
        	http_retries=int(os.getenv("HTTP_RETRIES", "5")),
        	http_backoff_factor=float(os.getenv("HTTP_BACKOFF_FACTOR", "0.5")),

        	#Logging
        	log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        	structured_logs=_parse_bool(os.getenv("STRUCTURED_LOGS"), default=False),
        	prometheus_enabled=_parse_bool(os.getenv("PROMETHEUS_ENABLED"), default=False),
        	prometheus_port=int(os.getenv("PROMETHEUS_PORT", "8000")),
    	)

	if settings.overlap_seconds < 0:
		raise ValueError(f"OVERLAP_SECONDS must be >= 0")
		
	if settings.batch_size <= 0:
		raise ValueError(f"BATCH_SIZE must be > 0")

	if settings.http_timeout_seconds <=0:
		raise ValueError(f"HTTP_TIMEOUT_SECONDS must be > 0")

	if settings.http_retries < 0:
		raise ValueError(f"HTTP_RETRIES must be >= 0")

	if settings.prometheus_port < 0:
		raise ValueError(f"PROMETHEUS_PORT must be > 0")

	return settings
