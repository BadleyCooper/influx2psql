from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator
import csv
import io
import requests

def to_rfc3339(dt: datetime) -> str:
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def quote_flux_string(value: str) -> str:
	escaped = value.replace("\\", "\\\\").replace('"', '\\"')
	return f'"{escaped}"'

@dataclass(frozen=True)
class FluxWindow:
	start: datetime
	stop: datetime | None = None

def build_flux_query(
	bucket: str,
	measurement: str,
	window: FluxWindow,
) -> str:
	start_expr = to_rfc3339(window.start)
	if window.stop is None:
		stop_expr = "now()"
	else:
		stop_expr = to_rfc3339(window.stop)
	bucket_q = quote_flux_string(bucket)
	measurement_q = quote_flux_string(measurement)

	return f"""
from(bucket: {bucket_q})
	|> range(start: {start_expr}, stop: {stop_expr})
	|> filter(fn: (r) => r._measurement == {measurement_q})
""".strip()

class InfluxClient:
	def __init__(
		self,
		base_url: str,
		org: str,
		token: str,
		session: requests.Session,
) -> None:
		self.base_url = base_url.rstrip("/")
		self.org = org
		self.session = session
		self.headers = {
			"Authorization": f"Token {token}",
			"Accept": "application/csv",
			"Content-Type": "application/vnd.flux",
		}
	
	def query_csv_text(self, flux: str, timeout_seconds: int) -> str:
		url = f"{self.base_url}/api/v2/query"
		response = self.session.post(
			url,
			params={"org": self.org},
			headers=self.headers,
			data=flux.encode("utf-8"),
			timeout=timeout_seconds,
		)
		response.raise_for_status()
		
		return response.text

	def iter_csv_rows(self, flux: str, timeout_seconds: int) -> Iterator[dict[str, str]]:
		raw_csv = self.query_csv_text(flux, timeout_seconds=timeout_seconds)
		yield from parse_annotated_csv(raw_csv)

def parse_annotated_csv(raw_csv: str) -> Iterator[dict[str, str]]:
	"""
	Упрощённый парсер annotated CSV от InfluxDB.

 	Он:
	- пропускает annotation rows (#datatype, #group, #default);
	- пропускает пустые строки;
	- корректно переживает несколько таблиц в одном ответе;
	- на каждой новой header row пересоздаёт DictReader.
	"""
	lines = raw_csv.splitlines()
	current_header: list[str] | None = None
	buffer: list[str] = []

	def flush_buffer() -> Iterator[dict[str, str]]:
		nonlocal buffer
		if not buffer:
			return
		text = "\n".join(buffer) + "\n"
		reader = csv.DictReader(io.StringIO(text))
		for row in reader:
			yield {k: v for k, v in row.items() if k is not None}
		buffer = []

	for line in lines:
		if not line.strip():
			yield from flush_buffer()
			current_header = None
			continue
		if line.startswith("#"):
			continue
		row = next(csv.reader([line]))
		is_header = "_time" in row and "_value" in row
		if is_header:
			yield from flush_buffer()
			current_header = row
			buffer = [line]
			continue
		if current_header is not None:
			buffer.append(line)

	yield from flush_buffer()
