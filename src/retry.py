from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def build_retry_session(
	retries: int,
	backoff_factor: int,
	status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
	retry = Retry(
		total=retries,
		connect=retries,
		read=retries,
		status=retries,
		backoff_factor=backoff_factor,
		status_forcelist=status_forcelist,
		allowed_methods=frozenset({"GET", "POST"}),
		raise_on_status=False,
	)

	adapter = HTTPAdapter(max_retries=retry)

	session = requests.Session()
	session.mount("http://", adapter)
	session.mount("https://", adapter)

	return session
