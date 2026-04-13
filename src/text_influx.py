from datetime import datetime, timezone, timedelta
from influx import build_flux_query, FluxWindow

now = datetime.now(timezone.utc)
start = now - timedelta(hours=12)

print(build_flux_query(
    bucket="my-bucket",
    measurement="cpu",
    window=FluxWindow(start=start),
))
