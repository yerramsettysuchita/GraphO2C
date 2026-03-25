"""
In-process operational metrics for GraphO2C.
Imported by both api.py and llm.py — no circular dependency.
"""
from datetime import datetime, timezone

startup_time: datetime = datetime.now(timezone.utc)
query_count: int = 0
query_errors: int = 0
rate_limit_hits: int = 0
