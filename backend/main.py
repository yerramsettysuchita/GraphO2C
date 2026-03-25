"""
GraphO2C backend entry point.

Startup sequence:
  1. Initialise Sentry APM (before all project imports)
  2. Run data ingestion  (JSONL → DuckDB)
  3. Build NetworkX graph from DuckDB tables
  4. Inject graph into FastAPI app
  5. Start Uvicorn server

Usage:
  cd backend
  python main.py

  or for auto-reload during development:
  uvicorn main:app --reload --port 8000
"""

import logging
import os
import sys

# ---------------------------------------------------------------------------
# Sentry — initialise BEFORE project imports so FastAPI/Starlette integration
# can instrument the app object at creation time.
# DSN is read from environment; when absent Sentry is a no-op.
# ---------------------------------------------------------------------------
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),          # None → disabled (local dev)
    traces_sample_rate=0.1,                     # 10 % of requests traced
    profiles_sample_rate=0.1,                   # 10 % profiled
    integrations=[
        FastApiIntegration(transaction_style="endpoint"),
        StarletteIntegration(transaction_style="endpoint"),
    ],
    environment=os.environ.get("ENVIRONMENT", "development"),
    release=os.environ.get("RENDER_GIT_COMMIT", "local"),
)

# ---------------------------------------------------------------------------
# Project imports (after Sentry so instrumentation is active)
# ---------------------------------------------------------------------------
import uvicorn
import metrics as _metrics
from ingestion import run_ingestion
from graph_builder import build_graph
from api import app, set_graph, RequestIdFilter   # noqa: E402
from llm import set_graph as set_llm_graph

# ---------------------------------------------------------------------------
# Logging — include request_id from ContextVar in every log line
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(request_id)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _handler in logging.root.handlers:
    _handler.addFilter(RequestIdFilter())

logger = logging.getLogger(__name__)


def startup() -> None:
    logger.info("=" * 60)
    logger.info(" GraphO2C Backend")
    logger.info("=" * 60)

    # Step 1: Ingest all JSONL files into DuckDB
    run_ingestion()

    # Step 2: Build the in-memory graph
    G = build_graph()

    # Step 3: Hand the graph to both the API layer and the LLM layer
    set_graph(G)
    set_llm_graph(G)

    logger.info("Graph loaded and API ready.")


# Run ingestion + graph build synchronously before uvicorn starts
startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 8000))
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )
