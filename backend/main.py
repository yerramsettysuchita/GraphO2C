"""
GraphO2C backend entry point.

Startup sequence:
  1. Run data ingestion  (JSONL → DuckDB)
  2. Build NetworkX graph from DuckDB tables
  3. Inject graph into FastAPI app
  4. Start Uvicorn server

Usage:
  cd backend
  python main.py

  or for auto-reload during development:
  uvicorn main:app --reload --port 8000
"""

import logging
import os
import sys
import uvicorn
from ingestion import run_ingestion
from graph_builder import build_graph
from api import app, set_graph
from llm import set_graph as set_llm_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
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
    # PORT: Render sets this env var; fall back to CLI arg, then 8000
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 8000))
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=port,
        reload=False,       # reload=True would rebuild the graph on every change
        log_level="info",
    )
