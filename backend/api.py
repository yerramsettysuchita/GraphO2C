"""
FastAPI application for GraphO2C.

Endpoints:
  GET  /health                            Health check
  GET  /graph/summary                     Node + edge counts by type
  GET  /graph/nodes?type=X&limit=50       List nodes of a given type
  GET  /graph/node/{node_id}              Node properties + 1-hop neighbors
  GET  /graph/path?from=X&to=Y            Shortest path between two nodes
  POST /query                             LLM-powered natural language query
"""

from __future__ import annotations

import logging
import os
import secrets
import time
import uuid
from collections import defaultdict
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import psutil
import sentry_sdk
import metrics as _metrics
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

# ---------------------------------------------------------------------------
# Request ID — propagated through ContextVar so every log line carries it
# ---------------------------------------------------------------------------

request_id_var: ContextVar[str] = ContextVar("request_id", default="--------")


class RequestIdFilter(logging.Filter):
    """Injects request_id into every log record from ContextVar."""
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("--------")  # type: ignore[attr-defined]
        return True

_FRONTEND_DIR  = Path(__file__).parent.parent / "frontend"

logger = logging.getLogger(__name__)

# The graph is injected at startup by main.py
_graph: nx.DiGraph | None = None


def set_graph(G: nx.DiGraph) -> None:
    global _graph
    _graph = G


def _get_graph() -> nx.DiGraph:
    if _graph is None:
        raise HTTPException(status_code=503, detail="Graph not yet initialized")
    return _graph


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="GraphO2C API",
    description="Graph-Based Order-to-Cash query interface",
    version="0.1.0",
)

class _RequestLogger(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "%s %s -> %d  (%.1f ms)",
            request.method, request.url.path, response.status_code, elapsed_ms,
        )
        return response


app.add_middleware(_RequestLogger)

# CORS — restrict to known origins in production
_ALLOWED_ORIGINS = [
    "http://localhost:5173",   # Vite dev server
    "http://localhost:8000",   # FastAPI serving built frontend
    "http://127.0.0.1:8000",
]
_prod_url = os.environ.get("RENDER_EXTERNAL_URL")
if _prod_url:
    _ALLOWED_ORIGINS.append(_prod_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key", "Authorization"],
)

# API key auth — only active when GRAPHO2C_API_KEY env var is set.
# /query (POST) is the only protected endpoint.
# All GET endpoints and static frontend remain public.
_API_KEY = os.environ.get("GRAPHO2C_API_KEY")


@app.middleware("http")
async def auth_middleware(request: StarletteRequest, call_next):
    needs_auth = (
        request.url.path == "/query"
        and request.method == "POST"
        and _API_KEY is not None
    )
    if needs_auth:
        provided = (
            request.headers.get("X-API-Key")
            or request.headers.get("Authorization", "").replace("Bearer ", "")
        )
        if not provided or not secrets.compare_digest(provided, _API_KEY):
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "message": "X-API-Key header required for /query",
                    "hint": "Add header: X-API-Key: <your key>",
                },
            )
    return await call_next(request)


@app.middleware("http")
async def request_id_middleware(request: StarletteRequest, call_next):
    """Assign a short ID to every request for log correlation."""
    req_id = str(uuid.uuid4())[:8]
    request_id_var.set(req_id)
    sentry_sdk.set_tag("request_id", req_id)
    response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str


class NeighborInfo(BaseModel):
    node_id: str
    node_type: str
    edge_type: str
    direction: str  # "outgoing" | "incoming"
    edge_properties: dict[str, Any]


class NodeDetail(BaseModel):
    node_id: str
    node_type: str
    properties: dict[str, Any]
    neighbors: list[NeighborInfo]


class PathResponse(BaseModel):
    source: str
    target: str
    path: list[str]
    path_length: int
    node_types: list[str]
    edges: list[dict[str, Any]]
    directed: bool = True
    warning: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node_props(G: nx.DiGraph, node_id: str) -> dict[str, Any]:
    """Return node attributes as a plain serializable dict."""
    raw = dict(G.nodes[node_id])
    return {k: v for k, v in raw.items() if k != "node_type"}


def _safe_props(data: dict) -> dict[str, Any]:
    """Strip non-serializable values from an edge-data dict."""
    return {k: v for k, v in data.items() if k != "edge_type"}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    G = _get_graph()
    return {
        "status": "ok",
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
    }


@app.get("/graph/summary")
def graph_summary():
    """Return node counts and edge counts grouped by type."""
    G = _get_graph()

    node_counts: dict[str, int] = defaultdict(int)
    for _, data in G.nodes(data=True):
        node_counts[data.get("node_type", "Unknown")] += 1

    edge_counts: dict[str, int] = defaultdict(int)
    for _, _, data in G.edges(data=True):
        edge_counts[data.get("edge_type", "Unknown")] += 1

    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "nodes_by_type": dict(sorted(node_counts.items())),
        "edges_by_type": dict(sorted(edge_counts.items())),
    }


@app.get("/graph/nodes")
def list_nodes(
    type: str = Query(..., description="Node type, e.g. SalesOrder, Customer"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return a paginated list of nodes of the given type with their properties."""
    G = _get_graph()
    matching = [
        (nid, data)
        for nid, data in G.nodes(data=True)
        if data.get("node_type") == type
    ]
    total = len(matching)
    page = matching[offset : offset + limit]
    return {
        "node_type": type,
        "total": total,
        "offset": offset,
        "limit": limit,
        "nodes": [
            {"node_id": nid, "properties": {k: v for k, v in d.items() if k != "node_type"}}
            for nid, d in page
        ],
    }


@app.get("/graph/node/{node_id:path}", response_model=NodeDetail)
def get_node(node_id: str):
    """
    Return full node properties plus all 1-hop neighbors (both directions).
    """
    G = _get_graph()
    if not G.has_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")

    node_data = G.nodes[node_id]
    neighbors: list[NeighborInfo] = []

    # Outgoing edges
    for _, dst, edata in G.out_edges(node_id, data=True):
        neighbors.append(
            NeighborInfo(
                node_id=dst,
                node_type=G.nodes[dst].get("node_type", "Unknown"),
                edge_type=edata.get("edge_type", ""),
                direction="outgoing",
                edge_properties=_safe_props(edata),
            )
        )

    # Incoming edges
    for src, _, edata in G.in_edges(node_id, data=True):
        neighbors.append(
            NeighborInfo(
                node_id=src,
                node_type=G.nodes[src].get("node_type", "Unknown"),
                edge_type=edata.get("edge_type", ""),
                direction="incoming",
                edge_properties=_safe_props(edata),
            )
        )

    return NodeDetail(
        node_id=node_id,
        node_type=node_data.get("node_type", "Unknown"),
        properties=_node_props(G, node_id),
        neighbors=neighbors,
    )


@app.get("/graph/path", response_model=PathResponse)
def get_path(
    source: str = Query(..., alias="from", description="Source node ID"),
    target: str = Query(..., alias="to",   description="Target node ID"),
):
    """
    Return the shortest directed path between two nodes.
    Falls back to undirected search if no directed path exists.
    """
    G = _get_graph()

    if not G.has_node(source):
        raise HTTPException(status_code=404, detail=f"Source node '{source}' not found")
    if not G.has_node(target):
        raise HTTPException(status_code=404, detail=f"Target node '{target}' not found")

    directed = True
    undirected_warning: Optional[str] = None

    try:
        path = nx.shortest_path(G, source, target)
    except nx.NetworkXNoPath:
        directed = False
        # Try undirected fallback
        try:
            path = nx.shortest_path(G.to_undirected(), source, target)
            undirected_warning = (
                "No directed path found. Showing undirected path — "
                "may include reverse traversals that are not valid O2C flow."
            )
        except nx.NetworkXNoPath:
            raise HTTPException(
                status_code=404,
                detail=f"No path found between '{source}' and '{target}'",
            )

    node_types = [G.nodes[n].get("node_type", "Unknown") for n in path]

    # Collect edge metadata along the path
    edges: list[dict[str, Any]] = []
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        if G.has_edge(u, v):
            edata = dict(G[u][v])
        else:
            # Undirected fallback path may use reversed edges
            edata = dict(G[v][u]) if G.has_edge(v, u) else {}
        edges.append({"from": u, "to": v, **edata})

    return PathResponse(
        source=source,
        target=target,
        path=path,
        path_length=len(path) - 1,
        node_types=node_types,
        edges=edges,
        directed=directed,
        warning=undirected_warning,
    )


@app.post("/query")
def llm_query(body: QueryRequest):
    """
    LLM-powered natural language query.

    Two-step pipeline (see llm.py):
      1. LLM classifies question and generates DuckDB SQL.
      2. SQL is executed; results fed back to LLM for answer synthesis.

    Returns grounded natural-language answer plus metadata for graph
    highlighting (nodes_referenced) and debugging (sql_executed).
    """
    from llm import run_query
    try:
        result = run_query(body.question)
        # Tag the active Sentry transaction so traces are filterable
        sentry_sdk.set_tag("query_type", result.get("query_type", "unknown"))
        sentry_sdk.set_measurement(
            "nodes_referenced",
            len(result.get("nodes_referenced", [])),
        )
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.exception("Unexpected error in /query")
        raise HTTPException(status_code=500, detail=f"Internal error: {exc}")


@app.get("/metrics")
def operational_metrics():
    """
    Live operational snapshot: uptime, query counts, memory, graph size.
    Read-only — no auth required.
    """
    uptime = (datetime.now(timezone.utc) - _metrics.startup_time).total_seconds()
    try:
        mem_mb = round(psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024, 1)
    except Exception:
        mem_mb = -1

    try:
        G = _get_graph()
        graph_info: dict[str, Any] = {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
        }
    except HTTPException:
        graph_info = {"nodes": 0, "edges": 0, "status": "initialising"}

    return {
        "status": "ok",
        "uptime_seconds": round(uptime),
        "uptime_human": f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m",
        "graph": graph_info,
        "queries": {
            "total": _metrics.query_count,
            "errors": _metrics.query_errors,
            "rate_limit_hits": _metrics.rate_limit_hits,
            "error_rate_pct": round(
                _metrics.query_errors / max(_metrics.query_count, 1) * 100, 1
            ),
        },
        "memory_mb": mem_mb,
        "environment": os.environ.get("ENVIRONMENT", "development"),
        "version": os.environ.get("RENDER_GIT_COMMIT", "local"),
    }


# ---------------------------------------------------------------------------
# Frontend — serve built React app (frontend-dist/) at /.
# Falls back to legacy single-file frontend when dist is not built yet.
# Must be registered AFTER all API routes so API paths take priority.
# ---------------------------------------------------------------------------

_FRONTEND_DIST = Path(__file__).parent.parent / "frontend-dist"

if _FRONTEND_DIST.exists():
    # Production: serve Vite-built React app
    app.mount(
        "/",
        StaticFiles(directory=str(_FRONTEND_DIST), html=True),
        name="frontend",
    )
elif _FRONTEND_DIR.exists():
    # Dev fallback: legacy single index.html
    @app.get("/", include_in_schema=False)
    def root():
        index = _FRONTEND_DIR / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return {"message": "GraphO2C API", "docs": "/docs"}

    app.mount(
        "/app",
        StaticFiles(directory=str(_FRONTEND_DIR), html=True),
        name="frontend",
    )
