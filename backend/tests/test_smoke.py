"""
Smoke tests for GraphO2C.

Tests are split into two groups:
  - Dataset-dependent tests (TestIngestion, TestGraph): skip in CI
    where Dataset/ JSONL files may not be present.
  - No-dataset tests (TestGuardrails, TestConstants, TestAPIEndpoints):
    always run, including in CI.

Run locally:   cd backend && pytest tests/ -v
Run in CI:     cd backend && pytest tests/ -v  (dataset tests auto-skipped)
"""

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ---------------------------------------------------------------------------
# CI detection + dataset availability
# ---------------------------------------------------------------------------

IS_CI = os.environ.get('CI') == 'true'

DATASET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    '..', 'Dataset',
)
HAS_DATASET = os.path.exists(DATASET_PATH)

requires_dataset = pytest.mark.skipif(
    not HAS_DATASET,
    reason="Dataset/ not available — run locally with full dataset",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def db():
    if not HAS_DATASET:
        pytest.skip("Dataset not available")
    from ingestion import run_ingestion
    run_ingestion()
    from db import get_connection
    return get_connection()


@pytest.fixture(scope="session")
def graph(db):
    from graph_builder import build_graph
    return build_graph()


# ---------------------------------------------------------------------------
# Tests: Ingestion counts (require Dataset)
# ---------------------------------------------------------------------------


class TestIngestion:
    @requires_dataset
    def test_sales_order_count(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM sales_order_headers"
        ).fetchone()[0]
        assert count == 100, f"Expected 100 SOs, got {count}"

    @requires_dataset
    def test_billing_doc_count(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM billing_document_headers"
        ).fetchone()[0]
        assert count == 163, f"Expected 163 BDs, got {count}"

    @requires_dataset
    def test_customer_view(self, db):
        count = db.execute("SELECT COUNT(*) FROM v_customer").fetchone()[0]
        assert count == 8, f"Expected 8 customers, got {count}"

    @requires_dataset
    def test_product_view(self, db):
        count = db.execute("SELECT COUNT(*) FROM v_product").fetchone()[0]
        assert count == 69, f"Expected 69 products, got {count}"


# ---------------------------------------------------------------------------
# Tests: Graph integrity (require Dataset)
# ---------------------------------------------------------------------------


class TestGraph:
    @requires_dataset
    def test_node_count(self, graph):
        assert graph.number_of_nodes() == 1262, \
            f"Expected 1262 nodes, got {graph.number_of_nodes()}"

    @requires_dataset
    def test_edge_count(self, graph):
        assert graph.number_of_edges() == 2059, \
            f"Expected 2059 edges, got {graph.number_of_edges()}"

    @requires_dataset
    def test_cancels_edges(self, graph):
        cancels = [
            (u, v) for u, v, d in graph.edges(data=True)
            if d.get('edge_type') == 'CANCELS'
        ]
        assert len(cancels) == 80, \
            f"Expected 80 CANCELS edges, got {len(cancels)}"

    @requires_dataset
    def test_full_o2c_path(self, graph):
        import networkx as nx
        path = nx.shortest_path(
            graph,
            'SalesOrder_740509',
            'Payment_ABCD_2025_9400000205_1',
        )
        assert len(path) == 7, \
            f"Expected 7-hop O2C path, got {len(path)}"

    @requires_dataset
    def test_all_node_types_present(self, graph):
        types = {d.get('node_type') for _, d in graph.nodes(data=True)}
        required = {
            'Customer', 'SalesOrder', 'BillingDocument',
            'JournalEntry', 'Payment', 'OutboundDelivery',
            'Product', 'Plant',
        }
        missing = required - types
        assert not missing, f"Missing node types: {missing}"


# ---------------------------------------------------------------------------
# Tests: Guardrails (no dataset needed — pure logic)
# ---------------------------------------------------------------------------


class TestGuardrails:
    def test_off_topic_detection(self):
        from llm import is_trace_query
        assert not is_trace_query("capital of France")
        assert not is_trace_query("write me a poem")

    def test_trace_keyword_detected(self):
        from llm import is_trace_query
        assert is_trace_query("trace sales order 740509")
        assert is_trace_query("full flow of billing document")
        assert is_trace_query("end to end journey for order 740509")

    def test_document_id_extraction(self):
        from llm import extract_document_id
        assert extract_document_id("trace order 740509") == "740509"
        assert extract_document_id("billing 90504204") == "90504204"
        assert extract_document_id("no number here") is None


# ---------------------------------------------------------------------------
# Tests: Constants (no dataset needed)
# ---------------------------------------------------------------------------


class TestConstants:
    def test_node_color_constants(self):
        from constants import NODE_COLORS, NODE_TYPES
        assert len(NODE_TYPES) > 0
        for t in NODE_TYPES:
            assert t in NODE_COLORS, f"Missing color for {t}"
        for t, color in NODE_COLORS.items():
            assert re.match(r'^#[0-9A-Fa-f]{6}$', color), \
                f"Invalid hex color '{color}' for {t}"

    def test_node_types_cover_graph_types(self):
        from constants import NODE_TYPES
        required = {
            'Customer', 'SalesOrder', 'OutboundDelivery',
            'BillingDocument', 'JournalEntry', 'Payment',
            'Product', 'Plant',
        }
        missing = required - set(NODE_TYPES)
        assert not missing, f"constants.NODE_TYPES missing: {missing}"


# ---------------------------------------------------------------------------
# Tests: Module imports and API structure (no dataset needed)
# ---------------------------------------------------------------------------


class TestAPIEndpoints:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api import app
        return TestClient(app)

    def test_api_module_imports(self, client):
        from api import app
        assert hasattr(app, 'routes')

    def test_llm_module_imports(self):
        import llm
        assert hasattr(llm, 'run_query')
        assert hasattr(llm, 'is_trace_query')
        assert hasattr(llm, 'extract_document_id')

    def test_health_endpoint_structure(self, client):
        r = client.get("/health")
        # 200 = graph loaded, 503 = graph not initialized (CI), 500 = crash
        assert r.status_code in [200, 503], \
            f"Unexpected status {r.status_code}: {r.text}"
        if r.status_code == 200:
            data = r.json()
            assert 'nodes' in data
            assert 'edges' in data
            assert 'status' in data

    def test_query_rejects_empty(self, client):
        r = client.post("/query", json={"question": ""})
        assert r.status_code == 200
        data = r.json()
        assert 'answer' in data

    def test_query_rejects_too_long(self, client):
        r = client.post("/query", json={"question": "a" * 600})
        assert r.status_code == 200
        data = r.json()
        assert 'answer' in data
        assert 'too long' in data['answer'].lower() or \
               'error' in data.get('query_type', '')
