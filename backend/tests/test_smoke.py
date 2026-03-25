"""
Smoke tests for GraphO2C.
Run: cd backend && pytest tests/ -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest


@pytest.fixture(scope="session")
def graph():
    from ingestion import run_ingestion
    from graph_builder import build_graph
    run_ingestion()
    return build_graph()


@pytest.fixture(scope="session")
def db():
    from db import get_connection
    return get_connection()


class TestIngestion:
    def test_sales_order_count(self, db):
        count = db.execute("SELECT COUNT(*) FROM sales_order_headers").fetchone()[0]
        assert count == 100, f"Expected 100 SOs, got {count}"

    def test_billing_doc_count(self, db):
        count = db.execute("SELECT COUNT(*) FROM billing_document_headers").fetchone()[0]
        assert count == 163, f"Expected 163 BDs, got {count}"

    def test_customer_view_exists(self, db):
        count = db.execute("SELECT COUNT(*) FROM v_customer").fetchone()[0]
        assert count == 8, f"Expected 8 customers, got {count}"

    def test_product_view_exists(self, db):
        count = db.execute("SELECT COUNT(*) FROM v_product").fetchone()[0]
        assert count == 69, f"Expected 69 products, got {count}"


class TestGraph:
    def test_node_count(self, graph):
        assert graph.number_of_nodes() == 1262, \
            f"Expected 1262 nodes, got {graph.number_of_nodes()}"

    def test_edge_count(self, graph):
        assert graph.number_of_edges() == 2059, \
            f"Expected 2059 edges, got {graph.number_of_edges()}"

    def test_cancels_edges_exist(self, graph):
        cancels = [(u, v) for u, v, d in graph.edges(data=True)
                   if d.get('edge_type') == 'CANCELS']
        assert len(cancels) == 80, \
            f"Expected 80 CANCELS edges, got {len(cancels)}"

    def test_all_node_types_present(self, graph):
        types = {d.get('node_type') for _, d in graph.nodes(data=True)}
        required = {
            'Customer', 'SalesOrder', 'BillingDocument',
            'JournalEntry', 'Payment', 'OutboundDelivery',
            'Product', 'Plant',
        }
        missing = required - types
        assert not missing, f"Missing node types: {missing}"


class TestGuardrails:
    def test_off_topic_not_trace(self):
        from llm import is_trace_query
        assert not is_trace_query("capital of France")

    def test_trace_keyword_detected(self):
        from llm import is_trace_query
        assert is_trace_query("trace sales order 740509")
        assert is_trace_query("full flow of billing document")

    def test_document_id_extraction(self):
        from llm import extract_document_id
        assert extract_document_id("trace order 740509") == "740509"
        assert extract_document_id("no number here") is None
