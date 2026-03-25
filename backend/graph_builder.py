"""
Graph construction: builds a NetworkX DiGraph from DuckDB tables.

Node ID convention:  "<NodeType>_<primaryKey>"
  Examples:
    SalesOrder_740506
    Customer_310000108
    SalesOrderItem_740506_10          (compound key: salesOrder_item)
    BillingDocItem_90504204_10
    DeliveryItem_80738076_000010

Edge types (directed):
  PLACED_BY        SalesOrder        -> Customer
  HAS_ITEM         SalesOrder        -> SalesOrderItem
  ORDERS_PRODUCT   SalesOrderItem    -> Product
  PRODUCED_AT      SalesOrderItem    -> Plant
  FULFILLED_BY     SalesOrderItem    -> DeliveryItem
  PART_OF          DeliveryItem      -> OutboundDelivery
  BILLED_AS        DeliveryItem      -> BillingDocItem
  BILLED_IN        BillingDocItem    -> BillingDocument
  BILLED_TO        BillingDocument   -> Customer
  POSTED_TO        BillingDocument   -> JournalEntry
  CLEARED_BY       JournalEntry      -> Payment
  PAID_BY          Payment           -> Customer
  CANCELS          BillingDocument   -> BillingDocument
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

import networkx as nx
import duckdb

from db import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(description: list, row: tuple) -> dict[str, Any]:
    """Convert a DuckDB row + cursor description into a plain dict."""
    return {
        col[0]: (v.isoformat() if hasattr(v, "isoformat") else v)
        for col, v in zip(description, row)
    }


def _safe_str(val: Any) -> str:
    """Coerce a value to a clean string, returning '' for None."""
    if val is None:
        return ""
    return str(val).strip()


# ---------------------------------------------------------------------------
# Node loaders
# ---------------------------------------------------------------------------

def _load_customers(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM v_customer")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"Customer_{d['businessPartner']}"
        G.add_node(nid, node_type="Customer", **d)
        count += 1
    return count


def _load_products(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM v_product")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"Product_{d['product']}"
        G.add_node(nid, node_type="Product", **d)
        count += 1
    return count


def _load_plants(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM plants")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"Plant_{d['plant']}"
        G.add_node(nid, node_type="Plant", **d)
        count += 1
    return count


def _load_sales_orders(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM sales_order_headers")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"SalesOrder_{d['salesOrder']}"
        G.add_node(nid, node_type="SalesOrder", **d)
        count += 1
    return count


def _load_sales_order_items(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM sales_order_items")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"SalesOrderItem_{d['salesOrder']}_{d['salesOrderItem']}"
        G.add_node(nid, node_type="SalesOrderItem", **d)
        count += 1
    return count


def _load_deliveries(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM outbound_delivery_headers")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"OutboundDelivery_{d['deliveryDocument']}"
        G.add_node(nid, node_type="OutboundDelivery", **d)
        count += 1
    return count


def _load_delivery_items(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM outbound_delivery_items")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"DeliveryItem_{d['deliveryDocument']}_{d['deliveryDocumentItem']}"
        G.add_node(nid, node_type="DeliveryItem", **d)
        count += 1
    return count


def _load_billing_documents(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM billing_document_headers")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"BillingDocument_{d['billingDocument']}"
        G.add_node(nid, node_type="BillingDocument", **d)
        count += 1
    return count


def _load_billing_doc_items(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM billing_document_items")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = f"BillingDocItem_{d['billingDocument']}_{d['billingDocumentItem']}"
        G.add_node(nid, node_type="BillingDocItem", **d)
        count += 1
    return count


def _load_journal_entries(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM journal_entry_items")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        # PK: companyCode + fiscalYear + accountingDocument + accountingDocumentItem
        nid = (
            f"JournalEntry_{d['companyCode']}"
            f"_{d['fiscalYear']}"
            f"_{d['accountingDocument']}"
            f"_{d['accountingDocumentItem']}"
        )
        G.add_node(nid, node_type="JournalEntry", **d)
        count += 1
    return count


def _load_payments(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    cur = conn.execute("SELECT * FROM payments")
    cols = cur.description
    count = 0
    for row in cur.fetchall():
        d = _row_to_dict(cols, row)
        nid = (
            f"Payment_{d['companyCode']}"
            f"_{d['fiscalYear']}"
            f"_{d['accountingDocument']}"
            f"_{d['accountingDocumentItem']}"
        )
        G.add_node(nid, node_type="Payment", **d)
        count += 1
    return count


# ---------------------------------------------------------------------------
# Edge builders
# ---------------------------------------------------------------------------

def _build_placed_by(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """SalesOrder -> Customer  (soldToParty)"""
    rows = conn.execute(
        "SELECT salesOrder, soldToParty FROM sales_order_headers"
    ).fetchall()
    count = 0
    for so, party in rows:
        src = f"SalesOrder_{so}"
        dst = f"Customer_{party}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="PLACED_BY", soldToParty=str(party))
            count += 1
    return count


def _build_has_item(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """SalesOrder -> SalesOrderItem"""
    rows = conn.execute(
        "SELECT salesOrder, salesOrderItem FROM sales_order_items"
    ).fetchall()
    count = 0
    for so, item in rows:
        src = f"SalesOrder_{so}"
        dst = f"SalesOrderItem_{so}_{item}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="HAS_ITEM", position=str(item))
            count += 1
    return count


def _build_orders_product(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """SalesOrderItem -> Product  (material)"""
    rows = conn.execute(
        "SELECT salesOrder, salesOrderItem, material, requestedQuantity, "
        "       requestedQuantityUnit, netAmount "
        "FROM sales_order_items"
    ).fetchall()
    count = 0
    for so, item, mat, qty, unit, amt in rows:
        src = f"SalesOrderItem_{so}_{item}"
        dst = f"Product_{mat}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(
                src, dst,
                edge_type="ORDERS_PRODUCT",
                requestedQuantity=_safe_str(qty),
                requestedQuantityUnit=_safe_str(unit),
                netAmount=_safe_str(amt),
            )
            count += 1
    return count


def _build_produced_at(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """SalesOrderItem -> Plant  (productionPlant)"""
    rows = conn.execute(
        "SELECT salesOrder, salesOrderItem, productionPlant "
        "FROM sales_order_items "
        "WHERE productionPlant IS NOT NULL AND productionPlant != ''"
    ).fetchall()
    count = 0
    for so, item, plant in rows:
        src = f"SalesOrderItem_{so}_{item}"
        dst = f"Plant_{plant}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="PRODUCED_AT", plant=str(plant))
            count += 1
    return count


def _build_fulfilled_by(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """
    SalesOrderItem -> DeliveryItem
    Join: delivery_items.referenceSdDocument = sales_order (740XXX range)
          delivery_items.referenceSdDocumentItem = salesOrderItem
    """
    rows = conn.execute(
        "SELECT deliveryDocument, deliveryDocumentItem, "
        "       referenceSdDocument, referenceSdDocumentItem "
        "FROM outbound_delivery_items "
        "WHERE referenceSdDocument IS NOT NULL AND referenceSdDocument != ''"
    ).fetchall()
    count = 0
    for del_doc, del_item, ref_doc, ref_item in rows:
        # Normalize item numbers (delivery uses '000010', SO uses '10')
        ref_item_norm = str(int(ref_item)) if ref_item else ref_item
        src = f"SalesOrderItem_{ref_doc}_{ref_item_norm}"
        dst = f"DeliveryItem_{del_doc}_{del_item}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(
                src, dst,
                edge_type="FULFILLED_BY",
                deliveryDocument=str(del_doc),
                deliveryDocumentItem=str(del_item),
            )
            count += 1
    return count


def _build_part_of(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """DeliveryItem -> OutboundDelivery"""
    rows = conn.execute(
        "SELECT deliveryDocument, deliveryDocumentItem FROM outbound_delivery_items"
    ).fetchall()
    count = 0
    for del_doc, del_item in rows:
        src = f"DeliveryItem_{del_doc}_{del_item}"
        dst = f"OutboundDelivery_{del_doc}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="PART_OF")
            count += 1
    return count


def _build_billed_as(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """
    DeliveryItem -> BillingDocItem
    billing_document_items.referenceSdDocument  = deliveryDocument (807XXXXX)
    billing_document_items.referenceSdDocumentItem = deliveryDocumentItem
    """
    rows = conn.execute(
        "SELECT billingDocument, billingDocumentItem, "
        "       referenceSdDocument, referenceSdDocumentItem "
        "FROM billing_document_items "
        "WHERE referenceSdDocument IS NOT NULL AND referenceSdDocument != ''"
    ).fetchall()
    count = 0
    for bill_doc, bill_item, ref_doc, ref_item in rows:
        # Delivery item IDs are stored as 6-digit padded strings ('000010'),
        # but billing_document_items.referenceSdDocumentItem uses unpadded '10'.
        # Zero-pad to match the node ID created in _load_delivery_items.
        ref_item_padded = str(ref_item).zfill(6) if ref_item else ref_item
        src = f"DeliveryItem_{ref_doc}_{ref_item_padded}"
        dst = f"BillingDocItem_{bill_doc}_{bill_item}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(
                src, dst,
                edge_type="BILLED_AS",
                billingDocument=str(bill_doc),
                billingDocumentItem=str(bill_item),
            )
            count += 1
    return count


def _build_billed_in(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """BillingDocItem -> BillingDocument"""
    rows = conn.execute(
        "SELECT billingDocument, billingDocumentItem FROM billing_document_items"
    ).fetchall()
    count = 0
    for bill_doc, bill_item in rows:
        src = f"BillingDocItem_{bill_doc}_{bill_item}"
        dst = f"BillingDocument_{bill_doc}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="BILLED_IN")
            count += 1
    return count


def _build_billed_to(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """BillingDocument -> Customer  (soldToParty)"""
    rows = conn.execute(
        "SELECT billingDocument, soldToParty FROM billing_document_headers "
        "WHERE soldToParty IS NOT NULL AND soldToParty != ''"
    ).fetchall()
    count = 0
    for bill_doc, party in rows:
        src = f"BillingDocument_{bill_doc}"
        dst = f"Customer_{party}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="BILLED_TO", soldToParty=str(party))
            count += 1
    return count


def _build_posted_to(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """
    BillingDocument -> JournalEntry
    billing_document_headers.accountingDocument matches
    journal_entry_items.accountingDocument
    """
    rows = conn.execute(
        """
        SELECT
            bdh.billingDocument,
            ji.companyCode,
            ji.fiscalYear,
            ji.accountingDocument,
            ji.accountingDocumentItem
        FROM billing_document_headers bdh
        JOIN journal_entry_items ji
          ON ji.accountingDocument = bdh.accountingDocument
        WHERE bdh.accountingDocument IS NOT NULL
          AND bdh.accountingDocument != ''
        """
    ).fetchall()
    count = 0
    for bill_doc, co, fy, acc_doc, acc_item in rows:
        src = f"BillingDocument_{bill_doc}"
        dst = f"JournalEntry_{co}_{fy}_{acc_doc}_{acc_item}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(
                src, dst,
                edge_type="POSTED_TO",
                accountingDocument=str(acc_doc),
                fiscalYear=str(fy),
            )
            count += 1
    return count


def _build_cleared_by(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """
    JournalEntry -> Payment
    journal_entry_items.clearingAccountingDocument matches
    payments.clearingAccountingDocument  (they share the clearing doc number)
    """
    rows = conn.execute(
        """
        SELECT
            ji.companyCode,
            ji.fiscalYear,
            ji.accountingDocument,
            ji.accountingDocumentItem,
            p.companyCode   AS p_co,
            p.fiscalYear    AS p_fy,
            p.accountingDocument  AS p_acc_doc,
            p.accountingDocumentItem AS p_acc_item
        FROM journal_entry_items ji
        JOIN payments p
          ON p.clearingAccountingDocument = ji.clearingAccountingDocument
         AND p.companyCode = ji.companyCode
        WHERE ji.clearingAccountingDocument IS NOT NULL
          AND ji.clearingAccountingDocument != ''
        """
    ).fetchall()
    count = 0
    for co, fy, acc_doc, acc_item, p_co, p_fy, p_acc_doc, p_acc_item in rows:
        src = f"JournalEntry_{co}_{fy}_{acc_doc}_{acc_item}"
        dst = f"Payment_{p_co}_{p_fy}_{p_acc_doc}_{p_acc_item}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="CLEARED_BY")
            count += 1
    return count


def _build_paid_by(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """Payment -> Customer"""
    rows = conn.execute(
        "SELECT companyCode, fiscalYear, accountingDocument, "
        "       accountingDocumentItem, customer "
        "FROM payments "
        "WHERE customer IS NOT NULL AND customer != ''"
    ).fetchall()
    count = 0
    for co, fy, acc_doc, acc_item, cust in rows:
        src = f"Payment_{co}_{fy}_{acc_doc}_{acc_item}"
        dst = f"Customer_{cust}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="PAID_BY", customer=str(cust))
            count += 1
    return count


def _build_cancels(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> int:
    """
    BillingDocument -> BillingDocument  (CANCELS)

    The relationship lives in billing_document_headers.cancelledBillingDocument:
    a row where cancelledBillingDocument is populated is a reversal document
    that cancels the referenced original billing document.

    Note: billing_document_cancellations.cancelledBillingDocument is always
    empty in this dataset — use billing_document_headers as the source.
    """
    rows = conn.execute(
        """
        SELECT billingDocument, cancelledBillingDocument
        FROM billing_document_headers
        WHERE cancelledBillingDocument IS NOT NULL
          AND cancelledBillingDocument != ''
        """
    ).fetchall()
    count = 0
    for cancel_doc, orig_doc in rows:
        src = f"BillingDocument_{cancel_doc}"
        dst = f"BillingDocument_{orig_doc}"
        if G.has_node(src) and G.has_node(dst):
            G.add_edge(src, dst, edge_type="CANCELS",
                       cancelledBillingDocument=str(orig_doc))
            count += 1
    return count


# ---------------------------------------------------------------------------
# Sample path tracer (diagnostic)
# ---------------------------------------------------------------------------

def _trace_sample_path(G: nx.DiGraph, conn: duckdb.DuckDBPyConnection) -> None:
    """
    Pick the first SalesOrder that has a full O2C chain and print the path.
    """
    logger.info("--- Sample O2C path trace ---")
    # Find a SalesOrder that has at least one Payment reachable
    so_nodes = [n for n, d in G.nodes(data=True) if d.get("node_type") == "SalesOrder"]
    if not so_nodes:
        logger.info("  No SalesOrder nodes found.")
        return

    # BFS from each SO; check if any reachable node is a Payment via a proper O2C path
    for so_node in so_nodes:
        reachable = nx.single_source_shortest_path(G, so_node, cutoff=8)
        for target, path in reachable.items():
            if G.nodes[target].get("node_type") == "Payment":
                types_in_path = [G.nodes[n].get("node_type") for n in path]
                if "JournalEntry" in types_in_path:
                    logger.info("  %s", so_node)
                    for node in path[1:]:
                        nt = G.nodes[node].get("node_type", "?")
                        logger.info("    -> [%s] %s", nt, node)
                    return

    # Fallback: show the first SO's direct neighbors
    so_node = so_nodes[0]
    logger.info("  %s (no full path to Payment found; showing direct neighbors)", so_node)
    for _, nbr, data in G.out_edges(so_node, data=True):
        nt = G.nodes[nbr].get("node_type", "?")
        logger.info("    -> [%s] %s  edge=%s", nt, nbr, data.get("edge_type"))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_graph() -> nx.DiGraph:
    """
    Build and return the full O2C DiGraph.
    Called once at application startup; the graph is held in memory.
    """
    conn = get_connection()
    G = nx.DiGraph()

    logger.info("=== Loading nodes ===")
    node_counts: dict[str, int] = {}

    loaders = [
        ("Customer",         _load_customers),
        ("Product",          _load_products),
        ("Plant",            _load_plants),
        ("SalesOrder",       _load_sales_orders),
        ("SalesOrderItem",   _load_sales_order_items),
        ("OutboundDelivery", _load_deliveries),
        ("DeliveryItem",     _load_delivery_items),
        ("BillingDocument",  _load_billing_documents),
        ("BillingDocItem",   _load_billing_doc_items),
        ("JournalEntry",     _load_journal_entries),
        ("Payment",          _load_payments),
    ]

    for label, fn in loaders:
        n = fn(G, conn)
        node_counts[label] = n
        logger.info("  %-20s %8d nodes", label, n)

    logger.info("  %-20s %8d nodes", "TOTAL", G.number_of_nodes())

    logger.info("=== Building edges ===")
    edge_counts: dict[str, int] = {}

    edge_builders = [
        ("PLACED_BY",      _build_placed_by),
        ("HAS_ITEM",       _build_has_item),
        ("ORDERS_PRODUCT", _build_orders_product),
        ("PRODUCED_AT",    _build_produced_at),
        ("FULFILLED_BY",   _build_fulfilled_by),
        ("PART_OF",        _build_part_of),
        ("BILLED_AS",      _build_billed_as),
        ("BILLED_IN",      _build_billed_in),
        ("BILLED_TO",      _build_billed_to),
        ("POSTED_TO",      _build_posted_to),
        ("CLEARED_BY",     _build_cleared_by),
        ("PAID_BY",        _build_paid_by),
        ("CANCELS",        _build_cancels),
    ]

    for label, fn in edge_builders:
        n = fn(G, conn)
        edge_counts[label] = n
        logger.info("  %-20s %8d edges", label, n)

    logger.info("  %-20s %8d edges", "TOTAL", G.number_of_edges())

    _trace_sample_path(G, conn)

    logger.info("=== Graph build complete ===")
    return G


if __name__ == "__main__":
    from ingestion import run_ingestion
    run_ingestion()
    build_graph()
