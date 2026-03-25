"""
LLM query layer for GraphO2C.

Two-step Text-to-SQL pipeline:
  Step A — Query generation:
      LLM receives full schema context + question → returns executable SQL
      or the literal string "OFF_TOPIC".
  Step B — Answer synthesis:
      SQL is executed against DuckDB → results sent back to LLM →
      LLM returns a grounded natural-language answer.

One automatic SQL-fix retry is attempted if execution fails.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import sentry_sdk
import metrics as _metrics

import networkx as nx
from dotenv import load_dotenv

# Load .env from the same directory as this file before touching Groq
load_dotenv(Path(__file__).parent / ".env")

from groq import Groq, RateLimitError

from db import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graph injection (set by main.py at startup, used for trace queries)
# ---------------------------------------------------------------------------

_graph: nx.DiGraph | None = None


def set_graph(G: nx.DiGraph) -> None:
    global _graph
    _graph = G

# ---------------------------------------------------------------------------
# Graph-traversal trace queries
# ---------------------------------------------------------------------------

# O2C node types in flow order (used for filtering + sorting trace results)
_O2C_NODE_TYPES = [
    "SalesOrder", "SalesOrderItem",
    "DeliveryItem", "OutboundDelivery",
    "BillingDocItem", "BillingDocument",
    "JournalEntry", "Payment",
]
_O2C_FLOW_ORDER = {t: i for i, t in enumerate(_O2C_NODE_TYPES)}

# Key properties to surface per node type (keeps synthesis prompt compact)
_KEY_PROPS: dict[str, list[str]] = {
    "SalesOrder":       ["salesOrder", "soldToParty", "totalNetAmount",
                         "transactionCurrency", "overallDeliveryStatus",
                         "overallOrdReltdBillgStatus", "creationDate"],
    "SalesOrderItem":   ["salesOrderItem", "material", "requestedQuantity",
                         "requestedQuantityUnit", "netAmount"],
    "DeliveryItem":     ["deliveryDocument", "deliveryDocumentItem",
                         "actualDeliveryQuantity", "deliveryQuantityUnit", "batch"],
    "OutboundDelivery": ["deliveryDocument", "creationDate",
                         "overallGoodsMovementStatus", "actualGoodsMovementDate"],
    "BillingDocItem":   ["billingDocument", "billingDocumentItem",
                         "material", "billingQuantity", "netAmount"],
    "BillingDocument":  ["billingDocument", "billingDocumentDate",
                         "totalNetAmount", "transactionCurrency",
                         "billingDocumentIsCancelled", "accountingDocument"],
    "JournalEntry":     ["accountingDocument", "accountingDocumentItem",
                         "postingDate", "amountInTransactionCurrency",
                         "transactionCurrency", "clearingDate",
                         "clearingAccountingDocument"],
    "Payment":          ["accountingDocument", "accountingDocumentItem",
                         "clearingDate", "amountInTransactionCurrency",
                         "transactionCurrency", "postingDate"],
}

_TRACE_KEYWORDS = [
    "trace", "flow", "end-to-end", "end to end", "full flow",
    "follow", "track", "journey", "lifecycle", "chain", "path",
    "step by step", "steps for", "process for",
]


def is_trace_query(question: str) -> bool:
    """Return True if the question is asking for an O2C flow trace."""
    q = question.lower()
    return any(k in q for k in _TRACE_KEYWORDS)


def extract_document_id(question: str) -> str | None:
    """Extract a 5-9 digit document ID from the question."""
    m = re.search(r'\b(\d{5,9})\b', question)
    return m.group(1) if m else None


def _pick_props(node_type: str, all_props: dict[str, Any]) -> dict[str, Any]:
    """Return only the key properties for a node type, dropping None/empty values."""
    keys = _KEY_PROPS.get(node_type, list(all_props.keys()))
    return {k: v for k, v in all_props.items() if k in keys and v not in (None, "", "0000-00-00")}


def trace_o2c_flow(doc_id: str, G: nx.DiGraph) -> dict[str, Any]:
    """
    Traverse the graph from a document node and return all reachable
    O2C-flow nodes within 8 hops, sorted by flow position.
    """
    # Try to find the starting node — prefer SalesOrder, then others
    start_node: str | None = None
    for node_type in ["SalesOrder", "OutboundDelivery", "BillingDocument", "Customer"]:
        candidate = f"{node_type}_{doc_id}"
        if G.has_node(candidate):
            start_node = candidate
            break

    if not start_node:
        return {
            "start_node": None,
            "nodes_found": [],
            "flow_complete": False,
            "types_found": [],
            "error": f"No graph node found for document ID '{doc_id}'",
        }

    # BFS — collect all nodes reachable within 8 hops
    reachable: dict[str, list[str]] = nx.single_source_shortest_path(G, start_node, cutoff=8)

    # Filter to O2C flow types only, build structured list
    o2c_nodes: list[dict[str, Any]] = []
    for node_id, path in reachable.items():
        node_type = G.nodes[node_id].get("node_type", "")
        if node_type not in _O2C_NODE_TYPES:
            continue
        all_props = {k: v for k, v in G.nodes[node_id].items() if k != "node_type"}
        o2c_nodes.append({
            "node_id":    node_id,
            "node_type":  node_type,
            "hop":        len(path) - 1,
            "properties": _pick_props(node_type, all_props),
        })

    # Sort by O2C flow position, then by hop distance for items
    o2c_nodes.sort(key=lambda x: (_O2C_FLOW_ORDER.get(x["node_type"], 99), x["hop"]))

    types_found = sorted({n["node_type"] for n in o2c_nodes},
                         key=lambda t: _O2C_FLOW_ORDER.get(t, 99))
    flow_complete = "Payment" in {n["node_type"] for n in o2c_nodes}

    return {
        "start_node":   start_node,
        "nodes_found":  o2c_nodes,
        "flow_complete": flow_complete,
        "types_found":  types_found,
    }


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODEL = "llama-3.3-70b-versatile"
MAX_QUESTION_LEN = 500
MAX_ROWS_FOR_SYNTHESIS = 100   # rows sent to Step B LLM call
MAX_ROWS_IN_RESPONSE = 50      # rows returned to caller in raw_results

# ---------------------------------------------------------------------------
# Schema prompt — uses actual DuckDB table names from ingestion.py
# (journal_entry_items, payments, v_customer, v_product)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """
You are GraphO2C, an AI assistant that answers questions about an
SAP Order-to-Cash dataset. You have access to the following DuckDB tables:

TABLES (use these exact names in SQL):

sales_order_headers
  salesOrder (PK), soldToParty, salesOrderType, salesOrganization,
  distributionChannel, totalNetAmount, transactionCurrency,
  overallDeliveryStatus, overallOrdReltdBillgStatus, creationDate,
  requestedDeliveryDate, pricingDate, headerBillingBlockReason,
  deliveryBlockReason, customerPaymentTerms, incotermsClassification

sales_order_items
  salesOrder, salesOrderItem (PK together), salesOrderItemCategory,
  material, requestedQuantity, requestedQuantityUnit, netAmount,
  materialGroup, productionPlant, storageLocation,
  salesDocumentRjcnReason, itemBillingBlockReason

sales_order_schedule_lines
  salesOrder, salesOrderItem, scheduleLine, confirmedDeliveryDate,
  orderQuantityUnit, confdOrderQtyByMatlAvailCheck

outbound_delivery_headers
  deliveryDocument (PK), creationDate, shippingPoint,
  overallGoodsMovementStatus, overallPickingStatus,
  overallProofOfDeliveryStatus, actualGoodsMovementDate,
  headerBillingBlockReason, deliveryBlockReason

outbound_delivery_items
  deliveryDocument, deliveryDocumentItem (PK together),
  actualDeliveryQuantity, deliveryQuantityUnit, batch,
  plant, storageLocation, itemBillingBlockReason,
  referenceSdDocument      ← references sales_order_headers.salesOrder
  referenceSdDocumentItem  ← references sales_order_items.salesOrderItem

billing_document_headers
  billingDocument (PK), billingDocumentType, soldToParty,
  totalNetAmount, transactionCurrency, billingDocumentDate,
  billingDocumentIsCancelled, cancelledBillingDocument,
  accountingDocument, fiscalYear, companyCode, creationDate

billing_document_items
  billingDocument, billingDocumentItem (PK together),
  material, billingQuantity, billingQuantityUnit, netAmount,
  transactionCurrency,
  referenceSdDocument      ← references outbound_delivery_headers.deliveryDocument
  referenceSdDocumentItem  ← references outbound_delivery_items.deliveryDocumentItem

billing_document_cancellations
  billingDocument, billingDocumentIsCancelled, cancelledBillingDocument,
  totalNetAmount, accountingDocument, soldToParty, fiscalYear
  NOTE: cancelledBillingDocument is always empty here; use
        billing_document_headers.cancelledBillingDocument instead

journal_entry_items
  companyCode, fiscalYear, accountingDocument, accountingDocumentItem
  (all four form the PK), glAccount, referenceDocument, profitCenter,
  costCenter, amountInTransactionCurrency, transactionCurrency,
  amountInCompanyCodeCurrency, postingDate, documentDate,
  accountingDocumentType, assignmentReference, customer,
  financialAccountType, clearingDate, clearingAccountingDocument,
  clearingDocFiscalYear

payments
  companyCode, fiscalYear, accountingDocument, accountingDocumentItem
  (all four form the PK), clearingDate, clearingAccountingDocument,
  clearingDocFiscalYear, amountInTransactionCurrency, transactionCurrency,
  amountInCompanyCodeCurrency, companyCodeCurrency, customer,
  invoiceReference, invoiceReferenceFiscalYear, salesDocument,
  salesDocumentItem, postingDate, documentDate, glAccount,
  financialAccountType, profitCenter, costCenter

v_customer  ← denormalized customer view (prefer this over raw tables)
  businessPartner (PK), fullName, category, grouping, isBlocked,
  isArchived, creationDate, lastChangeDate, cityName, country,
  region, postalCode, streetName, timeZone, reconciliationAccount,
  accountGroup, companyDeletionIndicator, salesOrganization,
  distributionChannel, currency, paymentTerms, incoterms,
  shippingCondition, deliveryPriority, creditControlArea

v_product  ← denormalized product view (prefer this over raw tables)
  product (PK), productDescription, productType, productGroup,
  baseUnit, division, industrySector, grossWeight, netWeight,
  weightUnit, isMarkedForDeletion, productOldId

plants
  plant (PK), plantName, salesOrganization, distributionChannel,
  division, factoryCalendar, isMarkedForArchiving

product_plants
  product, plant (PK together), availabilityCheckType, profitCenter, mrpType

product_storage_locations
  product, plant, storageLocation (PK together)

KEY JOIN CHAIN — O2C flow (follow this exactly):
  sales_order_headers.salesOrder
    → outbound_delivery_items.referenceSdDocument
    (then outbound_delivery_items.deliveryDocument
    → outbound_delivery_headers.deliveryDocument)
    → billing_document_items.referenceSdDocument
    (then billing_document_items.billingDocument
    → billing_document_headers.billingDocument)
    → journal_entry_items.accountingDocument
      via billing_document_headers.accountingDocument
    → payments.clearingAccountingDocument
      via journal_entry_items.clearingAccountingDocument

COLUMN TYPES — critical for correct SQL:

BOOLEAN columns — use TRUE/FALSE (never 'X', 'true', '1', or 'Y'):
  billing_document_headers.billingDocumentIsCancelled  → TRUE / FALSE
  v_customer.isBlocked                                 → TRUE / FALSE
  v_customer.isArchived                                → TRUE / FALSE
  v_product.isMarkedForDeletion                        → TRUE / FALSE
  plants.isMarkedForArchiving                          → TRUE / FALSE

  CORRECT:  WHERE billingDocumentIsCancelled = TRUE
  WRONG:    WHERE billingDocumentIsCancelled = 'X'      ← crashes
  WRONG:    WHERE billingDocumentIsCancelled = 'true'   ← crashes

VARCHAR columns — ALL non-boolean fields are VARCHAR strings.
  This includes IDs AND numeric-looking fields like amounts and quantities.
  Always CAST before numeric comparisons:

  CORRECT:  WHERE CAST(actualDeliveryQuantity AS INTEGER) > 0
  CORRECT:  SUM(CAST(totalNetAmount AS DECIMAL))
  CORRECT:  WHERE CAST(netAmount AS DECIMAL) > 1000
  CORRECT:  WHERE salesOrder = '740506'

  WRONG:    WHERE actualDeliveryQuantity > 0       ← crashes (VARCHAR vs INTEGER)
  WRONG:    WHERE totalNetAmount > 1000            ← crashes (VARCHAR vs INTEGER)

ITEM NUMBER PADDING:
  delivery items use 6-digit padded item numbers ('000010')
  SO items use unpadded ('10')
  Join: CAST(CAST(odi.referenceSdDocumentItem AS INTEGER) AS VARCHAR) = soi.salesOrderItem

OTHER JOIN NOTES:
  billing_document_items.material references v_product.product

SAP STATUS CODES — use these exact values in WHERE clauses:
  overallGoodsMovementStatus / overallPickingStatus:
    'A' = Not yet started   'B' = Partially done   'C' = Fully completed
  overallDeliveryStatus / overallOrdReltdBillgStatus:
    'A' = Not yet started   'B' = Partially processed   'C' = Fully processed
  headerBillingBlockReason / deliveryBlockReason:
    '' (empty string) = No block.  Any other value = blocked.

PREFERRED PATTERNS for common questions:
  "Delivered but not billed" — use actualDeliveryQuantity approach:
    SELECT DISTINCT soh.salesOrder
    FROM sales_order_headers soh
    JOIN outbound_delivery_items odi ON odi.referenceSdDocument = soh.salesOrder
    WHERE odi.actualDeliveryQuantity > 0
      AND soh.salesOrder NOT IN (
          SELECT bdi.referenceSdDocument FROM billing_document_items bdi
          WHERE bdi.referenceSdDocument IS NOT NULL)
  For COUNT version: wrap in SELECT COUNT(DISTINCT salesOrder) FROM (...) sub

RULES:
1. ONLY answer questions about this dataset. If the question is about
   anything else (general knowledge, coding, history, jokes, etc.),
   respond with the single word: OFF_TOPIC
2. Generate a single executable DuckDB SQL query to answer the question.
   DuckDB syntax: use CAST, TRY_CAST, strftime for dates; ILIKE for
   case-insensitive string matching. NEVER use RECURSIVE CTEs.
3. Return ONLY a valid JSON object — no markdown fences, no explanation
   outside the JSON. Keep the SQL compact (single line or short lines):
   {"query_type":"sql","sql":"<query>","explanation":"<one line>"}
4. For O2C flow traces use simple JOINs following the KEY JOIN CHAIN.
   Example for tracing a single sales order end-to-end:
   SELECT soh.salesOrder, soh.soldToParty, odh.deliveryDocument,
          bdh.billingDocument, ji.accountingDocument,
          p.accountingDocument AS paymentDoc, p.amountInTransactionCurrency
   FROM sales_order_headers soh
   JOIN outbound_delivery_items odi ON odi.referenceSdDocument = soh.salesOrder
   JOIN outbound_delivery_headers odh ON odh.deliveryDocument = odi.deliveryDocument
   JOIN billing_document_items bdi ON bdi.referenceSdDocument = odi.deliveryDocument
   JOIN billing_document_headers bdh ON bdh.billingDocument = bdi.billingDocument
   JOIN journal_entry_items ji ON ji.accountingDocument = bdh.accountingDocument
   JOIN payments p ON p.clearingAccountingDocument = ji.clearingAccountingDocument
   WHERE soh.salesOrder = '<order_id>'
   LIMIT 50
5. COUNT AND AGGREGATION RULES:
   If the question asks HOW MANY, COUNT, TOTAL NUMBER, or ANY AGGREGATE —
   generate a query using COUNT(*) or SUM(). Do NOT use SELECT * with LIMIT.
   CORRECT: SELECT COUNT(*) AS total FROM sales_order_headers WHERE ...
   WRONG:   SELECT salesOrder FROM sales_order_headers WHERE ... LIMIT 50
   For questions asking for BOTH a count AND examples, generate TWO queries
   separated by a semicolon: first COUNT(*), then SELECT ... LIMIT 10.
   Example: "SELECT COUNT(*) AS total FROM ...; SELECT ... FROM ... LIMIT 10"
   LIMIT 50 applies ONLY to row-fetching queries, never to COUNT/SUM queries.
6. Always add LIMIT 50 on SELECT queries unless user asks for more rows.
7. Never select * from large tables; always name the columns you need.
8. When joining billing_document_headers to journal_entry_items, join on
   billing_document_headers.accountingDocument = journal_entry_items.accountingDocument
""".strip()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client() -> Groq:
    # Re-read .env on every call so the server picks up key rotations
    # without requiring a restart.
    load_dotenv(Path(__file__).parent / ".env", override=True)
    key = os.environ.get("GROQ_API_KEY", "")
    if not key or key == "your_groq_api_key_here":
        raise RuntimeError(
            "GROQ_API_KEY is not set. "
            "Add it to backend/.env or export it as an environment variable."
        )
    return Groq(api_key=key)


def _call_groq(client: Groq, **kwargs):
    """Call Groq with Sentry performance tracing and one retry on rate limits."""
    with sentry_sdk.start_span(
        op="llm.groq",
        description=f"Groq {kwargs.get('model', 'unknown')} "
                    f"(temp={kwargs.get('temperature', '?')})",
    ) as span:
        span.set_data("max_tokens", kwargs.get("max_tokens"))
        span.set_data("temperature", kwargs.get("temperature"))
        start = time.time()
        try:
            result = client.chat.completions.create(**kwargs)
            span.set_data("duration_ms", round((time.time() - start) * 1000))
            return result
        except RateLimitError:
            span.set_data("retried", True)
            time.sleep(5)
            result = client.chat.completions.create(**kwargs)
            span.set_data("duration_ms", round((time.time() - start) * 1000))
            return result
        except Exception as exc:
            span.set_data("error", str(exc))
            raise


def _parse_llm_json(text: str) -> dict[str, Any]:
    """
    Extract a JSON object from an LLM response.
    Handles markdown code fences (```json ... ```) gracefully.
    """
    text = text.strip()
    # Strip optional markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
    # Find the outermost { ... }
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON object in LLM response: {text[:300]!r}")
    return json.loads(text[start:end])


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    """Convert DuckDB fetchall() results to a list of plain dicts."""
    cols = [c[0] for c in cursor.description]
    return [
        {
            col: (v.isoformat() if hasattr(v, "isoformat") else v)
            for col, v in zip(cols, row)
        }
        for row in cursor.fetchall()
    ]


# ---------------------------------------------------------------------------
# Node-ID extraction from SQL result rows
# ---------------------------------------------------------------------------

# Maps lowercase column name → graph node-type prefix
_NODE_COL_MAP: dict[str, str] = {
    "salesorder":          "SalesOrder",
    "billingdocument":     "BillingDocument",
    "deliverydocument":    "OutboundDelivery",
    "customer":            "Customer",
    "soldtoparty":         "Customer",
    "businesspartner":     "Customer",
    "material":            "Product",
    "product":             "Product",
}


def extract_node_ids(results: list[dict[str, Any]]) -> list[str]:
    """
    Walk every result row and build graph node IDs for any column whose
    name maps to a known node type (case-insensitive lookup).
    """
    node_ids: set[str] = set()
    for row in results:
        for col, val in row.items():
            prefix = _NODE_COL_MAP.get(col.lower())
            if prefix and val:
                node_ids.add(f"{prefix}_{val}")
    return sorted(node_ids)


# ---------------------------------------------------------------------------
# Step A — SQL generation
# ---------------------------------------------------------------------------

def _call_step_a(client: Groq, question: str) -> str:
    """
    Ask the LLM to classify the question and generate SQL.
    Returns the raw LLM response content string.
    """
    response = _call_groq(
        client,
        model=MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": question},
        ],
        temperature=0.0,
        max_tokens=2048,
    )
    return response.choices[0].message.content.strip()


def _call_sql_fix(client: Groq, question: str, failed_sql: str, error: str) -> str:
    """Ask the LLM to repair a SQL query that raised an error."""
    fix_prompt = (
        f"The SQL query below failed with the following error.\n\n"
        f"Original question: {question}\n\n"
        f"Failed SQL:\n{failed_sql}\n\n"
        f"Error:\n{error}\n\n"
        "Please provide a corrected SQL query that fixes this error.\n"
        "Return ONLY a JSON object (no markdown):\n"
        '{"query_type": "sql", "sql": "<corrected SQL>", '
        '"explanation": "<one line description>"}'
    )
    response = _call_groq(
        client,
        model=MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": fix_prompt},
        ],
        temperature=0.0,
        max_tokens=2048,
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Step B — Answer synthesis
# ---------------------------------------------------------------------------

def _call_step_b(
    client: Groq,
    question: str,
    sql: str,
    results: list[dict[str, Any]],
) -> str:
    """
    Send the SQL results back to the LLM and ask it to write a
    grounded natural-language answer.
    """
    # Truncate to MAX_ROWS_FOR_SYNTHESIS so we stay within token limits
    capped = results[:MAX_ROWS_FOR_SYNTHESIS]
    results_json = json.dumps(capped, indent=2, default=str)

    truncation_warning = (
        f"\nWARNING: Results are capped at {MAX_ROWS_FOR_SYNTHESIS} rows. "
        f"The actual dataset may contain more rows than shown. "
        f"Do NOT state a count as definitive if the number of rows shown "
        f"equals exactly {MAX_ROWS_FOR_SYNTHESIS} — instead write "
        f"'at least {MAX_ROWS_FOR_SYNTHESIS}' or note that results are limited."
        if len(results) >= MAX_ROWS_FOR_SYNTHESIS else ""
    )

    synthesis_prompt = (
        f"The user asked: {question}\n\n"
        f"The following SQL query was executed:\n{sql}\n\n"
        f"It returned {len(results)} row(s). "
        f"Here are the results (up to {MAX_ROWS_FOR_SYNTHESIS} rows shown):\n"
        f"{results_json}\n"
        f"{truncation_warning}\n\n"
        "Write a clear, concise natural-language answer grounded strictly in "
        "this data. Do not add information not present in the results. "
        "If the result set is empty, say so and suggest a likely reason."
    )
    response = _call_groq(
        client,
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful data analyst. Answer questions about "
                    "SAP Order-to-Cash data concisely and accurately. "
                    "Cite specific values from the data in your answer."
                ),
            },
            {"role": "user", "content": synthesis_prompt},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Step B (trace variant) — Synthesis from graph traversal results
# ---------------------------------------------------------------------------

def _call_step_b_trace(
    client: Groq,
    question: str,
    trace_result: dict[str, Any],
) -> str:
    """
    Synthesize a natural-language O2C flow description from graph traversal
    results.  Each node's key properties are provided; the LLM narrates the
    journey step by step.
    """
    # Build a compact summary grouped by node type
    by_type: dict[str, list[dict]] = {}
    for node in trace_result["nodes_found"]:
        by_type.setdefault(node["node_type"], []).append(node["properties"])

    # Limit to 3 documents per type to stay within token budget
    compact: dict[str, Any] = {t: docs[:3] for t, docs in by_type.items()}
    flow_complete = trace_result.get("flow_complete", False)
    types_found = trace_result.get("types_found", [])

    trace_json = json.dumps(compact, indent=2, default=str)

    _SAP_STATUS_CODES = (
        "SAP STATUS CODE REFERENCE — use these when describing node properties:\n"
        "overallGoodsMovementStatus / overallPickingStatus:\n"
        "  A = Not yet started  |  B = Partially completed  |  C = Fully completed\n"
        "overallDeliveryStatus / overallOrdReltdBillgStatus:\n"
        "  A = Not yet started  |  B = Partially processed  |  C = Fully processed\n"
        "billingDocumentIsCancelled:\n"
        "  true  = This is a reversal/cancellation document\n"
        "  false = Normal billing document\n"
        "headerBillingBlockReason / deliveryBlockReason:\n"
        "  (empty string) = No block active\n"
        "  (any value)    = Blocked with that reason code\n"
    )

    synthesis_prompt = (
        f"The user asked: {question}\n\n"
        f"A graph traversal was performed starting from node "
        f"'{trace_result.get('start_node')}'.\n"
        f"O2C stages found: {', '.join(types_found)}\n"
        f"Flow complete (reached Payment): {flow_complete}\n\n"
        f"Key properties by stage:\n{trace_json}\n\n"
        f"{_SAP_STATUS_CODES}\n"
        "Describe the complete Order-to-Cash journey step by step. "
        "For each stage present, cite the specific document IDs, amounts, "
        "dates, and statuses from the data above — using the status code "
        "reference above to interpret A/B/C codes correctly. "
        "Clearly note which stages are present and which are missing "
        "from the chain. If the flow is incomplete, explain the last "
        "stage reached and what would come next."
    )
    response = _call_groq(
        client,
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful SAP O2C analyst. Explain Order-to-Cash "
                    "flows clearly and concisely, stage by stage. Always cite "
                    "specific document IDs and values from the provided data. "
                    "Use the SAP status code reference provided to interpret "
                    "status fields correctly — never guess what A/B/C means."
                ),
            },
            {"role": "user", "content": synthesis_prompt},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def _execute_sql(sql: str) -> list[dict[str, Any]]:
    """Execute SQL against DuckDB, handling dual count+sample queries separated by semicolon."""
    conn = get_connection()
    queries = [q.strip() for q in sql.split(';') if q.strip()]
    if len(queries) == 2:
        cur1 = conn.execute(queries[0])
        cur2 = conn.execute(queries[1])
        return _rows_to_dicts(cur1) + _rows_to_dicts(cur2)
    cur = conn.execute(queries[0] if queries else sql)
    return _rows_to_dicts(cur)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_query(question: str) -> dict[str, Any]:
    """
    Full pipeline:
      1. Validate question length
      2. Step A: LLM → SQL (or OFF_TOPIC)
      3. Execute SQL against DuckDB (one retry on failure)
      4. Step B: LLM → natural-language answer
      5. Extract node IDs for frontend graph highlighting
    """
    question = question.strip()
    logger.info("Query received: %.120s", question)
    _metrics.query_count += 1

    # ── Guard: empty / length ─────────────────────────────────────────────
    if not question:
        return {
            "answer": "Please enter a question about the Order-to-Cash dataset.",
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "error",
        }
    if len(question) > MAX_QUESTION_LEN:
        return {
            "answer": (
                f"Question is too long ({len(question)} chars). "
                f"Please keep it under {MAX_QUESTION_LEN} characters."
            ),
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "error",
        }

    try:
        client = _get_client()
    except RuntimeError as exc:
        raise exc

    try:
        return _run_query_inner(question, client)
    except RateLimitError:
        _metrics.rate_limit_hits += 1
        logger.warning("Groq rate limit reached")
        sentry_sdk.capture_message(
            "Groq rate limit hit",
            level="warning",
        )
        return {
            "answer": (
                "The system is currently at capacity. "
                "Please try again in a few minutes. "
                "This is a free-tier API rate limit."
            ),
            "query_type": "rate_limited",
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
        }
    except Exception as exc:
        _metrics.query_errors += 1
        logger.exception("Unhandled error in run_query")
        sentry_sdk.capture_exception(exc)
        return {
            "answer": "An unexpected error occurred. Please try again.",
            "query_type": "error",
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
        }


def _run_query_inner(question: str, client: Groq) -> dict[str, Any]:
    # ── Trace query fast-path: use graph traversal instead of SQL ──────────
    if is_trace_query(question) and _graph is not None:
        doc_id = extract_document_id(question)
        if doc_id:
            logger.info("Trace query detected — traversing graph for doc_id=%s", doc_id)
            trace_result = trace_o2c_flow(doc_id, _graph)
            if trace_result.get("nodes_found"):
                answer = _call_step_b_trace(client, question, trace_result)
                node_ids = [n["node_id"] for n in trace_result["nodes_found"]]
                logger.info(
                    "Trace complete: %d nodes found, types=%s, flow_complete=%s",
                    len(node_ids), trace_result.get("types_found"), trace_result.get("flow_complete"),
                )
                return {
                    "answer": answer,
                    "sql_executed": None,
                    "row_count": len(node_ids),
                    "nodes_referenced": node_ids,
                    "query_type": "graph_traversal",
                    "raw_results": {
                        "start_node":   trace_result["start_node"],
                        "types_found":  trace_result["types_found"],
                        "flow_complete": trace_result["flow_complete"],
                        "node_count":   len(node_ids),
                    },
                }
            else:
                logger.warning("Trace: no O2C nodes found for doc_id=%s", doc_id)

    # ── Step A: generate SQL ───────────────────────────────────────────────
    raw_a = _call_step_a(client, question)
    logger.debug("Step A raw response: %s", raw_a)

    # OFF_TOPIC check — the LLM should return the literal word
    if raw_a.strip().upper() == "OFF_TOPIC":
        return {
            "answer": (
                "This system is designed to answer questions related to the "
                "Order-to-Cash dataset only."
            ),
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "off_topic",
        }

    # Parse the JSON envelope
    try:
        step_a = _parse_llm_json(raw_a)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.error("Failed to parse Step A JSON: %s | raw=%s", exc, raw_a[:400])
        return {
            "answer": "The query could not be parsed. Please rephrase your question.",
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "error",
            "debug": str(exc),
        }

    # Second OFF_TOPIC check inside the JSON envelope
    if str(step_a.get("sql", "")).strip().upper() == "OFF_TOPIC":
        return {
            "answer": (
                "This system is designed to answer questions related to the "
                "Order-to-Cash dataset only."
            ),
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "off_topic",
        }

    sql = step_a.get("sql", "").strip()
    logger.info("Step A: query_type=%s | sql=%.200s", step_a.get("query_type"), sql)
    if not sql:
        return {
            "answer": "No SQL was generated. Please rephrase your question.",
            "sql_executed": None,
            "row_count": 0,
            "nodes_referenced": [],
            "query_type": "error",
        }

    # ── Execute SQL (with one retry on failure) ────────────────────────────
    try:
        results = _execute_sql(sql)
    except Exception as first_err:
        logger.warning("SQL failed (attempt 1): %s\nSQL: %s", first_err, sql)
        # Ask LLM to fix the query
        try:
            raw_fix = _call_sql_fix(client, question, sql, str(first_err))
            fix_obj = _parse_llm_json(raw_fix)
            sql = fix_obj.get("sql", "").strip()
            results = _execute_sql(sql)
        except Exception as second_err:
            logger.error("SQL failed (attempt 2): %s\nSQL: %s", second_err, sql)
            return {
                "answer": (
                    f"The query could not be executed after one retry. "
                    f"Error: {second_err}"
                ),
                "sql_executed": sql,
                "row_count": 0,
                "nodes_referenced": [],
                "query_type": "error",
                "debug": str(second_err),
            }

    logger.info("SQL returned %d rows", len(results))

    # ── Step B: synthesize answer ──────────────────────────────────────────
    answer = _call_step_b(client, question, sql, results)
    logger.info("Answer: %.120s", answer)

    # ── Extract node IDs for graph highlighting ────────────────────────────
    nodes_referenced = extract_node_ids(results)

    return {
        "answer": answer,
        "sql_executed": sql,
        "row_count": len(results),
        "nodes_referenced": nodes_referenced,
        "query_type": "sql",
        "raw_results": results[:MAX_ROWS_IN_RESPONSE],
    }
