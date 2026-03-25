# GraphO2C — Graph-Based Order-to-Cash Query System

![CI](https://github.com/yerramsettysuchita/GraphO2C/actions/workflows/test.yml/badge.svg)

SAP's Order-to-Cash process spans at least a dozen tables. A sales order becomes a delivery, a delivery becomes an invoice, an invoice becomes a journal entry, and a journal entry becomes a payment. When this data lives in flat tables, connecting those dots takes complex SQL joins across every step. GraphO2C solves that by loading all 20 SAP tables into a single knowledge graph and letting you explore and query the full O2C flow visually and in plain English.

You type a question. The system figures out what SQL to run, runs it against DuckDB, and gives you a grounded answer with the relevant graph nodes highlighted on screen.

---

## What It Does

The application ingests 20 JSONL tables of SAP-style Order-to-Cash data, builds a directed NetworkX graph with 1,262 nodes and 2,059 edges, and exposes both a REST API and a browser-based graph explorer. A Groq-powered LLM pipeline handles natural language questions by generating and executing SQL, then synthesising a human-readable answer from the actual results.

A separate trace mode handles questions like *"show me the full flow of sales order 740506"* by walking the graph directly rather than going through SQL, which gives a richer picture of how one order connects to its deliveries, invoices, journal entries, and payment.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend API | Python 3.10 and FastAPI |
| Analytical queries | DuckDB (file-based, no server needed) |
| Graph engine | NetworkX (in-memory directed graph) |
| LLM | Groq API with `llama-3.3-70b-versatile` |
| Frontend | Vanilla JS with Cytoscape.js, single HTML file |

**Why DuckDB instead of Postgres?** The dataset is read-only and analytical. DuckDB reads multi-part JSONL files with a single glob expression and runs fast aggregations without any setup. There is no database server to manage and the file travels with the project.

**Why NetworkX instead of Neo4j?** The graph fits comfortably in memory. Shortest path, neighbor lookup, and BFS are single function calls. Neo4j makes sense when you have tens of millions of nodes, concurrent writes, or need a persistent graph database. For this scale, it would add operational overhead without any benefit.

---

## Graph Schema

The graph has nine node types, each mapped from one or more SAP source tables.

| Node Type | Source Table | Primary Key |
|---|---|---|
| Customer | business_partners joined with addresses and assignments | businessPartner |
| Product | products joined with product_descriptions | product |
| Plant | plants | plant |
| SalesOrder | sales_order_headers | salesOrder |
| SalesOrderItem | sales_order_items | salesOrder + salesOrderItem |
| OutboundDelivery | outbound_delivery_headers | deliveryDocument |
| DeliveryItem | outbound_delivery_items | deliveryDocument + deliveryDocumentItem |
| BillingDocument | billing_document_headers | billingDocument |
| JournalEntry | journal_entry_items | companyCode + fiscalYear + accountingDocument + item |
| Payment | payments | companyCode + fiscalYear + accountingDocument + item |

### The O2C Flow as Graph Edges

```
SalesOrder      ──PLACED_BY──►      Customer
SalesOrder      ──HAS_ITEM──►       SalesOrderItem
SalesOrderItem  ──ORDERS_PRODUCT──► Product
SalesOrderItem  ──PRODUCED_AT──►    Plant
SalesOrderItem  ──FULFILLED_BY──►   DeliveryItem
DeliveryItem    ──PART_OF──►        OutboundDelivery
DeliveryItem    ──BILLED_AS──►      BillingDocumentItem
BillingDocItem  ──BILLED_IN──►      BillingDocument
BillingDocument ──BILLED_TO──►      Customer
BillingDocument ──POSTED_TO──►      JournalEntry
JournalEntry    ──CLEARED_BY──►     Payment
Payment         ──PAID_BY──►        Customer
```

Every node ID follows the pattern `TypeName_PrimaryKey`, for example `SalesOrder_740506`, `Customer_310000108`, or `DeliveryItem_80738076_000010`. This makes it trivial to map SQL result columns back to graph nodes for automatic highlighting.

---

## How the LLM Pipeline Works

The pipeline runs in two steps and uses the Groq API for both.

**Step 1 — SQL generation** runs at temperature 0.0. The model receives a system prompt that lists every table, every column, the full O2C join chain, data types (including which fields are boolean versus VARCHAR in DuckDB), SAP status code meanings, and a strict instruction to return only a JSON object with `query_type`, `sql`, and `explanation` fields. The zero temperature means the same question always produces the same SQL, which makes debugging straightforward.

**Step 2 — Answer synthesis** runs at temperature 0.3. The SQL executes against DuckDB and the results (capped at 100 rows) go back to the model with a prompt that says to answer based only on what the data shows, not general knowledge. If the model returns exactly 100 rows, it is warned that results may be truncated and it should not state a definitive count.

If the SQL fails, the error message goes back to the model for one correction attempt before the error surfaces to the user.

**Guardrails.** The system checks for off-topic questions at two points: on the raw model output and again inside the parsed JSON. Questions outside the Order-to-Cash domain get a polite rejection. Input is capped at 500 characters. Testing confirmed that asking "What is the capital of France?" returns the rejection message correctly.

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Returns node and edge counts to confirm the graph loaded |
| GET | `/graph/summary` | Node and edge counts broken down by type |
| GET | `/graph/nodes?type=X&limit=50` | Paginated list of nodes of a given type |
| GET | `/graph/node/{node_id}` | All properties of one node plus its immediate neighbours |
| GET | `/graph/path?from=X&to=Y` | Shortest directed path between two nodes |
| POST | `/query` | Takes a natural language question and returns a grounded answer |

The interactive API docs are available at `http://localhost:8000/docs` when running locally.

---

## Running It Locally

You need Python 3.10 or above and a free Groq API key from [console.groq.com](https://console.groq.com).

```bash
git clone https://github.com/yerramsettysuchita/GraphO2C.git
cd GraphO2C/backend

pip install -r requirements.txt

cp .env.example .env
# Open .env and paste your key: GROQ_API_KEY=gsk_...

python main.py
```

The server starts at `http://localhost:8000`. The first run reads all Dataset files, writes a `grapho2c.duckdb` file, and builds the graph in memory. Every run after that reuses the DuckDB file, so startup is fast. Delete `grapho2c.duckdb` if you want to force a full re-ingestion.

Open `http://localhost:8000` in your browser to use the graph explorer.

---

## Example Questions to Try

These questions were used during testing and cover the main query patterns the system handles.

| Question | What it exercises |
|---|---|
| Which products are associated with the highest number of billing documents? | Multi-table aggregation |
| Trace the full flow of sales order 740506 | Graph traversal trace |
| Identify sales orders that have been delivered but not billed | Gap analysis with set logic |
| Which customers have the highest total net amount across all orders? | Revenue ranking |
| Show all billing documents that were cancelled | Status field filtering |

---

## Project Structure

```
GraphO2C/
├── Dataset/                     ← 20 JSONL table folders (SAP O2C data)
│   ├── sales_order_headers/
│   ├── billing_document_headers/
│   └── 18 more tables ...
├── backend/
│   ├── main.py                  ← Entry point: ingest, build graph, start API
│   ├── db.py                    ← DuckDB connection (singleton)
│   ├── ingestion.py             ← Glob-based JSONL ingestion and view creation
│   ├── graph_builder.py         ← Builds the NetworkX DiGraph (13 edge types)
│   ├── llm.py                   ← Two-step Groq pipeline + trace query logic
│   ├── api.py                   ← FastAPI route handlers
│   ├── requirements.txt
│   ├── render.yaml              ← Single-service Render deployment config
│   └── .env.example             ← Safe API key template
└── frontend/
    └── index.html               ← Full UI in one file, no build step needed
```

---

## Dataset

The dataset contains SAP-style synthetic Order-to-Cash data split across 20 tables in JSONL format. Download and place it at `GraphO2C/Dataset/` before running.

**Download:** [Google Drive](https://drive.google.com/file/d/1UqaLbFaveV-3MEuiUrzKydhKmkeC1iAL/view)

The tables cover the full O2C cycle: `sales_order_headers`, `sales_order_items`, `sales_order_schedule_lines`, `outbound_delivery_headers`, `outbound_delivery_items`, `billing_document_headers`, `billing_document_items`, `billing_document_cancellations`, `journal_entry_items_accounts_receivable`, `payments_accounts_receivable`, `business_partners`, `business_partner_addresses`, `customer_company_assignments`, `customer_sales_area_assignments`, `products`, `product_descriptions`, `product_plants`, `product_storage_locations`, and `plants`.
