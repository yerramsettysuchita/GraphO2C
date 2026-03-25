# GraphO2C — Graph-Based Order-to-Cash Query System

## Overview

GraphO2C unifies fragmented SAP ERP data spread across 20 tables (orders, deliveries, invoices, payments, customers, products) into a traversable knowledge graph. Users explore the Order-to-Cash flow visually and query it in natural language — the system translates questions into DuckDB SQL, executes them, and returns data-backed answers.

---

## Architecture

### Tech Stack

| Layer | Technology |
|---|---|
| Backend API | Python 3.10+, FastAPI |
| Data layer | DuckDB (file-based, analytical SQL) |
| Graph engine | NetworkX (in-memory DiGraph) |
| LLM | Groq API — `llama-3.3-70b-versatile` (free tier) |
| Frontend | Vanilla JS + Cytoscape.js (single HTML file, zero build step) |

### Why DuckDB?

File-based, no server required. Ingests multi-part JSONL files via glob in a single SQL statement (`read_json('Dataset/table/part-*.jsonl', union_by_name=true)`). Fast analytical queries on a read-heavy static dataset. Zero ops overhead for deployment.

### Why NetworkX over Neo4j?

Dataset fits in memory (1,262 nodes, 1,979 edges). Instant startup, no database server to deploy or manage. Shortest-path and neighbor traversal are single function calls. Neo4j becomes the right call at 10× this scale with concurrent writes.

---

## Graph Schema

### Node Types (9)

| Node Type | Source Table | Primary Key |
|---|---|---|
| Customer | `business_partners` + addresses + assignments | `businessPartner` |
| Product | `products` + `product_descriptions` | `product` |
| Plant | `plants` | `plant` |
| SalesOrder | `sales_order_headers` | `salesOrder` |
| SalesOrderItem | `sales_order_items` | `salesOrder` + `salesOrderItem` |
| OutboundDelivery | `outbound_delivery_headers` | `deliveryDocument` |
| DeliveryItem | `outbound_delivery_items` | `deliveryDocument` + `deliveryDocumentItem` |
| BillingDocument | `billing_document_headers` | `billingDocument` |
| JournalEntry | `journal_entry_items` | `companyCode` + `fiscalYear` + `accountingDocument` + `item` |
| Payment | `payments` | `companyCode` + `fiscalYear` + `accountingDocument` + `item` |

### Core O2C Edge Chain

```
SalesOrder      --PLACED_BY-->       Customer
SalesOrder      --HAS_ITEM-->        SalesOrderItem
SalesOrderItem  --ORDERS_PRODUCT-->  Product
SalesOrderItem  --PRODUCED_AT-->     Plant
SalesOrderItem  --FULFILLED_BY-->    DeliveryItem
DeliveryItem    --PART_OF-->         OutboundDelivery
DeliveryItem    --BILLED_AS-->       BillingDocItem
BillingDocItem  --BILLED_IN-->       BillingDocument
BillingDocument --BILLED_TO-->       Customer
BillingDocument --POSTED_TO-->       JournalEntry
JournalEntry    --CLEARED_BY-->      Payment
Payment         --PAID_BY-->         Customer
```

### Node ID Convention

```
SalesOrder_740506
Customer_310000108
SalesOrderItem_740506_10
DeliveryItem_80738076_000010
JournalEntry_ABCD_2025_9400000222_1
```

---

## LLM Prompting Strategy

### Two-Step Pattern

**Step A — Query generation** (temperature: 0.0)

The LLM receives a system prompt containing:
- All DuckDB table names and column names
- The full O2C join chain with direction comments
- Strict output format rule: return only a JSON object `{"query_type","sql","explanation"}`

The deterministic temperature ensures the same question always produces the same SQL, making debugging reproducible.

**Step B — Answer synthesis** (temperature: 0.3)

SQL is executed against DuckDB. Results (capped at 100 rows) are sent back to the LLM for natural-language synthesis. The response is strictly grounded in the query results — the prompt forbids adding information not present in the data.

### SQL Auto-Retry

On execution failure, the error message is fed back to the model alongside the failing SQL. One correction attempt is made before surfacing the error to the user.

### Node Highlighting

Result column names are mapped to graph node prefixes:

| Column | Node prefix |
|---|---|
| `salesOrder` | `SalesOrder_` |
| `billingDocument` | `BillingDocument_` |
| `deliveryDocument` | `OutboundDelivery_` |
| `customer`, `soldToParty` | `Customer_` |
| `material`, `product` | `Product_` |

This allows the frontend to automatically highlight relevant graph nodes after every query response.

### Guardrails

- **OFF_TOPIC detection**: checked twice — on raw string output and inside parsed JSON
- System prompt rule #1 explicitly restricts to dataset domain only
- Question length capped at 500 characters
- SQL results capped at 100 rows for synthesis step
- Rejection message: *"This system is designed to answer questions related to the Order-to-Cash dataset only."*
- Verified: `"What is the capital of France?"` → correctly rejected

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness check with node/edge totals |
| GET | `/graph/summary` | Node and edge counts by type |
| GET | `/graph/nodes?type=X&limit=50` | Paginated node listing |
| GET | `/graph/node/{node_id}` | Node properties + 1-hop neighbors |
| GET | `/graph/path?from=X&to=Y` | Shortest path between two nodes |
| POST | `/query` | Natural language query → SQL → grounded answer |

Interactive docs: `http://localhost:8000/docs`

---

## How to Run Locally

**Prerequisites:** Python 3.10+, Groq API key (free at [console.groq.com](https://console.groq.com))

```bash
git clone <repo-url>
cd GraphO2C/backend

pip install -r requirements.txt

cp .env.example .env
# Edit .env and set GROQ_API_KEY=gsk_...

python main.py
# Backend: http://localhost:8000
# API docs: http://localhost:8000/docs

# Open the frontend in any browser:
# frontend/index.html
```

The first run ingests all Dataset files into DuckDB and builds the graph.
Subsequent runs reuse the `grapho2c.duckdb` file (delete it to force re-ingestion).

---

## Example Queries

| Question | What it tests |
|---|---|
| Which products are associated with the highest number of billing documents? | Aggregation across join |
| Trace the full flow of sales order 740506 | Full O2C chain join |
| Identify sales orders that have been delivered but not billed | Gap analysis |
| Which customers have the highest total net amount across all orders? | Customer revenue ranking |
| Show all billing documents that were cancelled | Status filtering |

---

## Project Structure

```
GraphO2C/
├── Dataset/                          # 20 JSONL table folders (SAP O2C data)
│   ├── sales_order_headers/
│   ├── billing_document_headers/
│   └── ...                           # 18 more tables
├── backend/
│   ├── main.py                       # Startup: ingest → build graph → serve API
│   ├── db.py                         # DuckDB connection singleton
│   ├── ingestion.py                  # JSONL glob ingestion + denormalized views
│   ├── graph_builder.py              # NetworkX DiGraph construction (13 edge types)
│   ├── llm.py                        # Groq two-step Text-to-SQL pipeline
│   ├── api.py                        # FastAPI route handlers
│   ├── requirements.txt
│   ├── render.yaml                   # Render.com deployment config
│   ├── .env.example                  # Safe template (committed)
│   └── .env                          # Your secrets (gitignored)
└── frontend/
    └── index.html                    # Complete UI — open directly in browser
```

---

## Dataset

The Dataset directory contains SAP-style synthetic O2C data across 20 JSONL tables.
Download from: *(add Google Drive / S3 link here)*

Tables: `sales_order_headers`, `sales_order_items`, `sales_order_schedule_lines`,
`outbound_delivery_headers`, `outbound_delivery_items`, `billing_document_headers`,
`billing_document_items`, `billing_document_cancellations`, `journal_entry_items_accounts_receivable`,
`payments_accounts_receivable`, `business_partners`, `business_partner_addresses`,
`customer_company_assignments`, `customer_sales_area_assignments`, `products`,
`product_descriptions`, `product_plants`, `product_storage_locations`, `plants`
