# GraphO2C — Graph-Based Order-to-Cash Query System

![CI](https://github.com/yerramsettysuchita/GraphO2C/actions/workflows/test.yml/badge.svg)

**Live demo:** https://grapho2c.onrender.com

SAP's Order-to-Cash process spans at least a dozen tables. A sales order becomes a delivery, a delivery becomes an invoice, an invoice becomes a journal entry, and a journal entry becomes a payment. When this data lives in flat tables, tracing those connections requires complex SQL joins across every step. GraphO2C solves that by loading all 20 SAP tables into a single knowledge graph and letting you explore and query the full O2C flow visually and in plain English.

You type a question. The system figures out what SQL to run, executes it against DuckDB, and returns a grounded answer with the relevant graph nodes highlighted on screen.

---

## What It Does

- Ingests 20 JSONL tables of SAP-style O2C data into DuckDB
- Builds a directed NetworkX graph with **1,262 nodes** and **2,059 edges**
- Visualises the graph in the browser using Cytoscape.js — click any node to inspect its properties and expand its neighbours
- Accepts natural language questions, generates SQL dynamically, executes it, and returns data-backed answers
- A separate **trace mode** walks the graph directly for questions like *"show me the full flow of sales order 740506"*, returning the complete chain: SalesOrder → DeliveryItem → BillingDocument → JournalEntry → Payment
- Highlights the nodes referenced in each answer on the graph automatically

---

## Architecture Decisions

### Why a graph at all?

The O2C process is inherently relational and directional. A sales order spawns items, items spawn delivery items, delivery items spawn billing documents, billing documents post to journal entries, journal entries clear into payments. Representing this as a directed graph makes traversal and gap detection natural — finding "orders delivered but not billed" is a graph reachability query, not a five-table join.

### Why DuckDB instead of PostgreSQL or SQLite?

The dataset is read-only and analytical. DuckDB reads multi-part JSONL files with a single glob expression (`read_json_auto('folder/part-*.jsonl')`) and runs fast columnar aggregations with no server process, no connection pooling, and no schema migration. The `.duckdb` file travels with the project, making the deployment self-contained. PostgreSQL would add a server to manage and a migration pipeline to maintain with zero benefit for this workload.

### Why NetworkX instead of Neo4j?

At 1,262 nodes and 2,059 edges, the graph fits comfortably in memory. NetworkX gives shortest-path, BFS, and neighbour lookup as single function calls. Neo4j makes sense when you have tens of millions of nodes, concurrent writes, or need a persistent graph database that survives process restarts. For this scale, Neo4j would add significant operational overhead — a separate process, a driver, a query language (Cypher) — without any measurable benefit.

### Why Groq with llama-3.3-70b-versatile?

Groq's inference hardware runs llama-3.3-70b at speeds that make a two-step pipeline (SQL generation then answer synthesis) feel interactive rather than slow. The model is large enough to follow a complex schema prompt reliably and generate correct DuckDB SQL on the first try in the majority of cases.

### Two-step pipeline instead of one prompt

Separating SQL generation from answer synthesis means each step can be tuned independently. SQL generation runs at temperature 0.0 (deterministic, same question always produces the same SQL). Answer synthesis runs at temperature 0.3 (slightly looser for natural language). If they were combined, tuning one would compromise the other.

---

## Database Choice

**DuckDB** is used as the analytical store. Key decisions:

- **Storage:** A single `.duckdb` file committed to the repository. Render serves it without any database server.
- **Ingestion:** `ingestion.py` reads all `part-*.jsonl` files from each Dataset folder using DuckDB's `read_json_auto` glob. Twenty raw tables are loaded, then two denormalised views (`v_customer`, `v_product`) are created to flatten joins the LLM would otherwise have to reconstruct.
- **Smart cold start:** On startup, the code checks whether tables already exist. If yes, ingestion is skipped. This means the first deploy is slow (full ingest) but every subsequent restart is fast.
- **Graph construction:** After ingestion, `graph_builder.py` queries DuckDB to join tables and build all nodes and edges in memory using NetworkX. DuckDB is then used only at query time for SQL-mode answers.

---

## Graph Modelling

The graph has **11 node types** and **13 directed edge types**.

### Node Types

| Node Type | Source | Primary Key |
|---|---|---|
| Customer | v_customer (business_partners + addresses + assignments) | businessPartner |
| Product | v_product (products + product_descriptions) | product |
| Plant | plants | plant |
| SalesOrder | sales_order_headers | salesOrder |
| SalesOrderItem | sales_order_items | salesOrder + salesOrderItem |
| OutboundDelivery | outbound_delivery_headers | deliveryDocument |
| DeliveryItem | outbound_delivery_items | deliveryDocument + deliveryDocumentItem |
| BillingDocument | billing_document_headers | billingDocument |
| BillingDocItem | billing_document_items | billingDocument + billingDocumentItem |
| JournalEntry | journal_entry_items | companyCode + fiscalYear + accountingDocument + item |
| Payment | payments | companyCode + fiscalYear + accountingDocument + item |

### Edge Types (the O2C flow)

```
SalesOrder      ──PLACED_BY──►      Customer
SalesOrder      ──HAS_ITEM──►       SalesOrderItem
SalesOrderItem  ──ORDERS_PRODUCT──► Product
SalesOrderItem  ──PRODUCED_AT──►    Plant
SalesOrderItem  ──FULFILLED_BY──►   DeliveryItem
DeliveryItem    ──PART_OF──►        OutboundDelivery
DeliveryItem    ──BILLED_AS──►      BillingDocItem
BillingDocItem  ──BILLED_IN──►      BillingDocument
BillingDocument ──BILLED_TO──►      Customer
BillingDocument ──POSTED_TO──►      JournalEntry
BillingDocument ──CANCELS──►        BillingDocument  (cancellation link)
JournalEntry    ──CLEARED_BY──►     Payment
Payment         ──PAID_BY──►        Customer
```

Every node ID follows the pattern `TypeName_PrimaryKey` — for example `SalesOrder_740506` or `DeliveryItem_80738076_000010`. This convention means SQL result columns can be mapped back to graph nodes for automatic highlighting without any additional lookup.

---

## LLM Prompting Strategy

The pipeline has two distinct LLM calls per query.

### Step 1 — SQL Generation (temperature 0.0)

The system prompt is approximately 180 lines and contains:

1. **Full schema listing** — every table name, every column name, primary keys, and whether the column is a boolean or VARCHAR in DuckDB (this distinction matters because DuckDB booleans do not accept `'X'` or `'true'`, only `TRUE`/`FALSE`)
2. **Join chain** — the exact sequence of foreign key relationships from `sales_order_headers` through to `payments`
3. **SAP status code reference** — what `A`, `B`, `C` mean for `overallDeliveryStatus`, `overallGoodsMovementStatus`, and `overallOrdReltdBillgStatus`
4. **Execution rules** — use DuckDB syntax (`ILIKE`, `TRY_CAST`, `strftime`), no recursive CTEs, default `LIMIT 50` on SELECT queries, return only a JSON envelope: `{"query_type": "sql", "sql": "...", "explanation": "..."}`
5. **Off-topic instruction** — if the question has nothing to do with the O2C dataset, return the literal string `OFF_TOPIC` with no JSON envelope

Temperature 0.0 means the same question always produces the same SQL. This makes regression testing straightforward.

### Step 2 — Answer Synthesis (temperature 0.3)

The SQL executes against DuckDB. The results (capped at 100 rows) are sent back to the LLM with a short prompt that says: answer based only on what the data shows, not on general knowledge. If exactly 100 rows are returned, the model is warned that results may be truncated and must not state a definitive count.

### Trace Mode (graph traversal, no SQL)

For questions containing words like *"trace"*, *"flow"*, *"journey"*, or specific document IDs, the pipeline detects the pattern and switches to graph traversal instead of SQL. It finds the matching node in NetworkX using BFS, collects all reachable O2C nodes in flow order (SalesOrder → SalesOrderItem → DeliveryItem → OutboundDelivery → BillingDocument → JournalEntry → Payment), and sends that structured data to the LLM for synthesis. This gives a richer, path-aware answer that SQL cannot easily produce.

### Automatic SQL Repair

If Step 1 produces SQL that fails on execution, the error message is sent back to the LLM for one correction attempt. Only if the corrected SQL also fails does the error surface to the user.

---

## Guardrails

The system has six layers of guardrail:

| Layer | What it does |
|---|---|
| Input length cap | Questions longer than 500 characters are rejected before any LLM call |
| OFF_TOPIC — raw output check | If the LLM returns the literal string `OFF_TOPIC`, the user gets: *"This system is designed to answer questions related to the Order-to-Cash dataset only."* |
| OFF_TOPIC — JSON field check | If the parsed JSON has `"sql": "OFF_TOPIC"`, the same rejection fires — prevents envelope manipulation |
| JSON parse guard | Malformed LLM output triggers an error response rather than a crash |
| Result truncation warning | If results hit 100 rows, the synthesis prompt warns the model not to state a definitive count |
| Rate limit handling | Groq `RateLimitError` returns a friendly capacity message rather than a 500 error |

**Tested examples:**
- *"What is the capital of France?"* → guardrail rejects correctly
- *"Write me a poem"* → guardrail rejects correctly
- *"Which products have the most billing documents?"* → SQL generated and executed correctly

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Node and edge counts — confirms graph loaded |
| GET | `/graph/summary` | Counts broken down by node type and edge type |
| GET | `/graph/nodes?type=X&limit=50` | Paginated list of nodes of a given type |
| GET | `/graph/node/{node_id}` | All properties of one node plus its 1-hop neighbours |
| GET | `/graph/path?from=X&to=Y` | Shortest directed path between two nodes |
| POST | `/query` | Natural language question → grounded answer + nodes referenced |
| GET | `/metrics` | Live uptime, query count, error rate, memory usage |

Interactive API docs: https://grapho2c.onrender.com/docs

---

## Example Questions

| Question | Query mode |
|---|---|
| Which products are associated with the highest number of billing documents? | SQL — multi-table aggregation |
| Trace the full flow of sales order 740506 | Graph traversal trace |
| How many orders were delivered but not billed? | SQL — gap analysis |
| Which customers have the highest total order value? | SQL — revenue ranking |
| Show all billing documents that were cancelled | SQL — status filtering |
| What is the journal entry linked to billing document 90504204? | SQL — document lookup |

---

## Running Locally

Requires Python 3.10+ and a free Groq API key from [console.groq.com](https://console.groq.com).

```bash
git clone https://github.com/yerramsettysuchita/GraphO2C.git
cd GraphO2C/backend

pip install -r requirements.txt

cp .env.example .env
# Edit .env and set: GROQ_API_KEY=gsk_...

python main.py
```

Open `http://localhost:8000` in your browser.

The first run ingests all Dataset files and writes `grapho2c.duckdb`. Every subsequent run reuses the file. Delete it to force a full re-ingestion.

---

## Project Structure

```
GraphO2C/
├── Dataset/                     ← 20 JSONL table folders (SAP O2C data)
├── backend/
│   ├── main.py                  ← Entry point: ingest, build graph, start API
│   ├── db.py                    ← DuckDB singleton connection
│   ├── ingestion.py             ← Glob-based JSONL ingestion + denormalised views
│   ├── graph_builder.py         ← Builds the NetworkX DiGraph (13 edge types)
│   ├── llm.py                   ← Two-step Groq pipeline + graph trace mode
│   ├── api.py                   ← FastAPI routes + static file serving
│   ├── metrics.py               ← In-process query/error counters
│   ├── constants.py             ← Node type colours shared with tests
│   ├── requirements.txt
│   └── .env.example
├── frontend/
│   ├── src/
│   │   ├── App.jsx              ← Root layout
│   │   ├── api.js               ← Fetch wrapper for all API calls
│   │   ├── hooks/
│   │   │   ├── useServer.js     ← Polls /health until graph is ready
│   │   │   ├── useGraph.js      ← Cytoscape lifecycle: load, expand, highlight, search
│   │   │   └── useChat.js       ← Chat state, 45s timeout, slow-query indicator
│   │   └── components/
│   │       ├── TopBar.jsx
│   │       ├── GraphCanvas.jsx
│   │       ├── ChatPanel.jsx
│   │       ├── NodeInspector.jsx
│   │       └── LoadingOverlay.jsx
│   └── vite.config.js
├── frontend-dist/               ← Pre-built bundle (committed — served directly by FastAPI)
├── render.yaml                  ← Render deployment config
└── .github/workflows/test.yml   ← CI: pytest + bundle size check
```
