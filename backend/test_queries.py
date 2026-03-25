"""
End-to-end query tests for GraphO2C.

Usage (server must be running):
  python test_queries.py

Run from:  GraphO2C/backend/
Requires:  GROQ_API_KEY set in .env
"""

import json
import urllib.request
import urllib.error
import sys

BASE = "http://localhost:8000"

QUERIES = [
    "Which products are associated with the highest number of billing documents?",
    "Trace the full flow of sales order 740506",
    "Identify sales orders that have been delivered but not billed",
    "Which customers have the highest total net amount?",
    "What is the capital of France?",   # must be rejected as OFF_TOPIC
]

EXPECTED_TYPES = ["sql", "sql", "sql", "sql", "off_topic"]


def post(path: str, body: dict) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def run_tests() -> None:
    print("=" * 65)
    print(" GraphO2C — End-to-End Query Tests")
    print("=" * 65)

    passed = 0
    failed = 0

    for i, (q, expected_type) in enumerate(zip(QUERIES, EXPECTED_TYPES), 1):
        print(f"\n[{i}/5] {q}")
        status, data = post("/query", {"question": q})

        if status not in (200, 201):
            print(f"  FAIL  HTTP {status}: {data.get('detail', data)}")
            failed += 1
            continue

        query_type = data.get("query_type", "unknown")
        answer = data.get("answer", "")
        sql = data.get("sql_executed")
        row_count = data.get("row_count", 0)
        nodes = data.get("nodes_referenced", [])

        type_ok = query_type == expected_type
        has_answer = bool(answer and len(answer) > 10)

        status_str = "PASS" if (type_ok and has_answer) else "FAIL"
        if type_ok and has_answer:
            passed += 1
        else:
            failed += 1

        print(f"  {status_str}  type={query_type!r} (expected {expected_type!r})")
        if sql:
            display_sql = sql if len(sql) <= 90 else sql[:87] + "..."
            print(f"  SQL  : {display_sql}")
        print(f"  Rows : {row_count}   Nodes highlighted: {len(nodes)}")
        print(f"  Ans  : {answer[:200].strip()}")
        if not type_ok:
            print(f"  ERROR: query_type mismatch")
        if not has_answer:
            print(f"  ERROR: answer too short or empty")

    print(f"\n{'='*65}")
    print(f" Results: {passed} passed, {failed} failed out of {len(QUERIES)}")
    print("=" * 65)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    run_tests()
