"""
test_batch_upload.py — Integration test for the Option-2 batch upload flow.
==========================================================================

What this test covers
---------------------
1. Upload files one by one → rows land in staging tables with a batch_id
2. GET /uploads and GET /uploads/batch return the staged contents
3. POST /uploads/commit runs FK pre-flight and moves staging → real tables
4. FK pre-flight catches missing parents (sub_category pointing at a
   nonexistent category) and returns a 400 with violation details
5. POST /uploads/discard wipes the pending batch

How to run
----------
    cd analyst_agent_v3
    python scripts/test_batch_upload.py

The test uses client_id = 'CLT-TEST-BATCH' so it does NOT touch real data.
Before and after the test, all rows for that client are cleaned out.

Exit code 0 on success, non-zero on failure.
"""
import io
import os
import sys
import uuid

# Make `app.*` importable when running this script directly
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO_ROOT)

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.database import engine
from app.upload_router import router

# Build a minimal app just for this test so we don't depend on main.py
from fastapi import FastAPI
app = FastAPI()
app.include_router(router)
client = TestClient(app)

TEST_CLIENT = "CLT-TEST-BATCH"


# ── Utilities ────────────────────────────────────────────────────────────────

def make_csv(rows: list[dict]) -> bytes:
    """Build an in-memory CSV from a list of row dicts."""
    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8")


def upload(master_type: str, rows: list[dict]):
    """POST a CSV to /uploads/{master_type} for TEST_CLIENT."""
    csv_bytes = make_csv(rows)
    files = {"file": (f"{master_type}.csv", csv_bytes, "text/csv")}
    data = {"clientId": TEST_CLIENT}
    return client.post(f"/api/v1/uploads/{master_type}", files=files, data=data)


def cleanup():
    """Remove anything this test may have left behind (staging + real + batches)."""
    with engine.begin() as conn:
        # Real tables — delete in reverse-FK order
        for table in [
            "line_items", "orders", "customer_reviews", "support_tickets",
            "product_vendor_mapping", "product_prices", "products",
            "sub_sub_categories", "sub_categories", "brands",
            "categories", "vendors", "customers",
        ]:
            conn.execute(
                text(f"DELETE FROM {table} WHERE client_id = :cid"),
                {"cid": TEST_CLIENT},
            )
        # Staging rows for ALL batches this client may have
        for table in [
            "staging_line_items", "staging_orders", "staging_customer_reviews",
            "staging_support_tickets", "staging_product_vendor_mapping",
            "staging_product_prices", "staging_products",
            "staging_sub_sub_categories", "staging_sub_categories",
            "staging_brands", "staging_categories", "staging_vendors",
            "staging_customers",
        ]:
            conn.execute(
                text(f"DELETE FROM {table} WHERE client_id = :cid"),
                {"cid": TEST_CLIENT},
            )
        # Batches
        conn.execute(
            text("DELETE FROM upload_batches WHERE client_id = :cid"),
            {"cid": TEST_CLIENT},
        )


def count_in(table: str, client_id: str = TEST_CLIENT) -> int:
    with engine.begin() as conn:
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid"),
            {"cid": client_id},
        ).fetchone()
        return row[0] if row else 0


def say(msg: str):
    print(f"  {msg}")


def section(title: str):
    print(f"\n── {title} " + "─" * max(1, 70 - len(title)))


# ── The actual test ──────────────────────────────────────────────────────────

def test_happy_path():
    section("TEST 1: happy path — upload several files, commit, verify")

    # NOTE ON ID TYPES:
    # In the real schema, category_id / sub_category_id / sub_sub_category_id /
    # brand_id / vendor_id / product_id are all INTEGER columns. Only client_id,
    # customer_id, and order_id are varchar. So the test IDs for catalog tables
    # must be integers. We use the 999xxx range to stay well clear of real data.

    # 1. Upload categories
    r = upload("category", [
        {"client_id": TEST_CLIENT, "category_id": 999001, "category_name": "Test Grocery"},
        {"client_id": TEST_CLIENT, "category_id": 999002, "category_name": "Test Electronics"},
    ])
    assert r.status_code == 200, f"category upload failed: {r.status_code} {r.text}"
    body = r.json()
    assert body["status"] == "staged", body
    assert body["rowsStaged"] == 2, body
    batch_id = body["batchId"]
    say(f"Uploaded 2 categories into batch {batch_id}")

    # 2. Upload sub_categories (children of categories)
    r = upload("sub_category", [
        {"client_id": TEST_CLIENT, "sub_category_id": 999101, "sub_category_name": "Test Snacks",  "category_id": 999001},
        {"client_id": TEST_CLIENT, "sub_category_id": 999102, "sub_category_name": "Test Laptops", "category_id": 999002},
    ])
    assert r.status_code == 200, f"sub_category upload failed: {r.status_code} {r.text}"
    assert r.json()["batchId"] == batch_id, "batch_id changed between uploads!"
    say("Uploaded 2 sub_categories (same batch)")

    # 3. Upload vendors + brands
    upload("vendor", [
        {"client_id": TEST_CLIENT, "vendor_id": 999201, "vendor_name": "Test Vendor Inc"},
    ]).raise_for_status()
    upload("brand", [
        {"client_id": TEST_CLIENT, "brand_id": 999301, "brand_name": "TestBrand", "vendor_id": 999201},
    ]).raise_for_status()
    say("Uploaded 1 vendor + 1 brand")

    # 4. Upload customer (customer_id is VARCHAR, so strings are fine here)
    upload("customer", [
        {"client_id": TEST_CLIENT, "customer_id": "TCUST-01", "customer_email": "t@test.com", "customer_name": "Test Person"},
    ]).raise_for_status()
    say("Uploaded 1 customer")

    # 5. Check GET /uploads returns 5 staged files
    r = client.get(f"/api/v1/uploads?clientId={TEST_CLIENT}")
    assert r.status_code == 200
    uploads = r.json()
    assert len(uploads) == 5, f"Expected 5 staged file-types, got {len(uploads)}: {uploads}"
    say(f"GET /uploads returns {len(uploads)} staged file types")

    # 6. Check GET /uploads/batch shows the summary
    r = client.get(f"/api/v1/uploads/batch?clientId={TEST_CLIENT}")
    assert r.status_code == 200
    summary = r.json()["pendingBatch"]
    assert summary is not None, "pendingBatch should not be None"
    assert summary["batchId"] == batch_id
    assert summary["totalRows"] == 7  # 2 cat + 2 sub + 1 ven + 1 brand + 1 cust
    say(f"Batch summary: {summary['totalRows']} rows across {len(summary['files'])} files")

    # 7. Verify NOTHING is in real tables yet
    assert count_in("categories") == 0, "categories should still be empty before commit!"
    assert count_in("customers") == 0, "customers should still be empty before commit!"
    say("Real tables empty before commit (correct)")

    # 8. Commit
    r = client.post(f"/api/v1/uploads/commit?clientId={TEST_CLIENT}")
    assert r.status_code == 200, f"commit failed: {r.status_code} {r.text}"
    commit_body = r.json()
    assert commit_body["committed"] is True
    say(f"Commit returned: rowsCommitted={commit_body['rowsCommitted']}")

    # 9. Verify real tables have the data
    assert count_in("categories") == 2
    assert count_in("sub_categories") == 2
    assert count_in("vendors") == 1
    assert count_in("brands") == 1
    assert count_in("customers") == 1
    say("Real tables now contain all committed rows")

    # 10. Staging should be empty for this batch
    assert count_in("staging_categories") == 0
    assert count_in("staging_customers") == 0
    say("Staging tables cleared after commit")

    # 11. Batch should be marked committed, not pending
    with engine.begin() as conn:
        status = conn.execute(
            text("SELECT status FROM upload_batches WHERE batch_id = :bid"),
            {"bid": batch_id},
        ).fetchone()
        assert status and status[0] == "committed", f"batch status should be 'committed', got {status}"
    say("Batch marked 'committed' in upload_batches")


def test_fk_violation():
    section("TEST 2: FK pre-flight catches dangling foreign keys")

    # Upload a sub_category pointing at a category that does NOT exist
    # (not in real table AND not in staging for this batch).
    # category_id 999999 is intentionally outside our test range (999001-999301)
    # so it won't appear anywhere in staging or real.
    r = upload("sub_category", [
        {"client_id": TEST_CLIENT, "sub_category_id": 999199, "sub_category_name": "Orphan", "category_id": 999999},
    ])
    assert r.status_code == 200, f"staging upload failed: {r.text}"
    say("Uploaded a sub_category pointing at nonexistent category (id=999999)")

    # Commit should fail with 400 + violation details
    r = client.post(f"/api/v1/uploads/commit?clientId={TEST_CLIENT}")
    assert r.status_code == 400, f"Expected 400 FK violation, got {r.status_code}: {r.text}"
    body = r.json()["detail"]
    assert "violations" in body, f"Expected violations in detail, got {body}"
    violations = body["violations"]
    # missingKey comes back as an int because the column is integer-typed
    assert any(v["missingKey"] == 999999 for v in violations), \
        f"Expected 999999 in violations, got {violations}"
    say(f"Commit correctly rejected with {len(violations)} FK violation(s)")

    # Staging should still have the bad row (so the user can see + fix it)
    assert count_in("staging_sub_categories") == 1
    say("Staging preserved after failed commit (user can fix and retry)")


def test_discard():
    section("TEST 3: discard throws away the pending batch")

    # Verify there's still a pending batch from test 2
    r = client.get(f"/api/v1/uploads/batch?clientId={TEST_CLIENT}")
    assert r.json()["pendingBatch"] is not None, "Should still have pending batch from test 2"

    # Discard it
    r = client.post(f"/api/v1/uploads/discard?clientId={TEST_CLIENT}")
    assert r.status_code == 200
    body = r.json()
    assert body["discarded"] is True
    assert body["rowsDeleted"] >= 1
    say(f"Discarded batch: {body['rowsDeleted']} staging rows removed")

    # No pending batch remaining
    r = client.get(f"/api/v1/uploads/batch?clientId={TEST_CLIENT}")
    assert r.json()["pendingBatch"] is None
    say("No pending batch after discard (correct)")


def test_replace_semantics():
    section("TEST 4: re-uploading same master_type replaces previous rows")

    r = upload("category", [
        {"client_id": TEST_CLIENT, "category_id": 999801, "category_name": "Version 1"},
    ])
    assert r.json()["rowsStaged"] == 1 and r.json()["rowsReplaced"] == 0
    say("First upload: 1 row staged, 0 replaced")

    # Re-upload same type — should replace, not duplicate
    r = upload("category", [
        {"client_id": TEST_CLIENT, "category_id": 999802, "category_name": "Version 2 A"},
        {"client_id": TEST_CLIENT, "category_id": 999803, "category_name": "Version 2 B"},
    ])
    assert r.json()["rowsStaged"] == 2 and r.json()["rowsReplaced"] == 1
    say("Re-upload: 2 rows staged, 1 row replaced (old 999801 gone)")

    # Staging should have exactly 2 rows now, not 3
    assert count_in("staging_categories") == 2, \
        f"Replace semantics broken: got {count_in('staging_categories')} rows, expected 2"
    say("Staging table has exactly 2 rows (replace worked)")

    # Clean up
    client.post(f"/api/v1/uploads/discard?clientId={TEST_CLIENT}")


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    print("=" * 78)
    print(f"Batch upload integration test — client_id={TEST_CLIENT}")
    print("=" * 78)

    print("\n[setup] Cleaning up any leftover test data...")
    cleanup()

    try:
        test_happy_path()
        test_fk_violation()
        test_discard()
        test_replace_semantics()
    finally:
        print("\n[teardown] Cleaning up test data...")
        cleanup()

    print("\n" + "=" * 78)
    print("ALL TESTS PASSED")
    print("=" * 78)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)
