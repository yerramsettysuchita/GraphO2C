"""
Data ingestion: loads all JSONL part-files from Dataset/ into DuckDB.

One raw table is created per source folder by globbing all part-*.jsonl files.
After raw loading, two denormalized VIEW tables are built:
  - v_customer  : business_partners + addresses + company + sales area (default)
  - v_product   : products + product_descriptions (EN only)
"""

import logging
from pathlib import Path
import duckdb
from db import get_connection

logger = logging.getLogger(__name__)

# Root of the Dataset directory (sibling of GraphO2C/backend → GraphO2C/Dataset)
DATASET_DIR = Path(__file__).parent.parent / "Dataset"

# Mapping: DuckDB table name → Dataset subfolder name
RAW_TABLES: dict[str, str] = {
    "billing_document_cancellations":         "billing_document_cancellations",
    "billing_document_headers":               "billing_document_headers",
    "billing_document_items":                 "billing_document_items",
    "business_partner_addresses":             "business_partner_addresses",
    "business_partners":                      "business_partners",
    "customer_company_assignments":           "customer_company_assignments",
    "customer_sales_area_assignments":        "customer_sales_area_assignments",
    "journal_entry_items":                    "journal_entry_items_accounts_receivable",
    "outbound_delivery_headers":              "outbound_delivery_headers",
    "outbound_delivery_items":                "outbound_delivery_items",
    "payments":                               "payments_accounts_receivable",
    "plants":                                 "plants",
    "product_descriptions":                   "product_descriptions",
    "product_plants":                         "product_plants",
    "product_storage_locations":              "product_storage_locations",
    "products":                               "products",
    "sales_order_headers":                    "sales_order_headers",
    "sales_order_items":                      "sales_order_items",
    "sales_order_schedule_lines":             "sales_order_schedule_lines",
}


def _glob_pattern(folder_name: str) -> str:
    """Return a DuckDB-compatible glob string for all part files in a folder."""
    folder = DATASET_DIR / folder_name
    # Use forward slashes — DuckDB read_json glob works on all platforms
    return (folder / "part-*.jsonl").as_posix()


def load_raw_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop-and-recreate every raw table from JSONL files."""
    logger.info("=== Loading raw tables ===")
    for table, folder in RAW_TABLES.items():
        glob = _glob_pattern(folder)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(
            f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_json(
                '{glob}',
                auto_detect = true,
                union_by_name = true,
                format = 'newline_delimited'
            )
            """
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info("  %-45s %8d rows", table, count)


def create_denormalized_views(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Build denormalized tables for Customer and Product so the graph builder
    can pull a single rich record per entity without repeated joins.
    """
    logger.info("=== Creating denormalized views ===")

    # ------------------------------------------------------------------ #
    # v_customer
    # Joins: business_partners (1:1) + business_partner_addresses (latest)
    #        + customer_company_assignments + customer_sales_area_assignments
    #          (first row per customer — default sales area)
    # ------------------------------------------------------------------ #
    conn.execute("DROP TABLE IF EXISTS v_customer")
    conn.execute(
        """
        CREATE TABLE v_customer AS
        WITH ranked_addr AS (
            -- pick the most recently valid address per business partner
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY businessPartner
                       ORDER BY validityStartDate DESC
                   ) AS rn
            FROM business_partner_addresses
        ),
        ranked_sales AS (
            -- pick one sales area assignment per customer (deterministic)
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer
                       ORDER BY salesOrganization, distributionChannel
                   ) AS rn
            FROM customer_sales_area_assignments
        )
        SELECT
            bp.businessPartner                          AS businessPartner,
            bp.businessPartnerFullName                  AS fullName,
            bp.businessPartnerCategory                  AS category,
            bp.businessPartnerGrouping                  AS grouping,
            bp.businessPartnerIsBlocked                 AS isBlocked,
            bp.isMarkedForArchiving                     AS isArchived,
            bp.creationDate                             AS creationDate,
            bp.lastChangeDate                           AS lastChangeDate,
            -- address fields
            addr.cityName                               AS cityName,
            addr.country                                AS country,
            addr.region                                 AS region,
            addr.postalCode                             AS postalCode,
            addr.streetName                             AS streetName,
            addr.addressTimeZone                        AS timeZone,
            -- company-code fields
            cca.reconciliationAccount                   AS reconciliationAccount,
            cca.customerAccountGroup                    AS accountGroup,
            cca.deletionIndicator                       AS companyDeletionIndicator,
            -- default sales-area fields
            csa.salesOrganization                       AS salesOrganization,
            csa.distributionChannel                     AS distributionChannel,
            csa.currency                                AS currency,
            csa.customerPaymentTerms                    AS paymentTerms,
            csa.incotermsClassification                 AS incoterms,
            csa.shippingCondition                       AS shippingCondition,
            csa.deliveryPriority                        AS deliveryPriority,
            csa.creditControlArea                       AS creditControlArea
        FROM business_partners bp
        LEFT JOIN ranked_addr addr
               ON addr.businessPartner = bp.businessPartner AND addr.rn = 1
        LEFT JOIN customer_company_assignments cca
               ON cca.customer = bp.customer
        LEFT JOIN ranked_sales csa
               ON csa.customer = bp.customer AND csa.rn = 1
        """
    )
    count = conn.execute("SELECT COUNT(*) FROM v_customer").fetchone()[0]
    logger.info("  %-45s %8d rows", "v_customer", count)

    # ------------------------------------------------------------------ #
    # v_product
    # Joins: products + product_descriptions (EN only, one row per product)
    # ------------------------------------------------------------------ #
    conn.execute("DROP TABLE IF EXISTS v_product")
    conn.execute(
        """
        CREATE TABLE v_product AS
        SELECT
            p.product                   AS product,
            p.productType               AS productType,
            p.productGroup              AS productGroup,
            p.baseUnit                  AS baseUnit,
            p.division                  AS division,
            p.industrySector            AS industrySector,
            p.grossWeight               AS grossWeight,
            p.netWeight                 AS netWeight,
            p.weightUnit                AS weightUnit,
            p.isMarkedForDeletion       AS isMarkedForDeletion,
            p.productOldId              AS productOldId,
            p.creationDate              AS creationDate,
            p.lastChangeDate            AS lastChangeDate,
            pd.productDescription       AS productDescription
        FROM products p
        LEFT JOIN product_descriptions pd
               ON pd.product = p.product AND pd.language = 'EN'
        """
    )
    count = conn.execute("SELECT COUNT(*) FROM v_product").fetchone()[0]
    logger.info("  %-45s %8d rows", "v_product", count)


def run_ingestion() -> None:
    conn = get_connection()
    load_raw_tables(conn)
    create_denormalized_views(conn)
    logger.info("=== Ingestion complete ===")


if __name__ == "__main__":
    run_ingestion()
