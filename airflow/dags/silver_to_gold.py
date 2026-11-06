import shutil

from airflow.sdk import Asset, dag, task

from ecomlakehouse.utils.spark import get_spark
from ecomlakehouse.utils.logging_utils import get_logger

from ecomlakehouse.gold.loader import load_silver_table
from ecomlakehouse.gold.writer import write_gold_table

from ecomlakehouse.gold.dimensions import build_dim_date
from ecomlakehouse.gold.dimensions import build_dim_products
from ecomlakehouse.gold.dimensions import build_dim_customers
from ecomlakehouse.gold.marts import build_mart_payment_summary
from ecomlakehouse.gold.marts import build_mart_daily_sales
from ecomlakehouse.gold.facts import build_fact_order_items


SILVER_INGESTION_ASSET = Asset(
    "ecomlakehouse://silver/ingestion_completed"
)
GOLD_INGESTION_ASSET = Asset(
    "ecomlakehouse://gold/transformation_completed"
)

logger = get_logger(__name__)


@dag(
    dag_id="silver_to_gold",
    schedule=[SILVER_INGESTION_ASSET],
    catchup=False,
    tags=["Silver", "Gold", "Asset", "Dimensional_Modeling"],
    max_active_runs=1,
    max_active_tasks=4,
    doc_md="""
    ### Silver to Gold Dimensional Pipeline
    Transforms clean Silver tables into Kimball Star-Schema Dimensions,
    Line-Item Fact Tables, and Business Data Marts.
    Triggered automatically when Silver ingestion completes.
    """,
)
def incremental_silver_to_gold():
    @task
    def transform_dimensions() -> dict:
        """Build and write dim_customers, dim_products, and dim_date."""

        spark = get_spark()
        caches_to_clean = []

        try:
            customers_df, cust_cache = load_silver_table(
                spark=spark,
                table_name='customers'
            )
            products_df, prod_cache = load_silver_table(
                spark=spark,
                table_name="products"
            )
            categories_df, cat_cache = load_silver_table(
                spark=spark,
                table_name="categories"
            )

            caches_to_clean.extend(
                c for c in [cust_cache, prod_cache, cat_cache] if c
            )

            if (customers_df is None
                or products_df is None
                    or categories_df is None):
                logger.warning(
                    "Required Silver dimension tables not found. Skipping.")
                return {
                    "status": "skipped",
                    "reason": "missing_silver_data"
                }

            dim_cust = build_dim_customers(customer_df=customers_df)
            dim_prod = build_dim_products(
                products_df=products_df,
                categories_df=categories_df
            )
            dim_date = build_dim_date(
                spark=spark,
                start_date="2024-01-01",
                end_date="2025-12-31"
            )

            write_gold_table(
                df=dim_cust,
                table_name="dim_customers"
            )
            write_gold_table(
                df=dim_prod,
                table_name="dim_products"
            )
            write_gold_table(
                df=dim_date,
                table_name="dim_date"
            )

            logger.info(
                "Successfully published dim_customers, dim_products, and dim_date to Gold.")

            return {
                "status": "success",
                "dimensions": ["dim_customers", "dim_products", "dim_date"]
            }
        finally:
            for cache_dir in caches_to_clean:
                shutil.rmtree(cache_dir, ignore_errors=True)

    @task
    def transform_facts() -> dict:
        """Build and write fact_order_items."""

        spark = get_spark()
        caches_to_clean = []

        try:
            orders_df, ord_cache = load_silver_table(
                spark=spark,
                table_name='orders'
            )
            order_items_df, items_cache = load_silver_table(
                spark=spark,
                table_name='order_items'
            )
            payments_df, pay_cache = load_silver_table(
                spark=spark,
                table_name='payments'
            )
            payment_methods_df, pm_cache = load_silver_table(
                spark=spark,
                table_name="payment_methods"
            )

            caches_to_clean.extend(
                c for c in [ord_cache, items_cache, pay_cache, pm_cache] if c
            )

            if (orders_df is None
                or order_items_df is None
                or payments_df is None
                    or payment_methods_df is None):
                logger.warning(
                    "Required Silver fact tables not found. Skipping."
                )
                return {
                    "status": "skipped",
                    "reason": "missing_silver_data"
                }

            fact_df = build_fact_order_items(
                orders_df=orders_df,
                order_items_df=order_items_df,
                payments_df=payments_df,
                payment_methods_df=payment_methods_df
            )

            write_gold_table(fact_df, "fact_order_items")

            logger.info("Successfully published fact_order_items to Gold.")

            return {
                "status": "success",
                "facts": ["fact_order_items"]
            }
        finally:
            for cache_dir in caches_to_clean:
                shutil.rmtree(cache_dir, ignore_errors=True)

    @task
    def transform_marts() -> dict:
        """Build and write aggregated business marts."""

        spark = get_spark()
        caches_to_clean = []

        try:
            orders_df, ord_cache = load_silver_table(
                spark=spark,
                table_name="orders"
            )
            order_items_df, items_cache = load_silver_table(
                spark=spark,
                table_name="order_items"
            )
            payments_df, pay_cache = load_silver_table(
                spark=spark,
                table_name="payments"
            )
            payment_methods_df, pm_cache = load_silver_table(
                spark=spark,
                table_name="payment_methods"
            )
            products_df, prod_cache = load_silver_table(
                spark=spark,
                table_name="products"
            )
            categories_df, cat_cache = load_silver_table(
                spark=spark,
                table_name="categories"
            )

            caches_to_clean.extend(
                c for c in [ord_cache, items_cache, pay_cache, pm_cache, prod_cache, cat_cache] if c
            )

            if any(df is None for df in [orders_df,
                                         order_items_df,
                                         payments_df,
                                         payment_methods_df,
                                         products_df,
                                         categories_df]):
                logger.warning(
                    "Required Silver fact tables not found. Skipping."
                )
                return {
                    "status": "skipped",
                    "reason": "missing_silver_data"
                }

            dim_prod = build_dim_products(
                products_df=products_df,
                categories_df=categories_df
            )
            fact_df = build_fact_order_items(
                orders_df=orders_df,
                order_items_df=order_items_df,
                payments_df=payments_df,
                payment_methods_df=payment_methods_df
            )

            mart_sales = build_mart_daily_sales(
                fact_order_items_df=fact_df,
                dim_product_df=dim_prod
            )
            mart_payments = build_mart_payment_summary(
                fact_order_items_df=fact_df
            )

            write_gold_table(
                df=mart_sales,
                table_name="mart_daily_sales",
            )
            write_gold_table(
                df=mart_payments,
                table_name="mart_payment_summary"
            )

            logger.info(
                "Successfully published mart_daily_sales and mart_payment_summary to Gold.")
            return {
                "status": "success",
                "marts": ["mart_daily_sales", "mart_payment_summary"]
            }
        finally:
            for cache_dir in caches_to_clean:
                shutil.rmtree(cache_dir, ignore_errors=True)

    @task(outlets=[GOLD_INGESTION_ASSET])
    def complete_gold_transformation(dim_res: dict, fact_res: dict, mart_res: dict) -> dict:
        """Signal that the complete Medallion Gold transformation has completed."""
        logger.info(
            "Gold Medallion transformation complete. Emitted Asset event: %s",
            GOLD_INGESTION_ASSET.uri,
        )
        return {
            "status": "completed",
            "dim_status": dim_res.get("status"),
            "fact_status": fact_res.get("status"),
            "mart_status": mart_res.get("status"),
        }

    dim_result = transform_dimensions()
    fact_result = transform_facts()

    mart_result = transform_marts()

    [dim_result, fact_result] >> mart_result

    complete_gold_transformation(
        dim_res=dim_result,
        fact_res=fact_result,
        mart_res=mart_result
    )


incremental_silver_to_gold()
