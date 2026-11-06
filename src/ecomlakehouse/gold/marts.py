from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_mart_daily_sales(
    fact_order_items_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """
    Build Daily Sales & GMV Mart aggregated by Category.
    Grain: 1 row per order_date + category_name.
    """
    # Join fact items with dim_products to get category context
    joined_df = (
        fact_order_items_df
        .join(
            dim_product_df,
            on='product_id',
            how='left'
        )
    )

    return (
        joined_df
        .groupBy(
            F.col("order_date").cast("date").alias("order_date"),
            F.coalesce(
                F.col("category_name"),
                F.lit("UNKNOWN")
            ).alias("category_name")
        )
        .agg(
            F.countDistinct(F.col("order_id")).alias("total_orders"),
            F.sum(F.col("quantity")).alias("total_units_sold"),
            F.sum(F.col("line_total_amount"))
            .cast("decimal(14,2)")
            .alias("gross_revenue_gmv"),
            F.round(
                F.sum("line_total_amount") / F.countDistinct(F.col("order_id"))
            ).alias("average_order_value"),
            F.countDistinct(
                F.when(F.col("order_status") == "DELIVERED", F.col("order_id"))
            ).alias("delivered_orders"),
            F.countDistinct(
                F.when(
                    F.col("order_status") == "CANCELLED", F.col("order_id")
                )
            ).alias("cancelled_orders"),
        )
        .withColumn("mart_updated_at", F.current_timestamp())
        .orderBy(
            F.col("order_date").desc(),
            F.col("gross_revenue_gmv").desc()
        )
    )


def build_mart_payment_summary(
    fact_order_items_df: DataFrame
) -> DataFrame:
    """
    Build Payment Performance Mart.
    Grain: 1 row per payment_method + payment_status.
    """

    # Distinct at order level so payment amounts aren't duplicated across items
    orders_payment_level = (
        fact_order_items_df
        .select(
            F.col("order_id"),
            F.col("payment_method"),
            F.col("payment_status"),
            F.col("line_total_amount")
        )
        .groupBy(
            F.col("order_id"),
            F.col("payment_method"),
            F.col("payment_status")
        )
        .agg(
            F.sum(F.col("line_total_amount")).alias("order_total_amount")
        )
    )

    return (
        orders_payment_level
        .groupBy(
            F.col("payment_method"),
            F.col("payment_status")
        )
        .agg(
            F.count(F.col("order_id")).alias("total_transactions"),
            F.sum(F.col("order_total_amount"))
            .cast("decimal(14,2)")
            .alias("total_amount_processed"),

        )
        .withColumn("mart_updated_at", F.current_timestamp())
        .orderBy(
            F.col("total_amount_processed").desc()
        )
    )
