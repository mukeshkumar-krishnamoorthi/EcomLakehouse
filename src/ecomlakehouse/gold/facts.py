import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def build_fact_order_items(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    payments_df: DataFrame,
    payment_methods_df: DataFrame
) -> DataFrame:
    """
    Build the central Gold Line-Item Fact Table.
    Grain: 1 row per order item.
    Joins: order_items + orders + latest payment + payment method.
    """

    # Resolve payments: Get the latest payment record per order_id
    # (Orders can have multiple retry attempts; we want the most recent payment state)
    payment_with_method = (
        payments_df
        .join(
            payment_methods_df.select(
                F.col("payment_method_id"),
                F.col("method_name")
            ),
            on="payment_method_id",
            how="left"
        )
    )

    payment_window = (
        Window.partitionBy('order_id').orderBy(
            F.col("paid_at").desc_nulls_last(),
            F.col("created_at").desc()
        )
    )

    # Join order_items with orders and payment details
    latest_payment = (
        payment_with_method
        .withColumn(
            "rank",
            F.row_number().over(payment_window)
        )
        .filter(F.col("rank") == 1)
        .select(
            F.col("order_id"),
            F.coalesce(
                F.col("method_name"),
                F.lit("UNKNOWN")
            ).alias("payment_method"),
            F.coalesce(
                F.col("payment_status"),
                F.lit("UNKNOWN")
            ).alias("payment_status"),
            F.col("transaction_reference")
        )
    )

    selcted_orders_df = (
        orders_df.select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("store_id"),
            F.col("order_status"),
            F.col("order_date")
        )
    )

    fact_order_items_df = (
        order_items_df
        .join(
            selcted_orders_df,
            on="order_id",
            how="inner"
        )
        .join(
            latest_payment,
            on="order_id",
            how="left"
        )
        .select(
            F.col("order_item_id"),
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.date_format(
                F.col("order_date"),
                "yyyyMMdd"
            ).cast("int")
            .alias("order_date_key"),
            F.col("order_date"),
            F.col("order_status"),
            F.col("item_status"),
            F.col("quantity"),
            F.col("unit_price"),
            (F.col("quantity") * F.col("unit_price"))
            .cast("decimal(12,2)")
            .alias("line_total_amount"),
            F.coalesce(
                F.col("payment_method"),
                F.lit("UNKNOWN")
            ).alias("payment_method"),
            F.coalesce(
                F.col("payment_status"),
                F.lit("PENDING")
            ).alias("payment_status"),
            F.col("transaction_reference"),
            F.current_timestamp().alias("fact_updated_at")
        )
        .drop_duplicates(['order_item_id'])
    )

    return fact_order_items_df
