from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def build_dim_customers(
    customer_df: DataFrame
) -> DataFrame:
    """
    Transform Silver customers into Gold dim_customers dimension.
    Grain: 1 row per customer.
    """

    return (
        customer_df.select(
            F.col("customer_id"),
            F.concat_ws(" ", F.col("first_name"),
                        F.col("last_name")).alias("name"),
            F.trim(F.col("email")).alias("email"),
            F.trim(F.col("phone")).alias("phone_number"),
            F.upper(F.trim(F.col("city"))).alias("city"),
            F.upper(F.trim(F.col("state"))).alias("state"),
            F.coalesce(F.col("country"), F.lit("India")).alias("country"),
            F.col("created_at").alias("customer_since"),
            F.current_timestamp().alias("dim_updated_at"),
        )
        .dropDuplicates(["customer_id"])
    )


def build_dim_products(
        products_df,
        categories_df
) -> DataFrame:
    """
    Denormalize Silver products and categories into Gold dim_products.
    Grain: 1 row per product.
    """

    categories_clean_df = (
        categories_df.select(
            F.col("category_id"),
            F.col("category_name")
        ).dropDuplicates(["category_id"])
    )

    return (
        products_df
        .join(
            categories_clean_df,
            on="category_id",
            how="left"
        )
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("sku"),
            F.col("price"),
            F.when(F.col("price") < 500, "Budget")
            .when(F.col("price").between(500, 1000), "Mid-Range")
            .otherwise("Premium").alias("price_tier"),
            F.col("stock_quantity"),
            F.col("is_active"),
            F.col("category_id"),
            F.coalesce(
                F.col("category_name"),
                F.lit("UNKNOWN")
            ).alias("category_name"),
            F.current_timestamp().alias("dim_updated_at")
        ).dropDuplicates(["product_id"])
    )


def build_dim_date(
    spark: SparkSession,
    start_date: str = '2024-01-01',
    end_date: str = '2026-12-31'
) -> DataFrame:
    """
    Generate standard calendar date dimension.
    Grain: 1 row per calendar day.
    """

    date_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), 
                                to_date('{end_date}'), 
                                interval 1 day))
                                as calendar_date
        """)

    return (
        date_df
        .select(
            F.date_format(
                F.col("calendar_date"),
                "yyyyMMdd"
            ).cast("int").alias("date_key"),
            F.col("calendar_date").alias("calendar_date"),
            F.dayofmonth(F.col("calendar_date")).alias("day_of_month"),
            F.dayofweek(F.col("calendar_date")).alias("day_of_week"),
            F.date_format(F.col("calendar_date"), "EEEE").alias("day_name"),
            F.month(F.col("calendar_date")).alias("month"),
            F.date_format(F.col("calendar_date"), "MMMM").alias("month_name"),
            F.quarter(F.col("calendar_date")).alias("quarter"),
            F.year(F.col("calendar_date")).alias("year"),
            F.when(F.dayofweek(F.col("calendar_date")).isin([1, 7]), True)
            .otherwise(False).alias("is_weekend")
        )
    )
