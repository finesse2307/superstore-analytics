# Databricks notebook source
# MAGIC %md
# MAGIC # Superstore Sales Analytics
# MAGIC
# MAGIC End-to-end pipeline: ingest Kaggle Superstore CSV -> clean with PySpark -> Delta Lake -> SQL analysis
# MAGIC
# MAGIC Dataset: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
# MAGIC - 9,994 order line items, Jan 2014 to Dec 2017
# MAGIC - 793 customers, 4 regions, 3 product categories

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.window import Window

print("Spark version:", spark.version)

DATABASE  = "superstore_analytics"
RAW_PATH  = "dbfs:/mnt/raw/superstore/"
DELTA_PATH = "dbfs:/delta/superstore_analytics/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingest Raw CSV
# MAGIC
# MAGIC Download Sample - Superstore.csv from Kaggle and upload via:
# MAGIC ```
# MAGIC databricks fs cp "Sample - Superstore.csv" dbfs:/mnt/raw/superstore/
# MAGIC ```
# MAGIC The file uses Windows-1252 encoding (standard Excel export format).

# Defining schema explicitly rather than inferSchema to avoid an extra full scan
# and prevent type mismatches on postal_code which has 11 nulls in Vermont rows
raw_schema = StructType([
    StructField("row_id",        IntegerType(), True),
    StructField("order_id",      StringType(),  False),
    StructField("order_date",    StringType(),  True),
    StructField("ship_date",     StringType(),  True),
    StructField("ship_mode",     StringType(),  True),
    StructField("customer_id",   StringType(),  False),
    StructField("customer_name", StringType(),  True),
    StructField("segment",       StringType(),  True),
    StructField("country",       StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("postal_code",   StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("product_id",    StringType(),  False),
    StructField("category",      StringType(),  True),
    StructField("sub_category",  StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("sales",         DoubleType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("discount",      DoubleType(),  True),
    StructField("profit",        DoubleType(),  True),
])

raw_df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "windows-1252")
    .option("multiLine", True)
    .option("escape", '"')
    .schema(raw_schema)
    .load(RAW_PATH)
)

print(f"Rows: {raw_df.count():,} | Columns: {len(raw_df.columns)}")
raw_df.show(3, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Checks

# Null counts - postal_code will show 11 nulls, all others should be 0
print("Null counts per column:")
raw_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in raw_df.columns
]).show(truncate=False)

print("Distinct values:")
for col in ["region", "category", "sub_category", "segment", "ship_mode"]:
    vals = [r[0] for r in raw_df.select(col).distinct().orderBy(col).collect()]
    print(f"  {col} ({len(vals)}): {vals}")

raw_df.select(
    F.min("order_date").alias("earliest_order"),
    F.max("order_date").alias("latest_order"),
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Clean and Enrich

clean_df = (
    raw_df
    # Dates come in as MM/dd/yyyy strings from Excel
    .withColumn("order_date", F.to_date("order_date", "MM/dd/yyyy"))
    .withColumn("ship_date",  F.to_date("ship_date",  "MM/dd/yyyy"))
    # Time columns used later for partitioned Delta writes
    .withColumn("order_year",    F.year("order_date"))
    .withColumn("order_month",   F.month("order_date"))
    .withColumn("order_quarter", F.quarter("order_date"))
    # Some same-day orders will show 0 ship_days, that's expected
    .withColumn("ship_days", F.datediff("ship_date", "order_date"))
    .withColumn(
        "profit_margin_pct",
        F.round(
            F.when(F.col("sales") > 0, F.col("profit") / F.col("sales") * 100)
             .otherwise(None),
            2
        )
    )
    .withColumn(
        "discount_tier",
        F.when(F.col("discount") == 0,    "No Discount")
         .when(F.col("discount") <= 0.10, "Low (1-10%)")
         .when(F.col("discount") <= 0.20, "Mid (11-20%)")
         .otherwise("High (>20%)")
    )
    .withColumn("is_profitable", F.when(F.col("profit") > 0, True).otherwise(False))
    # Flag Vermont rows rather than dropping them - preserves row count for auditing
    .withColumn("has_postal_code", F.col("postal_code").isNotNull())
    # row_id is just an Excel row counter, not a business key
    .drop("row_id")
)

n_before = clean_df.count()
n_after  = clean_df.dropDuplicates(["order_id", "product_id"]).count()
print(f"Dedup check: {n_before:,} before, {n_after:,} after, {n_before - n_after:,} dupes found")

clean_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Window Functions

# Running revenue per customer over time
customer_running_window = (
    Window
    .partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

subcat_profit_window = (
    Window
    .partitionBy("sub_category")
    .orderBy(F.desc("profit"))
)

region_month_window = (
    Window
    .partitionBy("region")
    .orderBy("order_year", "order_month")
)

monthly_region = (
    clean_df
    .groupBy("region", "order_year", "order_month")
    .agg(F.round(F.sum("sales"), 2).alias("monthly_sales"))
    .withColumn("prev_month_sales", F.lag("monthly_sales").over(region_month_window))
    .withColumn(
        "mom_growth_pct",
        F.round(
            (F.col("monthly_sales") - F.col("prev_month_sales"))
            / F.col("prev_month_sales") * 100,
            1
        )
    )
)

monthly_region.filter(
    (F.col("order_year") == 2017) & F.col("mom_growth_pct").isNotNull()
).show(10)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build Summary Tables

yearly_summary = (
    clean_df
    .groupBy("order_year", "region", "category", "segment")
    .agg(
        F.round(F.sum("sales"),    2).alias("total_sales"),
        F.round(F.sum("profit"),   2).alias("total_profit"),
        F.round(F.avg("discount"), 4).alias("avg_discount"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("unique_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn(
        "profit_margin_pct",
        F.round(F.col("total_profit") / F.col("total_sales") * 100, 2)
    )
    .orderBy("order_year", "region", "category")
)

# Tables and Bookcases are the loss-makers - visible in this summary
subcat_summary = (
    clean_df
    .groupBy("category", "sub_category")
    .agg(
        F.round(F.sum("sales"),  2).alias("total_sales"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("num_orders"),
        F.round(F.avg("discount"), 4).alias("avg_discount"),
    )
    .withColumn(
        "profit_margin_pct",
        F.round(F.col("total_profit") / F.col("total_sales") * 100, 2)
    )
    .withColumn(
        "profitability",
        F.when(F.col("total_profit") > 0, "Profitable").otherwise("Loss-Making")
    )
    .orderBy(F.desc("total_sales"))
)

subcat_summary.select(
    "sub_category", "total_sales", "total_profit", "profit_margin_pct", "profitability"
).show(17, truncate=False)

customer_ltv = (
    clean_df
    .groupBy("customer_id", "customer_name", "segment", "region")
    .agg(
        F.round(F.sum("sales"),  2).alias("lifetime_sales"),
        F.round(F.sum("profit"), 2).alias("lifetime_profit"),
        F.countDistinct("order_id").alias("num_orders"),
        F.sum("quantity").alias("total_units"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("product_id").alias("unique_products"),
    )
    .withColumn(
        "value_tier",
        F.when(F.col("lifetime_sales") > 10000, "High Value")
         .when(F.col("lifetime_sales") > 5000,  "Mid Value")
         .otherwise("Low Value")
    )
)

customer_ltv.groupBy("value_tier").agg(
    F.count("*").alias("num_customers"),
    F.round(F.avg("lifetime_sales"), 2).alias("avg_lifetime_sales"),
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Delta Lake

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS superstore_analytics
# MAGIC LOCATION 'dbfs:/delta/superstore_analytics/';

# Partitioning by year means queries on a single year skip other partitions entirely
(
    clean_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_year")
    .saveAsTable(f"{DATABASE}.fact_orders")
)

for table_name, df in [
    ("agg_yearly_summary", yearly_summary),
    ("agg_subcat_summary", subcat_summary),
    ("agg_customer_ltv",   customer_ltv),
    ("agg_monthly_region", monthly_region),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{DATABASE}.{table_name}")
    )
    print(f"Written: {DATABASE}.{table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optimize Delta Tables

# MAGIC %sql
# MAGIC OPTIMIZE superstore_analytics.fact_orders
# MAGIC ZORDER BY (customer_id, product_id, region);

# MAGIC %sql
# MAGIC OPTIMIZE superstore_analytics.agg_subcat_summary
# MAGIC ZORDER BY (category, sub_category);

# MAGIC %sql
# MAGIC DESCRIBE HISTORY superstore_analytics.fact_orders;

# COMMAND ----------
# MAGIC %md
# MAGIC ## SQL Analysis

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT order_id)    AS total_orders,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     ROUND(SUM(sales),  2)       AS total_sales,
# MAGIC     ROUND(SUM(profit), 2)       AS total_profit,
# MAGIC     ROUND(SUM(profit) / SUM(sales) * 100, 2) AS overall_margin_pct,
# MAGIC     SUM(quantity) AS total_units_sold
# MAGIC FROM superstore_analytics.fact_orders;

# MAGIC %sql
# MAGIC -- Tables and Bookcases both lose money despite decent sales volume
# MAGIC SELECT
# MAGIC     sub_category,
# MAGIC     category,
# MAGIC     ROUND(SUM(sales),  2) AS total_sales,
# MAGIC     ROUND(SUM(profit), 2) AS total_profit,
# MAGIC     ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct,
# MAGIC     ROUND(AVG(discount) * 100, 1) AS avg_discount_pct
# MAGIC FROM superstore_analytics.fact_orders
# MAGIC GROUP BY sub_category, category
# MAGIC HAVING total_profit < 0
# MAGIC ORDER BY total_profit ASC;

# MAGIC %sql
# MAGIC WITH yearly AS (
# MAGIC     SELECT
# MAGIC         category,
# MAGIC         order_year,
# MAGIC         ROUND(SUM(sales), 2) AS sales
# MAGIC     FROM superstore_analytics.fact_orders
# MAGIC     GROUP BY category, order_year
# MAGIC ),
# MAGIC with_prev AS (
# MAGIC     SELECT *,
# MAGIC         LAG(sales) OVER (PARTITION BY category ORDER BY order_year) AS prev_year_sales
# MAGIC     FROM yearly
# MAGIC )
# MAGIC SELECT
# MAGIC     category,
# MAGIC     order_year,
# MAGIC     sales,
# MAGIC     prev_year_sales,
# MAGIC     ROUND((sales - prev_year_sales) / prev_year_sales * 100, 1) AS yoy_growth_pct
# MAGIC FROM with_prev
# MAGIC WHERE prev_year_sales IS NOT NULL
# MAGIC ORDER BY category, order_year;

# MAGIC %sql
# MAGIC -- Discounts above 20% make the segment unprofitable overall
# MAGIC SELECT
# MAGIC     discount_tier,
# MAGIC     COUNT(*) AS num_line_items,
# MAGIC     ROUND(AVG(sales),  2) AS avg_sales_per_item,
# MAGIC     ROUND(AVG(profit), 2) AS avg_profit_per_item,
# MAGIC     ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct
# MAGIC FROM superstore_analytics.fact_orders
# MAGIC GROUP BY discount_tier
# MAGIC ORDER BY margin_pct DESC;

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     region,
# MAGIC     lifetime_sales,
# MAGIC     lifetime_profit,
# MAGIC     num_orders,
# MAGIC     ROUND(lifetime_profit / lifetime_sales * 100, 2) AS margin_pct,
# MAGIC     value_tier
# MAGIC FROM superstore_analytics.agg_customer_ltv
# MAGIC ORDER BY lifetime_sales DESC
# MAGIC LIMIT 10;

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ship_mode,
# MAGIC     COUNT(DISTINCT order_id) AS num_orders,
# MAGIC     ROUND(AVG(ship_days), 1) AS avg_ship_days,
# MAGIC     ROUND(SUM(sales),  2)    AS total_sales,
# MAGIC     ROUND(AVG(profit), 2)    AS avg_profit_per_item,
# MAGIC     ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct
# MAGIC FROM superstore_analytics.fact_orders
# MAGIC GROUP BY ship_mode
# MAGIC ORDER BY avg_ship_days;

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region,
# MAGIC     ROUND(SUM(sales), 2) AS region_sales,
# MAGIC     ROUND(SUM(sales) / SUM(SUM(sales)) OVER () * 100, 1) AS sales_share_pct,
# MAGIC     ROUND(SUM(profit), 2) AS region_profit,
# MAGIC     ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct
# MAGIC FROM superstore_analytics.fact_orders
# MAGIC GROUP BY region
# MAGIC ORDER BY region_sales DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Time Travel and MERGE

prev_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("superstore_analytics.fact_orders")
)
print(f"Version 0 row count: {prev_df.count():,}")

# MERGE handles late-arriving corrections without reprocessing the full table
from delta.tables import DeltaTable

corrections = spark.createDataFrame([
    ("CA-2017-152156", "FUR-BO-10001798", 261.96, 5, 41.91),
], ["order_id", "product_id", "sales", "quantity", "profit"])

delta_tbl = DeltaTable.forName(spark, "superstore_analytics.fact_orders")

(
    delta_tbl.alias("target")
    .merge(
        corrections.alias("source"),
        "target.order_id = source.order_id AND target.product_id = source.product_id"
    )
    .whenMatchedUpdate(set={
        "sales":    "source.sales",
        "quantity": "source.quantity",
        "profit":   "source.profit",
    })
    .execute()
)

print("MERGE complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Export for Dashboard

OUTPUT = "dbfs:/mnt/outputs/superstore_dashboard/"

exports = {
    "yearly_trend":   spark.table("superstore_analytics.agg_yearly_summary"),
    "subcat_perf":    spark.table("superstore_analytics.agg_subcat_summary"),
    "customer_ltv":   spark.table("superstore_analytics.agg_customer_ltv"),
    "monthly_region": spark.table("superstore_analytics.agg_monthly_region"),
}

for name, df in exports.items():
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(OUTPUT + name + "/")
    )
    print(f"Exported: {OUTPUT + name}/")
