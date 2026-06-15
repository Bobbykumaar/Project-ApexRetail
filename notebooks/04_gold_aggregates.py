# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Layer: Business Aggregates + Z-Order Optimization
# MAGIC
# MAGIC **Purpose:** Build analytics-ready Gold tables from `fact_sales` (the ~45M row
# MAGIC Silver fact table), and apply **Z-Order clustering** on high-cardinality keys
# MAGIC so downstream BI queries that filter/join on those keys read far fewer files.
# MAGIC
# MAGIC ### Gold tables produced
# MAGIC | Table | Grain | Z-Ordered on |
# MAGIC |---|---|---|
# MAGIC | `customer_summary`     | 1 row per customer | `customer_id` |
# MAGIC | `product_summary`      | 1 row per product  | `product_id`, `category` |
# MAGIC | `monthly_revenue`       | 1 row per month     | n/a (small) |
# MAGIC | `category_performance`  | 1 row per category | n/a (small) |
# MAGIC | `data_quality_summary`  | 1 row per check     | n/a (small) |
# MAGIC
# MAGIC ### Why Z-Order matters at this scale
# MAGIC `fact_sales` is partitioned by `order_date` (~1,095 daily partitions for a 3-year
# MAGIC range). Partitioning alone helps with date-range filters, but a query like
# MAGIC `WHERE customer_id = 123456` still has to scan every file in every partition.
# MAGIC `OPTIMIZE ... ZORDER BY (customer_id)` co-locates rows with similar `customer_id`
# MAGIC values into the same files, so Delta's data-skipping can prune most files for
# MAGIC point lookups and range filters on that column — without restructuring partitions.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

SILVER_BASE = "dbfs:/mnt/retail_silver"
GOLD_BASE   = "dbfs:/mnt/retail_gold"

PATHS_SILVER = {
    "customers":     f"{SILVER_BASE}/customers",
    "products":      f"{SILVER_BASE}/products",
    "orders":        f"{SILVER_BASE}/orders",
    "order_items":   f"{SILVER_BASE}/order_items",
    "fact_sales":    f"{SILVER_BASE}/fact_sales",
}

PATHS_GOLD = {
    "customer_summary":     f"{GOLD_BASE}/customer_summary",
    "product_summary":      f"{GOLD_BASE}/product_summary",
    "monthly_revenue":      f"{GOLD_BASE}/monthly_revenue",
    "category_performance": f"{GOLD_BASE}/category_performance",
    "data_quality_summary": f"{GOLD_BASE}/data_quality_summary",
}

spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.adaptive.enabled", "true")

fact_sales         = spark.read.format("delta").load(PATHS_SILVER["fact_sales"])
customers_silver   = spark.read.format("delta").load(PATHS_SILVER["customers"])
products_silver    = spark.read.format("delta").load(PATHS_SILVER["products"])
orders_silver      = spark.read.format("delta").load(PATHS_SILVER["orders"])
order_items_silver = spark.read.format("delta").load(PATHS_SILVER["order_items"])

print(f"fact_sales row count: {fact_sales.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `customer_summary` — 1 row per customer (~1M rows)
# MAGIC
# MAGIC Aggregating ~45M `fact_sales` rows down to ~1M customer rows. With the
# MAGIC broadcast join already applied in Silver, `fact_sales` has no remaining
# MAGIC skew on `product_id`, so this group-by on `customer_id` (uniformly
# MAGIC distributed) runs efficiently under AQE.

# COMMAND ----------

customer_summary = (
    fact_sales
    .filter(F.col("status") == "completed")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.max("order_date").alias("last_purchase_date"),
        F.min("order_date").alias("first_purchase_date"),
        F.countDistinct("product_id").alias("unique_products_bought"),
        F.sum(F.col("quantity_anomaly").cast("int")).alias("quantity_anomaly_count"),
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
)

# Enrich with customer attributes
customer_summary = (
    customers_silver
    .select("customer_id", "full_name", "email", "city", "country", "is_premium")
    .join(customer_summary, on="customer_id", how="left")
    .fillna({"total_orders": 0, "total_revenue": 0.0, "quantity_anomaly_count": 0})
)

print(f"customer_summary row count: {customer_summary.count():,}")
display(customer_summary.limit(5))

# COMMAND ----------

(
    customer_summary
    .repartition(20)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_GOLD["customer_summary"])
)
print(f"customer_summary written -> {PATHS_GOLD['customer_summary']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `product_summary` — 1 row per product (~50K rows)
# MAGIC
# MAGIC This is the table most affected by the engineered skew during its construction:
# MAGIC the underlying group-by on `fact_sales.product_id` has ~5% of keys ("hot SKUs")
# MAGIC each accounting for roughly 10x the rows of an average key. AQE's
# MAGIC `skewJoin` / `coalescePartitions` settings (enabled in config) split and
# MAGIC rebalance these large partitions automatically during the shuffle this
# MAGIC `groupBy` triggers.

# COMMAND ----------

product_summary = (
    fact_sales
    .filter(F.col("status").isin("completed", "shipped"))
    .groupBy("product_id")
    .agg(
        F.sum("quantity_clean").alias("total_units_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("unit_price_clean"), 2).alias("avg_selling_price"),
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
)

product_summary = (
    products_silver
    .select("product_id", "product_name", "category", "brand", "price",
            "margin_pct", "is_hot_sku", "is_price_valid")
    .join(product_summary, on="product_id", how="left")
    .fillna({"total_units_sold": 0, "total_revenue": 0.0, "order_count": 0})
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue"))))
)

print(f"product_summary row count: {product_summary.count():,}")

# Validate skew is reflected correctly in the aggregate
hot_share = (
    product_summary
    .groupBy("is_hot_sku")
    .agg(F.sum("total_revenue").alias("revenue"), F.sum("total_units_sold").alias("units"))
)
display(hot_share)

# COMMAND ----------

(
    product_summary
    .repartition(4)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_GOLD["product_summary"])
)
print(f"product_summary written -> {PATHS_GOLD['product_summary']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `monthly_revenue` — time-series trend (~36 rows for 3 years)

# COMMAND ----------

monthly_revenue = (
    fact_sales
    .filter(F.col("status").isin("completed", "shipped"))
    .groupBy("order_year", "order_month", "order_month_label")
    .agg(
        F.sum("revenue").alias("monthly_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.sum("quantity_clean").alias("total_units_sold"),
    )
    .withColumn("monthly_revenue", F.round(F.col("monthly_revenue"), 2))
    .orderBy("order_year", "order_month")
)

print(f"monthly_revenue row count: {monthly_revenue.count():,}")
display(monthly_revenue)

(
    monthly_revenue
    .coalesce(1)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_GOLD["monthly_revenue"])
)
print(f"monthly_revenue written -> {PATHS_GOLD['monthly_revenue']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `category_performance` — revenue share by category (~10 rows)

# COMMAND ----------

category_performance = (
    fact_sales
    .filter(F.col("status").isin("completed", "shipped"))
    .groupBy("category")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity_clean").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("unit_price_clean"), 2).alias("avg_unit_price"),
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("revenue_share_pct",
        F.round(F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()) * 100, 2))
    .orderBy(F.desc("total_revenue"))
)

display(category_performance)

(
    category_performance
    .coalesce(1)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_GOLD["category_performance"])
)
print(f"category_performance written -> {PATHS_GOLD['category_performance']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `data_quality_summary` — operational metrics for monitoring
# MAGIC
# MAGIC A small table summarizing every data quality issue injected by the generator
# MAGIC and caught in Silver. Useful as a dashboard tile or alerting source.

# COMMAND ----------

dq_rows = []

total_customers = customers_silver.count()
invalid_email   = customers_silver.filter(F.col("email").isNull()).count()
dq_rows.append(("customers", "null_or_invalid_email", invalid_email, total_customers))

total_orders     = orders_silver.count()
missing_delivery = orders_silver.filter(F.col("delivery_date_missing")).count()
dq_rows.append(("orders", "missing_delivery_date", missing_delivery, total_orders))

total_items      = order_items_silver.count()
qty_anomalies    = order_items_silver.filter(F.col("quantity_anomaly")).count()
dq_rows.append(("order_items", "negative_or_zero_quantity", qty_anomalies, total_items))

lt_mismatches    = order_items_silver.filter(F.col("line_total_mismatch")).count()
dq_rows.append(("order_items", "line_total_mismatch", lt_mismatches, total_items))

total_products   = products_silver.count()
invalid_price    = products_silver.filter(~F.col("is_price_valid")).count()
dq_rows.append(("products", "invalid_price_lte_zero", invalid_price, total_products))

dq_df = spark.createDataFrame(dq_rows, schema=["table_name", "issue_type", "issue_count", "total_rows"])
dq_df = dq_df.withColumn("issue_pct", F.round(F.col("issue_count") / F.col("total_rows") * 100, 2))

display(dq_df)

(
    dq_df
    .coalesce(1)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_GOLD["data_quality_summary"])
)
print(f"data_quality_summary written -> {PATHS_GOLD['data_quality_summary']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Z-Order Optimization
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY` co-locates rows with similar values in the specified
# MAGIC column(s) within the same data files. This dramatically improves data-skipping
# MAGIC for queries that filter on these columns — most valuable on **high-cardinality**
# MAGIC columns that aren't already the partition key.
# MAGIC
# MAGIC | Table | ZORDER columns | Why |
# MAGIC |---|---|---|
# MAGIC | `fact_sales` (Silver) | `customer_id`, `product_id` | Point lookups + customer/product filters across 1,095 date partitions |
# MAGIC | `customer_summary` | `customer_id` | Primary key lookups from serving layer |
# MAGIC | `product_summary` | `category`, `product_id` | Category-filtered dashboards |
# MAGIC
# MAGIC **Note:** `OPTIMIZE` can be run directly against a Delta path using
# MAGIC the `delta.\`<path>\`` syntax shown below — no table registration required.

# COMMAND ----------

# ── Z-Order the large Silver fact table (most impactful — 45M rows, 1095 partitions) ──
spark.sql(f"OPTIMIZE delta.`{PATHS_SILVER['fact_sales']}` ZORDER BY (customer_id, product_id)")
print("OPTIMIZE + ZORDER complete: fact_sales (customer_id, product_id)")

# COMMAND ----------

# ── Z-Order Gold summary tables on their primary lookup keys ──
spark.sql(f"OPTIMIZE delta.`{PATHS_GOLD['customer_summary']}` ZORDER BY (customer_id)")
print("OPTIMIZE + ZORDER complete: customer_summary (customer_id)")

spark.sql(f"OPTIMIZE delta.`{PATHS_GOLD['product_summary']}` ZORDER BY (category, product_id)")
print("OPTIMIZE + ZORDER complete: product_summary (category, product_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: Vacuum after Optimize
# MAGIC
# MAGIC `OPTIMIZE` rewrites data into new, better-organized files but leaves the old
# MAGIC files in place (for Delta time-travel). On multi-GB tables, run `VACUUM`
# MAGIC periodically to reclaim storage — but only after you're sure you don't need
# MAGIC to time-travel past the retention window (default 7 days).
# MAGIC
# MAGIC ```sql
# MAGIC -- Reclaim space from files older than the default 7-day retention
# MAGIC VACUUM delta.`dbfs:/mnt/retail_silver/fact_sales`;
# MAGIC ```
# MAGIC
# MAGIC Not run automatically in this notebook — uncomment if you're done
# MAGIC investigating historical versions of this run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER COMPLETE")
print("=" * 60)
for name, path in PATHS_GOLD.items():
    print(f"  {name:<22} -> {path}")
print()
print("Z-Order optimization applied:")
print("  - fact_sales (Silver)    ZORDER BY (customer_id, product_id)")
print("  - customer_summary       ZORDER BY (customer_id)")
print("  - product_summary        ZORDER BY (category, product_id)")
print()
print("Pipeline complete. Gold tables are ready for BI / SQL consumption.")
