# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer: Cleaning, Deduplication & Skew Handling
# MAGIC
# MAGIC **Purpose:** Transform Bronze tables into clean, deduplicated, validated Silver tables
# MAGIC — at the scale of tens of millions of rows, where naive joins and group-bys on the
# MAGIC `order_items` table will hit **data skew** from the engineered "hot SKU" problem.
# MAGIC
# MAGIC ### What this notebook demonstrates
# MAGIC 1. **Deduplication at scale** using `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`
# MAGIC    — the standard, shuffle-efficient way to dedupe tens of millions of rows
# MAGIC    (avoids `dropDuplicates()` which doesn't let you control *which* duplicate survives)
# MAGIC 2. **Skew-aware join strategy** for `order_items` to `products`:
# MAGIC    - **Broadcast join** for `products` (50K rows, easily fits in memory) — eliminates
# MAGIC      the shuffle on the large side entirely
# MAGIC    - **Salting technique** explained and provided as a reference pattern for
# MAGIC      large-large joins on a skewed key
# MAGIC 3. **Data quality enforcement**: malformed emails, null handling, negative
# MAGIC    quantity/price anomalies, line_total reconciliation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import broadcast

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

BRONZE_BASE = "dbfs:/mnt/retail_bronze"
SILVER_BASE = "dbfs:/mnt/retail_silver"

PATHS_BRONZE = {
    "customers":   f"{BRONZE_BASE}/customers",
    "products":    f"{BRONZE_BASE}/products",
    "orders":      f"{BRONZE_BASE}/orders",
    "order_items": f"{BRONZE_BASE}/order_items",
}

PATHS_SILVER = {
    "customers":   f"{SILVER_BASE}/customers",
    "products":    f"{SILVER_BASE}/products",
    "orders":      f"{SILVER_BASE}/orders",
    "order_items": f"{SILVER_BASE}/order_items",
    "fact_sales":  f"{SILVER_BASE}/fact_sales",
}

spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE skew join handles MODERATE skew automatically by splitting large partitions.
# We additionally use a broadcast join below for the most severe skew case
# (5% of SKUs / 50% of order_items rows) -- broadcasting the small side makes
# the skew on the large side irrelevant for that join.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver `customers` — dedupe + email/contact cleaning
# MAGIC
# MAGIC **Deduplication strategy:** Bronze has ~3% duplicate `customer_id` rows (different
# MAGIC `signup_date`, simulating re-sent CDC records). We keep the **earliest** `signup_date`
# MAGIC per `customer_id` using `ROW_NUMBER()`.
# MAGIC
# MAGIC **Why `ROW_NUMBER()` over `dropDuplicates()`:**
# MAGIC `dropDuplicates(["customer_id"])` keeps an *arbitrary* row — non-deterministic across
# MAGIC runs and unsuitable once you need "keep the most recent" or "keep the earliest" logic.
# MAGIC `ROW_NUMBER()` with an explicit `ORDER BY` is deterministic and lets you express that rule.

# COMMAND ----------

customers_bronze = spark.read.format("delta").load(PATHS_BRONZE["customers"])

dedup_window = Window.partitionBy("customer_id").orderBy(F.col("signup_date").asc())

customers_silver = (
    customers_bronze
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingest_timestamp", "_batch_id", "_source_file")

    # ── Email validation: regex check for a syntactically valid address ────
    .withColumn("email_valid",
        F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
    .withColumn("email",
        F.when(F.col("email_valid"), F.lower(F.trim(F.col("email")))).otherwise(F.lit(None)))

    # ── Normalize text ──────────────────────────────────────────────────
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
    .withColumn("full_name",  F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("city",       F.coalesce(F.initcap(F.trim(F.col("city"))), F.lit("Unknown")))
    .withColumn("country",    F.initcap(F.trim(F.col("country"))))

    .drop("email_valid")
)

print(f"customers_silver row count: {customers_silver.count():,}")
display(customers_silver.limit(5))

# COMMAND ----------

(
    customers_silver
    .repartition(20)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_SILVER["customers"])
)
print(f"customers_silver written -> {PATHS_SILVER['customers']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver `products` — brand normalization + price validation
# MAGIC
# MAGIC No dedup needed (Bronze `products` has no injected duplicates), but:
# MAGIC - `brand` casing is normalized to uppercase, nulls become `"UNKNOWN"`
# MAGIC - Rows with `price <= 0` are **flagged**, not dropped — Gold may want to report
# MAGIC   on these as a data-quality metric rather than silently losing rows

# COMMAND ----------

products_bronze = spark.read.format("delta").load(PATHS_BRONZE["products"])

products_silver = (
    products_bronze
    .drop("_ingest_timestamp", "_batch_id", "_source_file")
    .withColumn("brand", F.upper(F.trim(F.coalesce(F.col("brand"), F.lit("UNKNOWN")))))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("is_price_valid", F.col("price") > 0)
    .withColumn("margin_pct",
        F.when(F.col("price") > 0,
               F.round((F.col("price") - F.col("cost_price")) / F.col("price") * 100, 2))
         .otherwise(F.lit(None)))
)

print(f"products_silver row count: {products_silver.count():,}")
invalid_price_count = products_silver.filter(~F.col("is_price_valid")).count()
print(f"  -> {invalid_price_count:,} products flagged with invalid price (<=0)")

(
    products_silver
    .repartition(4)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS_SILVER["products"])
)
print(f"products_silver written -> {PATHS_SILVER['products']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver `orders` — dedupe on `order_id`, keep latest `order_timestamp`
# MAGIC
# MAGIC Bronze has ~2% duplicate `order_id` rows where the duplicate has a **later**
# MAGIC `order_timestamp` (simulating a re-fired event). Business rule: **keep the latest
# MAGIC timestamp** — it represents the most recent known state of the order.

# COMMAND ----------

orders_bronze = spark.read.format("delta").load(PATHS_BRONZE["orders"])

order_dedup_window = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())

orders_silver = (
    orders_bronze
    .withColumn("_rn", F.row_number().over(order_dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingest_timestamp", "_batch_id", "_source_file")

    .withColumn("delivery_date_missing", F.col("delivery_date").isNull())
    .withColumn("fulfilment_days",
        F.when(F.col("delivery_date").isNotNull(),
               F.datediff(F.col("delivery_date"), F.col("order_date"))))

    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("order_month_label", F.date_format("order_date", "yyyy-MM"))
)

print(f"orders_silver row count: {orders_silver.count():,}")

(
    orders_silver
    .repartition("order_date")
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .save(PATHS_SILVER["orders"])
)
print(f"orders_silver written -> {PATHS_SILVER['orders']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver `order_items` — dedupe + structural anomaly handling
# MAGIC
# MAGIC `order_items.item_id` is unique by construction in the generator (no duplicates
# MAGIC injected at this grain), so this step focuses on **data quality**:
# MAGIC - Negative `quantity` (~1%) -> flagged as `quantity_anomaly`, set to `NULL` for
# MAGIC   aggregation purposes (Gold can report the anomaly count separately)
# MAGIC - Null `unit_price` (~2%) -> filled via `line_total / quantity` where possible
# MAGIC - `line_total` reconciliation: recompute `line_total_clean = unit_price * quantity`
# MAGIC   and flag rows where the original `line_total` deviated (~5%)

# COMMAND ----------

order_items_bronze = spark.read.format("delta").load(PATHS_BRONZE["order_items"])

order_items_silver = (
    order_items_bronze
    .drop("_ingest_timestamp", "_batch_id")

    .withColumn("quantity_anomaly", F.col("quantity") <= 0)
    .withColumn("quantity_clean",
        F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(F.lit(None).cast(IntegerType())))

    # Fill null unit_price from line_total / quantity where both are available
    .withColumn("unit_price_clean",
        F.coalesce(
            F.col("unit_price"),
            F.when(F.col("quantity_clean").isNotNull() & F.col("line_total").isNotNull() & (F.col("quantity_clean") > 0),
                   F.round(F.col("line_total") / F.col("quantity_clean"), 2))
        ))

    .withColumn("line_total_clean",
        F.when(F.col("unit_price_clean").isNotNull() & F.col("quantity_clean").isNotNull(),
               F.round(F.col("unit_price_clean") * F.col("quantity_clean"), 2)))

    .withColumn("line_total_mismatch",
        (F.col("line_total").isNotNull()) &
        (F.col("line_total_clean").isNotNull()) &
        (F.abs(F.col("line_total") - F.col("line_total_clean")) > 0.01))
)

print(f"order_items_silver row count: {order_items_silver.count():,}")

(
    order_items_silver
    .repartition("order_date")
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .save(PATHS_SILVER["order_items"])
)
print(f"order_items_silver written -> {PATHS_SILVER['order_items']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build `fact_sales` — the skewed join
# MAGIC
# MAGIC This is the join that actually feels the engineered skew: `order_items_silver`
# MAGIC (~45M rows) joined to `products_silver` (50K rows) on `product_id`, where 5% of
# MAGIC `product_id` values account for ~50% of `order_items` rows.
# MAGIC
# MAGIC ### Strategy 1 — Broadcast join for `products` (used here)
# MAGIC `products_silver` is small (50K rows, a few MB) and easily fits in executor memory.
# MAGIC Broadcasting it means **no shuffle happens on `order_items` at all** for this join —
# MAGIC the skew in `order_items.product_id` becomes irrelevant because each executor has
# MAGIC the full `products` table locally. This is almost always the right call when one
# MAGIC side of a join is small, regardless of skew on the large side.
# MAGIC
# MAGIC ### Strategy 2 — Salting (reference pattern, see Section 5b)
# MAGIC `orders_silver` (20M rows) is too large to broadcast. The join key here is
# MAGIC `order_id`, which is **not** the skewed column (only `product_id` is engineered
# MAGIC to be skewed) — so a plain join + AQE skew-join handling is sufficient. Salting
# MAGIC is documented below as the reference pattern for a large-large join on a
# MAGIC skewed key (e.g. if you needed to join `order_items` to another large table
# MAGIC on `product_id`).

# COMMAND ----------

order_items_silver = spark.read.format("delta").load(PATHS_SILVER["order_items"])
products_silver    = spark.read.format("delta").load(PATHS_SILVER["products"])
orders_silver      = spark.read.format("delta").load(PATHS_SILVER["orders"])

# ── Strategy 1: broadcast join on the skewed key (product_id) ──────────────
# products_silver (50K rows) is broadcast to every executor -- order_items_silver
# is read straight through with NO shuffle on its 45M rows for this join.
fact_sales = (
    order_items_silver
    .join(broadcast(products_silver.select(
            "product_id", "product_name", "category", "brand", "price", "margin_pct", "is_hot_sku")),
          on="product_id", how="left")
)

# ── Join to orders on order_id (not the skewed column) ─────────────────────
# Both sides are large (45M and 20M rows), but order_id is uniformly distributed
# (it's a monotonically increasing range), so AQE's default skew-join handling
# (enabled in config above) is sufficient -- no salting needed for THIS join.
fact_sales = (
    fact_sales
    .join(orders_silver.select(
            "order_id", "customer_id", "order_date", "status", "payment_method",
            "discount_pct", "order_year", "order_month", "order_month_label"),
          on="order_id", how="left")
)

# ── Final revenue calculation ───────────────────────────────────────────────
fact_sales = (
    fact_sales
    .withColumn("revenue",
        F.when(F.col("line_total_clean").isNotNull(),
               F.round(F.col("line_total_clean") * (1 - F.col("discount_pct") / 100.0), 2)))
)

print("fact_sales schema:")
fact_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Reference: Salting pattern (for large-large skewed joins)
# MAGIC
# MAGIC Not executed in this notebook (not needed for the joins above), but included
# MAGIC as the standard reference implementation. Use this if you ever need to join
# MAGIC `order_items_silver` to another *large* table on the skewed `product_id` column.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import rand, concat, lit, floor, col
# MAGIC
# MAGIC SALT_BUCKETS = 20  # split each skewed key into N sub-keys
# MAGIC
# MAGIC # 1. Add a random salt to the large/skewed side
# MAGIC left_salted = (
# MAGIC     order_items_silver
# MAGIC     .withColumn("salt", floor(rand() * SALT_BUCKETS))
# MAGIC     .withColumn("product_id_salted", concat(col("product_id").cast("string"), lit("_"), col("salt")))
# MAGIC )
# MAGIC
# MAGIC # 2. Explode the other large table across all salt buckets
# MAGIC right_salted = (
# MAGIC     other_large_table
# MAGIC     .crossJoin(spark.range(0, SALT_BUCKETS).withColumnRenamed("id", "salt"))
# MAGIC     .withColumn("product_id_salted", concat(col("product_id").cast("string"), lit("_"), col("salt")))
# MAGIC )
# MAGIC
# MAGIC # 3. Join on the salted key -- rows for hot product_ids are now spread across
# MAGIC #    SALT_BUCKETS partitions instead of landing on one executor
# MAGIC result = left_salted.join(right_salted, "product_id_salted", "left").drop("salt", "product_id_salted")
# MAGIC ```
# MAGIC
# MAGIC **When to reach for salting vs. broadcast vs. AQE skew join:**
# MAGIC
# MAGIC | Situation | Recommended approach |
# MAGIC |---|---|
# MAGIC | One side is small (<~100MB) | Broadcast join — eliminates shuffle entirely |
# MAGIC | Both sides large, moderate skew | AQE `skewJoin.enabled=true` (default in this notebook) |
# MAGIC | Both sides large, severe skew (10x+) | Salting — manually spreads hot keys across partitions |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write `fact_sales`

# COMMAND ----------

(
    fact_sales
    .repartition("order_date")
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .save(PATHS_SILVER["fact_sales"])
)

print(f"fact_sales written -> {PATHS_SILVER['fact_sales']}")
print(f"fact_sales row count: {fact_sales.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate the skew is visible (and survived the broadcast join correctly)

# COMMAND ----------

skew_validation = (
    spark.read.format("delta").load(PATHS_SILVER["fact_sales"])
    .groupBy("is_hot_sku")
    .agg(F.count("*").alias("row_count"), F.round(F.sum("revenue"), 2).alias("total_revenue"))
)
display(skew_validation)
# Expect: is_hot_sku=true rows ~= 50% of total row_count, despite being only ~5% of products

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("SILVER TRANSFORMATION COMPLETE")
print("=" * 60)
for name, path in PATHS_SILVER.items():
    print(f"  {name:<14} -> {path}")
print()
print("Data quality flags written:")
print("  - quantity_anomaly      (quantity <= 0) in order_items_silver / fact_sales")
print("  - line_total_mismatch   (original line_total != unit_price * quantity)")
print("  - email handling in customers_silver (invalid -> NULL)")
print("  - is_price_valid in products_silver (price <= 0)")
print("  - delivery_date_missing in orders_silver")
print()
print("Skew handling applied:")
print("  - Broadcast join: order_items (45M) x products (50K) on product_id -- no shuffle on large side")
print("  - AQE skew join enabled for order_items x orders on order_id")
print("  - Salting pattern documented as reference for large-large skewed joins")
print()
print("Next: run 04_gold_aggregates.py")
