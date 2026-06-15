# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Distributed Big Data Generator (Retail Domain)
# MAGIC
# MAGIC **Purpose:** Generate a realistic, multi-gigabyte raw retail dataset entirely in parallel
# MAGIC using Spark — no Python `for` loops, no driver-side data collection.
# MAGIC
# MAGIC **Target volumes**
# MAGIC | Dataset       | Rows         | Approx Size |
# MAGIC |---------------|--------------|-------------|
# MAGIC | customers     | 1,000,000+   | ~250 MB     |
# MAGIC | products      | 50,000+      | ~15 MB      |
# MAGIC | orders        | 20,000,000+  | ~3-4 GB     |
# MAGIC | order_items   | ~45,000,000  | ~4-5 GB     |
# MAGIC
# MAGIC **Engineered data quality + skew issues (intentional)**
# MAGIC - Data skew: 5% of products (the "hot" SKUs) drive ~50% of order_items volume
# MAGIC - Duplicate order_ids with different timestamps (simulates re-fired events / retries)
# MAGIC - Null emails, null phone, null delivery_date
# MAGIC - Malformed emails (~4%)
# MAGIC - Mixed-case / inconsistent brand strings
# MAGIC - Negative / zero quantities and prices (structural anomalies)
# MAGIC - Output partitioned by `order_date` and written as Delta to DBFS paths
# MAGIC
# MAGIC **Why this matters for the rest of the pipeline**
# MAGIC The Bronze/Silver/Gold notebooks downstream are written assuming these specific
# MAGIC problems exist — that's the point. This generator IS the "messy production source system."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import math

# ── Scale knobs ──────────────────────────────────────────────────────────────
# Drop these down (e.g. /100) for a quick local/dev test run.
N_CUSTOMERS   = 1_000_000
N_PRODUCTS    = 50_000
N_ORDERS      = 20_000_000
ITEMS_PER_ORDER_AVG = 2.2          # → ~44M order_items rows

# ── Output paths ─────────────────────────────────────────────────────────────
RAW_BASE = "dbfs:/mnt/retail_raw"
PATH_CUSTOMERS   = f"{RAW_BASE}/customers"
PATH_PRODUCTS    = f"{RAW_BASE}/products"
PATH_ORDERS      = f"{RAW_BASE}/orders"
PATH_ORDER_ITEMS = f"{RAW_BASE}/order_items"

# ── Date range for orders (drives partitioning) ───────────────────────────────
ORDER_DATE_START = "2023-01-01"
ORDER_DATE_END   = "2025-12-31"

# ── Parallelism ───────────────────────────────────────────────────────────────
# Tune to your cluster: aim for partitions roughly = total_cores * 2-4
SHUFFLE_PARTITIONS = 400
spark.conf.set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")   # AQE handles skew at runtime too

print(f"Target rows  → customers: {N_CUSTOMERS:,}  products: {N_PRODUCTS:,}  "
      f"orders: {N_ORDERS:,}  order_items: ~{int(N_ORDERS*ITEMS_PER_ORDER_AVG):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers (1M+ rows)
# MAGIC
# MAGIC Generated via `spark.range()` — every row is computed independently and in parallel
# MAGIC across executors. No data is collected to the driver.
# MAGIC
# MAGIC Injected issues:
# MAGIC - ~4% malformed emails
# MAGIC - ~8% null phone
# MAGIC - ~3% null city
# MAGIC - ~3% duplicate customer_id (simulates upstream CDC re-sends)

# COMMAND ----------

FIRST_NAMES = ["Aarav","Priya","Rahul","Anjali","Vikram","Sneha","Arjun","Neha","Rohan","Kavya",
               "Amit","Divya","James","Sarah","Michael","Emily","David","Jessica","Chris","Ashley"]
LAST_NAMES  = ["Sharma","Patel","Singh","Kumar","Verma","Gupta","Mehta","Smith","Johnson","Williams",
               "Brown","Jones","Miller","Davis","Wilson","Moore","Taylor","Anderson","Thomas","Garcia"]
CITIES      = ["Mumbai","Delhi","Bangalore","Chennai","Hyderabad","Pune","Kolkata","Ahmedabad",
                "New York","Los Angeles","Chicago","Houston","Phoenix","Dallas","San Diego"]
COUNTRIES   = ["India","India","India","India","India","India","India","India","USA","USA","USA","USA","USA","USA","USA"]

customers_base = spark.range(0, N_CUSTOMERS).withColumnRenamed("id", "customer_id")

customers_df = (
    customers_base
    # Deterministic pseudo-random helper columns derived from customer_id
    .withColumn("_r1", (F.col("customer_id") * 2654435761) % 100)            # 0-99
    .withColumn("_r2", (F.col("customer_id") * 40503) % 100)
    .withColumn("_r3", (F.col("customer_id") * 715827883) % len(FIRST_NAMES))
    .withColumn("_r4", (F.col("customer_id") * 2246822519) % len(LAST_NAMES))
    .withColumn("_r5", (F.col("customer_id") * 374761393) % len(CITIES))

    .withColumn("first_name", F.element_at(F.array(*[F.lit(x) for x in FIRST_NAMES]), (F.col("_r3") + 1).cast("int")))
    .withColumn("last_name",  F.element_at(F.array(*[F.lit(x) for x in LAST_NAMES]),  (F.col("_r4") + 1).cast("int")))
    .withColumn("city",       F.element_at(F.array(*[F.lit(x) for x in CITIES]),      (F.col("_r5") + 1).cast("int")))
    .withColumn("country",    F.element_at(F.array(*[F.lit(x) for x in COUNTRIES]),   (F.col("_r5") + 1).cast("int")))

    # ── Email: ~4% malformed (missing @, missing domain, double @@) ──────────
    .withColumn("email_raw",
        F.concat_ws(".", F.lower(F.col("first_name")), F.lower(F.col("last_name")), F.col("customer_id").cast("string")))
    .withColumn("email",
        F.when(F.col("_r1") < 1,  F.concat(F.col("email_raw"), F.lit("gmail.com")))            # missing @
         .when(F.col("_r1") < 2,  F.concat(F.col("email_raw"), F.lit("@@gmail.com")))          # double @@
         .when(F.col("_r1") < 4,  F.lit(None).cast("string"))                                  # null email
         .otherwise(F.concat(F.col("email_raw"), F.lit("@gmail.com")))
    )

    # ── Phone: ~8% null ────────────────────────────────────────────────────
    .withColumn("phone",
        F.when(F.col("_r2") < 8, F.lit(None).cast("string"))
         .otherwise(F.concat(F.lit("+91-"), (F.lit(7000000000) + (F.col("customer_id") % 999999999)).cast("string")))
    )

    # ── City: ~3% null ─────────────────────────────────────────────────────
    .withColumn("city", F.when(F.col("_r2") < 3, F.lit(None).cast("string")).otherwise(F.col("city")))

    # ── signup_date spread over ~4 years ──────────────────────────────────
    .withColumn("signup_date",
        F.date_add(F.to_date(F.lit("2021-01-01")), (F.col("customer_id") % 1460).cast("int")))

    .withColumn("is_premium", F.col("_r1") % 5 == 0)   # 20% premium

    .select("customer_id","first_name","last_name","email","phone","city","country","signup_date","is_premium")
)

# ── Inject ~3% duplicate customer_id rows (different signup_date — simulates re-sent CDC records) ──
dupe_customers = (
    customers_df
    .sample(fraction=0.03, seed=42)
    .withColumn("signup_date", F.date_add(F.col("signup_date"), 1))
)
customers_df = customers_df.unionByName(dupe_customers)

print(f"customers_df row count (incl. ~3% dupes): {customers_df.count():,}")
display(customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Products (50K+ rows) — including the "hot SKU" skew set
# MAGIC
# MAGIC We tag 5% of products as `is_hot_sku = true`. Downstream, order_items generation
# MAGIC will heavily over-sample these products, producing genuine **data skew** —
# MAGIC the same kind you'd see with a handful of viral SKUs dominating order volume.

# COMMAND ----------

CATEGORIES = ["Electronics","Clothing","Home & Kitchen","Books","Sports","Beauty","Grocery","Toys","Automotive","Garden"]
BRANDS_RAW = ["BrandX","BRANDX","brandx","GenericCo","PremiumBrand","ValuePack","NoName",None]

products_base = spark.range(0, N_PRODUCTS).withColumnRenamed("id", "product_id")

products_df = (
    products_base
    .withColumn("_r1", (F.col("product_id") * 2654435761) % 100)
    .withColumn("_r2", (F.col("product_id") * 40503) % len(CATEGORIES))
    .withColumn("_r3", (F.col("product_id") * 715827883) % len(BRANDS_RAW))

    .withColumn("category", F.element_at(F.array(*[F.lit(x) for x in CATEGORIES]), (F.col("_r2") + 1).cast("int")))
    .withColumn("brand",    F.element_at(F.array(*[F.lit(x) for x in BRANDS_RAW]),  (F.col("_r3") + 1).cast("int")))

    .withColumn("product_name", F.concat(F.lit("Product-"), F.col("product_id").cast("string")))

    .withColumn("price",
        F.round((F.col("_r1").cast("double") * 89.99) + 9.99, 2))

    .withColumn("cost_price", F.round(F.col("price") * 0.6, 2))

    # ── Structural anomaly: ~1% negative or zero price ─────────────────────
    .withColumn("price",
        F.when(F.col("_r1") == 0, F.lit(0.0))
         .when(F.col("_r1") == 1, F.lit(-9.99))
         .otherwise(F.col("price")))

    .withColumn("stock_qty", (F.col("_r1") * 7) % 500)
    .withColumn("is_active", F.col("_r1") % 10 != 0)   # 90% active

    # ── THE SKEW FLAG: top 5% of product_ids are "hot SKUs" ────────────────
    .withColumn("is_hot_sku", F.col("product_id") < int(N_PRODUCTS * 0.05))

    .select("product_id","product_name","category","brand","price","cost_price",
            "stock_qty","is_active","is_hot_sku")
)

n_hot = products_df.filter("is_hot_sku").count()
print(f"products_df row count: {products_df.count():,}")
print(f"hot SKUs (5%): {n_hot:,}  — these will drive ~50% of order_items volume")
display(products_df.filter("is_hot_sku").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orders (20M+ rows)
# MAGIC
# MAGIC Generated with `spark.range()` partitioned across `SHUFFLE_PARTITIONS` slices —
# MAGIC each Spark task independently computes its rows; nothing is collected centrally.
# MAGIC
# MAGIC Injected issues:
# MAGIC - ~2% duplicate `order_id` with a **different** `order_timestamp` (re-fired events)
# MAGIC - ~3% null `delivery_date`
# MAGIC - Even spread of `order_date` across the configured date range → drives the
# MAGIC   `order_date` partitioning of the Delta table

# COMMAND ----------

date_start_epoch = F.unix_timestamp(F.lit(ORDER_DATE_START))
date_end_epoch   = F.unix_timestamp(F.lit(ORDER_DATE_END))
date_range_days  = F.datediff(F.to_date(F.lit(ORDER_DATE_END)), F.to_date(F.lit(ORDER_DATE_START)))

STATUSES        = ["completed","completed","completed","shipped","processing","cancelled","returned"]
PAYMENT_METHODS = ["UPI","Credit Card","Debit Card","Net Banking","COD","Wallet"]

orders_base = (
    spark.range(0, N_ORDERS, numPartitions=SHUFFLE_PARTITIONS)
    .withColumnRenamed("id", "order_id")
)

orders_df = (
    orders_base
    .withColumn("_r1", (F.col("order_id") * 2654435761) % 100)
    .withColumn("_r2", (F.col("order_id") * 40503) % 100)
    .withColumn("_r3", (F.col("order_id") * 715827883) % len(STATUSES))
    .withColumn("_r4", (F.col("order_id") * 2246822519) % len(PAYMENT_METHODS))

    .withColumn("customer_id", F.col("order_id") % N_CUSTOMERS)

    .withColumn("order_date",
        F.date_add(F.to_date(F.lit(ORDER_DATE_START)),
                    (F.col("order_id") % date_range_days).cast("int")))

    .withColumn("order_timestamp",
        (F.unix_timestamp(F.col("order_date").cast("timestamp")) + (F.col("order_id") % 86400)).cast("timestamp"))

    .withColumn("status",         F.element_at(F.array(*[F.lit(x) for x in STATUSES]), (F.col("_r3") + 1).cast("int")))
    .withColumn("payment_method", F.element_at(F.array(*[F.lit(x) for x in PAYMENT_METHODS]), (F.col("_r4") + 1).cast("int")))

    # ── delivery_date: ~3% null ──────────────────────────────────────────
    .withColumn("delivery_date",
        F.when(F.col("_r2") < 3, F.lit(None).cast("date"))
         .otherwise(F.date_add(F.col("order_date"), (F.col("_r2") % 14 + 1).cast("int"))))

    .withColumn("discount_pct", (F.col("_r1") * 3) % 30)

    .select("order_id","customer_id","order_date","order_timestamp","status",
            "payment_method","delivery_date","discount_pct")
)

# ── Inject ~2% duplicate order_id rows with a different (later) order_timestamp ──
dupe_orders = (
    orders_df
    .sample(fraction=0.02, seed=7)
    .withColumn("order_timestamp", F.col("order_timestamp") + F.expr("INTERVAL 3 HOURS"))
)
orders_df = orders_df.unionByName(dupe_orders)

print(f"orders_df row count (incl. ~2% dupes): {orders_df.count():,}")
display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Order Items (~45M rows) — the skewed fact table
# MAGIC
# MAGIC This is the most important generation step for testing Spark performance.
# MAGIC
# MAGIC ### How the skew is engineered
# MAGIC For each order, we generate 1-4 line items using `explode()` on a sequence column —
# MAGIC this avoids any driver-side loop entirely. For **each item**, the product is chosen
# MAGIC from one of two pools:
# MAGIC - **80% of the time** → sampled from the 5% "hot SKU" pool (`product_id < N_PRODUCTS*0.05`)
# MAGIC - **20% of the time** → sampled from the remaining 95% of products
# MAGIC
# MAGIC Net effect: ~5% of products receive roughly half of all order_item rows.
# MAGIC Any `GROUP BY product_id` or join on `product_id` downstream will be **skewed** —
# MAGIC exactly what the Silver layer notebook is written to handle.
# MAGIC
# MAGIC ### Other injected issues
# MAGIC - ~2% null `unit_price`
# MAGIC - ~1% negative `quantity` (structural anomaly / return adjustments recorded incorrectly)
# MAGIC - `line_total` sometimes inconsistent with `unit_price * quantity` (~5%) — tests Silver reconciliation logic

# COMMAND ----------

N_HOT_PRODUCTS = int(N_PRODUCTS * 0.05)

# explode a small array [0,1,2,3] onto every order row to create 1-4 item rows per order,
# weighted so the average works out to ~ITEMS_PER_ORDER_AVG
items_per_order_seq = F.array(*[F.lit(i) for i in range(1, 5)])

order_items_df = (
    orders_df
    .select("order_id", "order_date")
    .withColumn("_n_items",
        # deterministic 1-4 based on order_id, weighted toward 2
        F.when((F.col("order_id") % 10) < 4, F.lit(2))
         .when((F.col("order_id") % 10) < 7, F.lit(1))
         .when((F.col("order_id") % 10) < 9, F.lit(3))
         .otherwise(F.lit(4)))
    .withColumn("_item_seq", F.explode(F.sequence(F.lit(1), F.col("_n_items"))))

    .withColumn("item_id", F.concat(F.col("order_id").cast("string"), F.lit("-"), F.col("_item_seq").cast("string")))

    # combine order_id + item_seq into one deterministic hash-like number for product selection
    .withColumn("_pick", (F.col("order_id") * 31 + F.col("_item_seq") * 97) )
    .withColumn("_pick_mod100", F.abs(F.col("_pick")) % 100)

    # ── THE SKEW: 80% of items draw from the hot 5% of product_ids ─────────
    .withColumn("product_id",
        F.when(F.col("_pick_mod100") < 80,
               F.abs(F.col("_pick") * 2654435761) % N_HOT_PRODUCTS)                       # hot pool: 0..N_HOT_PRODUCTS-1
         .otherwise(N_HOT_PRODUCTS + (F.abs(F.col("_pick") * 40503) % (N_PRODUCTS - N_HOT_PRODUCTS)))  # long-tail pool
    )

    .withColumn("_qty_r",  F.abs(F.col("_pick") * 715827883) % 100)
    .withColumn("quantity",
        F.when(F.col("_qty_r") < 1, F.lit(-1))                     # ~1% negative qty anomaly
         .otherwise((F.col("_qty_r") % 5) + 1))

    .withColumn("_price_r", F.abs(F.col("_pick") * 2246822519) % 100)
    .withColumn("unit_price",
        F.when(F.col("_price_r") < 2, F.lit(None).cast("double"))  # ~2% null unit_price
         .otherwise(F.round((F.col("_price_r").cast("double") * 4.5) + 9.99, 2)))

    # ── line_total: ~5% deliberately inconsistent with unit_price * quantity ──
    .withColumn("_lt_r", F.abs(F.col("_pick") * 374761393) % 100)
    .withColumn("line_total",
        F.when(F.col("unit_price").isNull(), F.lit(None).cast("double"))
         .when(F.col("_lt_r") < 5,
               F.round(F.coalesce(F.col("unit_price"), F.lit(0.0)) * F.col("quantity") * 0.9, 2))  # inconsistent
         .otherwise(F.round(F.coalesce(F.col("unit_price"), F.lit(0.0)) * F.col("quantity"), 2)))

    .select("item_id","order_id","order_date","product_id","quantity","unit_price","line_total")
)

print("order_items_df schema:")
order_items_df.printSchema()

# Row count is an action — for 45M rows this triggers a full pass.
# Comment out for faster iteration; uncomment to verify before a big write.
# print(f"order_items_df row count: {order_items_df.count():,}")

display(order_items_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify the engineered skew (sanity check on a SAMPLE)
# MAGIC
# MAGIC Running this on the full 45M rows is expensive — sample first to confirm the
# MAGIC skew ratio looks right before committing to the full write.

# COMMAND ----------

sample_items = order_items_df.sample(fraction=0.01, seed=1)  # ~450K rows

skew_check = (
    sample_items
    .withColumn("is_hot", F.col("product_id") < N_HOT_PRODUCTS)
    .groupBy("is_hot")
    .agg(F.count("*").alias("item_count"))
)
display(skew_check)

# Expect: is_hot=true rows ≈ 50% of total, despite hot products being only 5% of the catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write raw datasets to DBFS as Delta, partitioned by `order_date`
# MAGIC
# MAGIC `customers` and `products` are not date-partitioned (no natural date column at this grain).
# MAGIC `orders` and `order_items` are partitioned by `order_date` — this is the partition
# MAGIC column the Bronze Auto Loader notebook expects.
# MAGIC
# MAGIC **Performance notes:**
# MAGIC - `repartition()` before write controls output file count/size — aim for
# MAGIC   128MB-1GB files per partition for large tables.
# MAGIC - For a 3-year date range with daily partitions (~1095 partitions) and 20M+ orders,
# MAGIC   repartitioning by `order_date` before write avoids creating thousands of tiny files.

# COMMAND ----------

# ── Customers ──────────────────────────────────────────────────────────────
(
    customers_df
    .repartition(20)
    .write
    .format("delta")
    .mode("overwrite")
    .save(PATH_CUSTOMERS)
)
print(f"customers written → {PATH_CUSTOMERS}")

# ── Products ──────────────────────────────────────────────────────────────
(
    products_df
    .repartition(4)
    .write
    .format("delta")
    .mode("overwrite")
    .save(PATH_PRODUCTS)
)
print(f"products written → {PATH_PRODUCTS}")

# COMMAND ----------

# ── Orders — partitioned by order_date ───────────────────────────────────
(
    orders_df
    .repartition("order_date")
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(PATH_ORDERS)
)
print(f"orders written → {PATH_ORDERS}")

# COMMAND ----------

# ── Order Items — partitioned by order_date (largest table, ~45M rows) ───
# repartition("order_date") groups rows by the partition column before write,
# which is critical at this scale to avoid the small-files problem.
(
    order_items_df
    .repartition("order_date")
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(PATH_ORDER_ITEMS)
)
print(f"order_items written → {PATH_ORDER_ITEMS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("RAW DATA GENERATION COMPLETE")
print("=" * 60)
for name, path in [("customers", PATH_CUSTOMERS), ("products", PATH_PRODUCTS),
                    ("orders", PATH_ORDERS), ("order_items", PATH_ORDER_ITEMS)]:
    print(f"  {name:<14} → {path}")
print()
print("Engineered issues present in this dataset:")
print("  - Duplicate customer_id (~3%), duplicate order_id with later timestamp (~2%)")
print("  - Malformed / null emails (~6%), null phone (~8%), null city (~3%)")
print("  - Null delivery_date (~3%), null unit_price (~2%)")
print("  - Negative/zero product price (~2%), negative quantity (~1%)")
print("  - line_total inconsistent with unit_price * quantity (~5%)")
print("  - DATA SKEW: ~5% of product_ids ('hot SKUs') drive ~50% of order_items volume")
print()
print("Next: run 02_bronze_ingestion.py")
