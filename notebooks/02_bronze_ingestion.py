# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer: Ingestion + Audit Log
# MAGIC
# MAGIC **Purpose:** Ingest the raw multi-gigabyte Delta sources from `01_big_data_generator.py`
# MAGIC into Bronze tables, with:
# MAGIC - Auto Loader (`cloudFiles` / `readStream`) pattern for the large fact tables
# MAGIC - High-performance batch read for the smaller dimension tables
# MAGIC - An operational audit log table recording every ingestion run (row counts, duration, status)
# MAGIC - No data transformation — Bronze is raw + metadata only (ingest timestamp, source file, batch id)
# MAGIC
# MAGIC **Note on Auto Loader in this project:**
# MAGIC The data generator writes Delta directly (not landing files in a folder for cloudFiles
# MAGIC to discover). In a real production setup, source systems would land Parquet/CSV/JSON
# MAGIC files into a cloud storage path and Auto Loader (`cloudFiles` format) would incrementally
# MAGIC discover and ingest new files. We demonstrate **both patterns** below:
# MAGIC - `order_items` (largest table): Auto Loader-style streaming read using `.readStream`
# MAGIC   with `trigger(availableNow=True)` — the recommended pattern for large incremental batch loads
# MAGIC - `customers` / `products` / `orders`: high-performance batch read (`spark.read`) since these
# MAGIC   are either small or read in full each run

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import uuid
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

RAW_BASE        = "dbfs:/mnt/retail_raw"
BRONZE_BASE     = "dbfs:/mnt/retail_bronze"
CHECKPOINT_BASE = "dbfs:/mnt/retail_checkpoints"

PATHS_RAW = {
    "customers":   f"{RAW_BASE}/customers",
    "products":    f"{RAW_BASE}/products",
    "orders":      f"{RAW_BASE}/orders",
    "order_items": f"{RAW_BASE}/order_items",
}

PATHS_BRONZE = {
    "customers":   f"{BRONZE_BASE}/customers",
    "products":    f"{BRONZE_BASE}/products",
    "orders":      f"{BRONZE_BASE}/orders",
    "order_items": f"{BRONZE_BASE}/order_items",
}

AUDIT_LOG_PATH = f"{BRONZE_BASE}/_audit_log"

BATCH_ID  = str(uuid.uuid4())
INGEST_TS = F.current_timestamp()

print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit Log Helper
# MAGIC
# MAGIC Every ingestion run appends one row per table to `_audit_log`, capturing:
# MAGIC `batch_id`, `table_name`, `row_count`, `source_path`, `status`, `duration_seconds`, `error_message`

# COMMAND ----------

audit_schema = StructType([
    StructField("batch_id",         StringType(),    False),
    StructField("table_name",       StringType(),    False),
    StructField("source_path",      StringType(),    False),
    StructField("target_path",      StringType(),    False),
    StructField("row_count",        IntegerType(),   True),
    StructField("status",           StringType(),    False),
    StructField("error_message",    StringType(),    True),
    StructField("duration_seconds", IntegerType(),   True),
    StructField("ingest_timestamp", TimestampType(), False),
])

def log_audit_row(table_name, source_path, target_path, row_count, status, error_message, duration_seconds):
    row = [(BATCH_ID, table_name, source_path, target_path, row_count, status,
            error_message, duration_seconds, datetime.now())]
    audit_df = spark.createDataFrame(row, schema=audit_schema)
    (
        audit_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(AUDIT_LOG_PATH)
    )
    rc = row_count if row_count is not None else "NA"
    print(f"[AUDIT] {table_name:<14} status={status:<7} rows={rc:>12}  duration={duration_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Dimension Tables — `customers`, `products`, `orders`
# MAGIC
# MAGIC High-performance batch read with `spark.read`. Bronze adds only metadata columns:
# MAGIC - `_ingest_timestamp` — when this row was loaded into Bronze
# MAGIC - `_batch_id` — which ingestion run produced this row
# MAGIC - `_source_file` — best-effort source path (useful when source is file-based)

# COMMAND ----------

def ingest_batch(table_name):
    t0 = time.time()
    try:
        src = PATHS_RAW[table_name]
        dst = PATHS_BRONZE[table_name]

        df = (
            spark.read.format("delta").load(src)
            .withColumn("_ingest_timestamp", INGEST_TS)
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source_file", F.input_file_name())
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(dst)
        )

        row_count = df.count()
        duration = int(time.time() - t0)
        log_audit_row(table_name, src, dst, row_count, "SUCCESS", None, duration)
        return df

    except Exception as e:
        duration = int(time.time() - t0)
        log_audit_row(table_name, PATHS_RAW[table_name], PATHS_BRONZE[table_name],
                       None, "FAILED", str(e), duration)
        raise

# COMMAND ----------

customers_bronze = ingest_batch("customers")
products_bronze  = ingest_batch("products")

# COMMAND ----------

# MAGIC %md
# MAGIC `orders` (20M+ rows, partitioned by `order_date`) is read in full each run here.
# MAGIC For incremental loads in production, switch this to the same Auto Loader
# MAGIC streaming pattern used for `order_items` below, keyed on `order_date`.

# COMMAND ----------

orders_bronze = ingest_batch("orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest `order_items` — Auto Loader streaming pattern (largest table, ~45M rows)
# MAGIC
# MAGIC This is the table most worth ingesting incrementally. We use `.readStream` with
# MAGIC `trigger(availableNow=True)`, which:
# MAGIC - processes all currently-available data and then stops (acts like a batch job)
# MAGIC - tracks progress via a checkpoint, so subsequent runs only pick up new partitions
# MAGIC - is the recommended Databricks pattern for "batch-like" incremental ingestion
# MAGIC
# MAGIC **If your raw data lands as files** (CSV/JSON/Parquet) rather than Delta, replace
# MAGIC `.format("delta")` below with the standard Auto Loader block:
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "parquet")          # or csv / json
# MAGIC     .option("cloudFiles.schemaLocation", schema_loc)
# MAGIC     .option("cloudFiles.maxFilesPerTrigger", 1000)    # tune for throughput
# MAGIC     .load(raw_landing_path)
# MAGIC ```

# COMMAND ----------

t0 = time.time()

try:
    checkpoint_path = f"{CHECKPOINT_BASE}/order_items_bronze"

    order_items_stream = (
        spark.readStream
        .format("delta")                       # swap to "cloudFiles" for file-based sources — see markdown above
        .load(PATHS_RAW["order_items"])
        .withColumn("_ingest_timestamp", INGEST_TS)
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    query = (
        order_items_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)            # process everything available, then stop — batch-like semantics
        .start(PATHS_BRONZE["order_items"])
    )

    query.awaitTermination()

    row_count = spark.read.format("delta").load(PATHS_BRONZE["order_items"]).count()
    duration  = int(time.time() - t0)
    log_audit_row("order_items", PATHS_RAW["order_items"], PATHS_BRONZE["order_items"],
                   row_count, "SUCCESS", None, duration)

except Exception as e:
    duration = int(time.time() - t0)
    log_audit_row("order_items", PATHS_RAW["order_items"], PATHS_BRONZE["order_items"],
                   None, "FAILED", str(e), duration)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Review the Audit Log

# COMMAND ----------

audit_df = spark.read.format("delta").load(AUDIT_LOG_PATH)
display(
    audit_df
    .filter(F.col("batch_id") == BATCH_ID)
    .select("table_name", "row_count", "status", "duration_seconds", "ingest_timestamp")
    .orderBy("table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE INGESTION COMPLETE")
print(f"Batch ID: {BATCH_ID}")
print("=" * 60)
for name, path in PATHS_BRONZE.items():
    print(f"  {name:<14} -> {path}")
print()
print("Next: run 03_silver_transformation.py")
