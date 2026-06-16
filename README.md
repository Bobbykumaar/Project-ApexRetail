# ApexRetail — Retail Data Engineering Pipeline
## Databricks · PySpark · Delta Lake · Medallion Architecture

A production-grade, multi-gigabyte retail data pipeline built on the **Bronze / Silver / Gold
medallion architecture**, designed to run end-to-end on Databricks. The pipeline generates
~10 GB of realistic, intentionally messy retail data entirely with distributed Spark
transformations (no Python loops), then cleans, deduplicates, and aggregates it at scale —
including handling **real data skew**.

---

## Project Layout

```
Project-ApexRetail/
│
├── notebooks/
│   ├── 01_big_data_generator      # Distributed PySpark data generator (1M+ customers, 20M+ orders, ~45M order_items)
│   ├── 02_bronze_ingestion        # Structured Streaming + batch ingestion with audit log
│   ├── 03_silver_transformation   # Dedup, cleaning, skew-aware joins
│   └── 04_gold_aggregates         # Business aggregates + Z-Order optimization
│
├── config/
│   └── cluster_config.json        # Recommended cluster sizes, runtime estimates, scale-down guide
│
├── requirements.txt               # For local/dev testing only
├── .gitignore
└── README.md
```

---

## Storage Layout (Unity Catalog Volume)

All data lives under a Unity Catalog volume:

```
/Volumes/workspace/default/retail_pipeline/
│
├── raw/                        # Written by 01_big_data_generator (partition overwrite by order_date)
│   ├── customers/
│   ├── products/
│   ├── orders/
│   └── order_items/            # Largest table — ~45M rows, streamed into Bronze
│
├── bronze/                     # Written by 02_bronze_ingestion
│   ├── customers/
│   ├── products/
│   ├── orders/
│   └── order_items/
│
├── silver/                     # Written by 03_silver_transformation
│   └── fact_sales/
│
├── gold/                       # Written by 04_gold_aggregates
│   ├── customer_summary/
│   ├── product_summary/
│   ├── monthly_revenue/
│   ├── category_performance/
│   └── data_quality_summary/
│
└── checkpoints/                # Spark Structured Streaming checkpoints
    └── order_items_bronze/     # ⚠️ Must be cleared when re-running the full pipeline from scratch
```

---

## Scale

| Dataset       | Rows           | Approx Size  |
|---------------|----------------|--------------|
| `customers`   | 1,000,000+     | ~250 MB      |
| `products`    | 50,000+        | ~15 MB       |
| `orders`      | 20,000,000+    | ~3–4 GB      |
| `order_items` | ~45,000,000    | ~4–5 GB      |
| **Total**     |                | **~8–10 GB** |

All datasets are generated using `spark.range()`, `explode()`, and deterministic
hash-based expressions — every row is computed independently and in parallel across
executors. There is no driver-side `for` loop anywhere in the generator.

---

## Engineered Data Quality & Skew Issues

These are **intentional** — the Silver notebook is written specifically to detect and handle them.

| Issue | Where | Rate |
|---|---|---|
| Duplicate `customer_id` (different `signup_date`) | customers | ~3% |
| Malformed / null emails | customers | ~6% |
| Null phone | customers | ~8% |
| Null city | customers | ~3% |
| Duplicate `order_id` with later `order_timestamp` | orders | ~2% |
| Null `delivery_date` | orders | ~3% |
| Negative / zero `price` | products | ~2% |
| Negative `quantity` | order_items | ~1% |
| Null `unit_price` | order_items | ~2% |
| `line_total` inconsistent with `unit_price * quantity` | order_items | ~5% |
| **Data skew**: 5% of `product_id` values drive ~50% of `order_items` rows | order_items | by design |

---

## Pipeline Stages

### 1. `01_big_data_generator` — Distributed Generation
- Pure Spark — `spark.range()`, `explode(sequence(...))`, deterministic hash expressions
- Writes raw Delta tables to `/Volumes/workspace/default/retail_pipeline/raw/`, partitioned by `order_date`
- Uses **partition overwrite** mode (`mode=Overwrite, partitionBy=order_date`) — intentional full regeneration
- Includes a built-in skew validation step on a 1% sample before the full write

> ⚠️ **Note:** Because this notebook uses partition overwrite (not append), re-running it
> replaces all raw data. This requires clearing the streaming checkpoint before the next
> pipeline run — see [Known Issue: Checkpoint Reset](#known-issue--checkpoint-reset-required) below.

### 2. `02_bronze_ingestion` — Bronze Layer
- `order_items` (largest table) ingested via **Spark Structured Streaming**:
  `spark.readStream.format("delta")` + `trigger(availableNow=True)` — batch-like
  incremental semantics with checkpoint tracking
- `customers`, `products`, `orders` ingested via high-performance batch read
- Every run writes to a Delta **audit log** (`_audit_log`): row counts, duration, status, errors

### 3. `03_silver_transformation` — Silver Layer
- **Deduplication via `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`** —
  deterministic "keep latest" / "keep earliest" rules, not `dropDuplicates()`
- Email regex validation, brand normalization, price/quantity anomaly flags
- **Skew-aware join strategy:**
  - `order_items` (45M) join `products` (50K) on `product_id` (the skewed key) →
    **broadcast join** — eliminates shuffle on the large side entirely
  - `order_items` join `orders` on `order_id` (not skewed) → AQE skew-join handling
  - **Salting pattern documented as a reference** for large–large joins on a skewed key

### 4. `04_gold_aggregates` — Gold Layer
- `customer_summary`, `product_summary`, `monthly_revenue`, `category_performance`,
  `data_quality_summary`
- **`OPTIMIZE ... ZORDER BY`** applied to:
  - `fact_sales` (Silver, 45M rows, 1095 date partitions) → `ZORDER BY (customer_id, product_id)`
  - `customer_summary` → `ZORDER BY (customer_id)`
  - `product_summary` → `ZORDER BY (category, product_id)`

---

## Job Orchestration

The pipeline is orchestrated as a **Databricks Lakeflow Job** with 4 sequential tasks,
each running the corresponding notebook. Every task runs only if the previous one succeeds.

```
01_Data_Generation  →  02_Bronze_Ingestion  →  03_Silver_Transformation  →  04_Gold_Aggregates
```

The job currently runs on **Serverless compute** — no cluster management required.

To trigger a run: go to **Jobs** in your Databricks workspace and click **Run now**.

> Before triggering a full re-run (after new data has been generated), clear the streaming
> checkpoint first — see [Known Issue](#known-issue--checkpoint-reset-required) below.

---

## Known Issue — Checkpoint Reset Required

**Error:** `DELTA_SOURCE_TABLE_IGNORE_CHANGES`

**Cause:** `01_big_data_generator` writes raw data using partition overwrite mode. Spark
Structured Streaming treats this as an unsupported data mutation on the source Delta table
and terminates the streaming query with:

```
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update
(WRITE mode=Overwrite, partitionBy=[order_date]) in the source table.
```

**Fix — clear the checkpoint before every full pipeline re-run:**

```python
dbutils.fs.rm(
    "/Volumes/workspace/default/retail_pipeline/checkpoints/order_items_bronze",
    recurse=True
)
```

Run this once before triggering a fresh end-to-end run. The streaming query will reprocess
all raw data from scratch.

**Alternative (incremental-only mode):** Add `.option("skipChangeCommits", "true")` to the
stream read in `02_bronze_ingestion` to skip overwrite commits and continue from the last
checkpoint — note this will not reflect regenerated/changed rows.

---

## Running on Databricks

### Option A — Full Job Run (recommended)

1. If re-running from scratch, clear the streaming checkpoint first (see above)
2. Go to **Jobs** → find the ApexRetail job → click **Run now**
3. All 4 tasks execute in sequence automatically

### Option B — Manual Notebook-by-Notebook

1. Clear the checkpoint if re-running from scratch
2. Open and run notebooks in order: `01` → `02` → `03` → `04`
3. Attach to Serverless compute or a cluster (see `config/cluster_config.json`)

### Quick Dev / Test Run

Edit the scale constants at the top of `01_big_data_generator`:

```python
N_CUSTOMERS = 10_000      # was 1_000_000
N_PRODUCTS  = 500         # was 50_000
N_ORDERS    = 200_000     # was 20_000_000
```

This produces ~440K `order_items` rows and runs end-to-end in a couple of minutes
on a single-node cluster — useful for validating logic before a full-scale run.

---

## Compute

The pipeline runs on **Databricks Serverless compute** by default — no cluster setup required.
For large-scale runs or fixed-cost environments, `config/cluster_config.json` documents
recommended Job Cluster sizing (worker count, node type, auto-termination settings).

---

## Key Engineering Concepts Demonstrated

- Distributed data generation with `spark.range()` and `explode()` — zero driver-side loops
- Intentional data skew (5% of keys drive 50% of rows) and three strategies to handle it:
  broadcast join, AQE skew join, and salting (documented reference)
- Window-function deduplication at tens-of-millions-of-rows scale
- Spark Structured Streaming + `trigger(availableNow=True)` for incremental batch ingestion
- Streaming checkpoint management — why checkpoints must be cleared when source data is
  fully regenerated via partition overwrite
- Operational audit logging as a first-class Delta table
- Partitioning by `order_date` plus `OPTIMIZE ZORDER BY` on high-cardinality keys
- Data quality flagging (not silent row-dropping) surfaced as a dedicated Gold-layer table
- Unity Catalog volumes as the primary storage layer for all pipeline data

---

## License

MIT — free to use, adapt, and build on.
