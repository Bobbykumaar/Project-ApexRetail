# Retail Data Engineering — Medallion Pipeline (Databricks, PySpark, Delta Lake)

A production-grade, multi-gigabyte retail data pipeline built on the **Bronze / Silver / Gold
medallion architecture**, designed to run on Databricks. The pipeline generates ~10GB of
realistic, intentionally messy retail data entirely with distributed Spark transformations
(no Python loops), and then cleans, deduplicates, and aggregates it at scale — including
handling **real data skew**.

---

## Project Layout

```
retail-medallion-pipeline/
│
├── notebooks/
│   ├── 01_big_data_generator.py     # Distributed PySpark data generator (1M+ customers, 20M+ orders, ~45M order_items)
│   ├── 02_bronze_ingestion.py       # Auto Loader / batch ingestion + audit log
│   ├── 03_silver_transformation.py  # Dedup, cleaning, skew-aware joins (broadcast + salting reference)
│   └── 04_gold_aggregates.py        # Business aggregates + Z-Order optimization
│
├── config/
│   └── cluster_config.json          # Recommended cluster sizes, runtime estimates, scale-down guide
│
├── requirements.txt                 # For local/dev testing only
├── .gitignore
└── README.md
```

---

## Scale

| Dataset       | Rows           | Approx Size |
|---------------|----------------|-------------|
| `customers`   | 1,000,000+     | ~250 MB     |
| `products`    | 50,000+        | ~15 MB      |
| `orders`      | 20,000,000+    | ~3-4 GB     |
| `order_items` | ~45,000,000    | ~4-5 GB     |
| **Total**     |                | **~8-10 GB** |

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

### 1. `01_big_data_generator.py` — Distributed Generation
- Pure Spark — `spark.range()`, `explode(sequence(...))`, deterministic hash expressions
- Writes raw Delta tables to `dbfs:/mnt/retail_raw/`, partitioned by `order_date`
- Includes a built-in skew validation step on a 1% sample before the full write

### 2. `02_bronze_ingestion.py` — Bronze Layer
- `order_items` (largest table) ingested via **Auto Loader pattern**:
  `spark.readStream.format("delta")` + `trigger(availableNow=True)` — batch-like
  incremental semantics with checkpoint tracking
- `customers`, `products`, `orders` ingested via high-performance batch read
- Every run writes to a Delta **audit log** (`_audit_log`): row counts, duration, status, errors

### 3. `03_silver_transformation.py` — Silver Layer
- **Deduplication via `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`** —
  deterministic "keep latest" / "keep earliest" rules, not `dropDuplicates()`
- Email regex validation, brand normalization, price/quantity anomaly flags
- **Skew-aware join strategy**:
  - `order_items` (45M) join `products` (50K) on `product_id` (the skewed key) →
    **broadcast join** — eliminates shuffle on the large side entirely
  - `order_items` join `orders` on `order_id` (not skewed) → AQE skew-join handling
  - **Salting pattern documented as a reference** for large-large joins on a skewed key

### 4. `04_gold_aggregates.py` — Gold Layer
- `customer_summary`, `product_summary`, `monthly_revenue`, `category_performance`,
  `data_quality_summary`
- **`OPTIMIZE ... ZORDER BY`** applied to:
  - `fact_sales` (Silver, 45M rows, 1095 date partitions) → `ZORDER BY (customer_id, product_id)`
  - `customer_summary` → `ZORDER BY (customer_id)`
  - `product_summary` → `ZORDER BY (category, product_id)`

---

## Running on Databricks

1. Import the `notebooks/` folder into your Databricks workspace
2. Attach to a cluster — see `config/cluster_config.json` for recommended sizing
3. Run notebooks in order: `01` then `02` then `03` then `04`

### Quick dev/test run
Edit the scale constants at the top of `01_big_data_generator.py`:

```python
N_CUSTOMERS = 10_000      # was 1_000_000
N_PRODUCTS  = 500         # was 50_000
N_ORDERS    = 200_000     # was 20_000_000
```

This produces ~440K `order_items` rows and runs end-to-end in a couple of minutes
on a single-node cluster — useful for validating logic before a full-scale run.

---

## Key Engineering Concepts Demonstrated

- Distributed data generation with `spark.range()` and `explode()` — zero driver-side loops
- Intentional data skew (5% of keys drive 50% of rows) and three strategies to handle it:
  broadcast join, AQE skew join, and salting (documented reference)
- Window-function deduplication at 10s-of-millions-of-rows scale
- Auto Loader / `readStream` + `trigger(availableNow=True)` for incremental batch ingestion
- Operational audit logging as a first-class Delta table
- Partitioning by `order_date` plus `OPTIMIZE ZORDER BY` on high-cardinality keys
- Data quality flagging (not silent row-dropping) surfaced as a Gold-layer table

---

## License

MIT — free to use, adapt, and build on.
