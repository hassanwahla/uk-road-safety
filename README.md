# UK Road Safety Lakehouse

A production-style data engineering project built on **Databricks** and **Delta Lake**, implementing a full **Medallion Architecture** (Bronze → Silver → Gold) with a **Star Schema** analytical layer on the UK Department for Transport road safety dataset (2025).

---

## Architecture

```
Raw CSVs (DfT)
     │
     ▼
┌─────────────────────────────────────────┐
│              BRONZE LAYER               │
│  Raw ingestion · Delta format           │
│  Audit columns · Partitioned by year    │
│  bronze_collisions · bronze_vehicles    │
│  bronze_casualties                      │
└────────────────┬────────────────────────┘
                 │ Delta Live Tables style transforms
                 ▼
┌─────────────────────────────────────────┐
│              SILVER LAYER               │
│  Deduplication · Null handling          │
│  Type casting · Derived columns         │
│  silver_collisions · silver_vehicles    │
│  silver_casualties                      │
└────────────────┬────────────────────────┘
                 │ PySpark joins + aggregations
                 ▼
┌─────────────────────────────────────────┐
│               GOLD LAYER                │
│         Star Schema Model               │
│                                         │
│  gold_fact_collision (114,852 rows)     │
│         ├── gold_dim_date               │
│         ├── gold_dim_location           │
│         ├── gold_dim_vehicle            │
│         └── gold_dim_casualty           │
└─────────────────────────────────────────┘
```

---

## Dataset

**Source:** [UK Road Safety Data — Department for Transport](https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data)

| File | Description | Rows |
|---|---|---|
| `dft-road-casualty-statistics-collision-provisional-2025.csv` | Collision events with location, severity, conditions | ~114k |
| `dft-road-casualty-statistics-vehicles-provisional-2025.csv` | Vehicles involved in each collision | ~87k |
| `dft-road-casualty-statistics-casualty-provisional-2025.csv` | Casualties per collision | ~60k |

---

## Notebooks

| Notebook | Purpose | Key Techniques |
|---|---|---|
| `01_bronze_ingest` | Raw CSV ingestion into Delta tables | `input_file_name()`, `current_timestamp()`, partition by year |
| `02_silver_transform` | Cleansing, enrichment, type casting | `dropDuplicates()`, `fillna()`, `to_date()`, `when/otherwise` |
| `03_gold_layer` | Star schema dimensional model | Multi-table left joins, surrogate keys, `monotonically_increasing_id()` |
| `04_optimisation` | Table performance tuning | Z-Order indexing, file compaction, VACUUM, DESCRIBE HISTORY |

---

## Star Schema

```
                   [gold_dim_date]
                   - date_key (PK)
                   - day, month_name, year
                   - quarter, is_weekend
                          │
[gold_dim_vehicle]────────┼────────[gold_dim_location]
- vehicle_sk (PK)         │        - location_sk (PK)
- vehicle_type      [gold_fact_collision]   - latitude, longitude
- is_large_vehicle  - collision_index       - local_authority
- vehicle_age_band  - date_key (FK)         - road_type
- driver_age_band   - location_sk (FK)      - speed_limit
                    - vehicle_sk (FK)       - urban_or_rural_area
                    - casualty_sk (FK)
                    - collision_severity    [gold_dim_casualty]
                    - number_of_casualties  - casualty_sk (PK)
                    - number_of_vehicles    - severity_label
                    - speed_limit           - sex_label
                                            - casualty_age_band
                                            - is_fatal
```

---

## Key Design Decisions

**Why partition by `collision_year`?**
Queries filtering on a specific year skip all other partitions entirely (partition pruning). Since analysts typically query recent years, this avoids full table scans and reduces compute cost significantly.

**Why left joins in the fact table?**
A collision record should never be dropped just because a vehicle or casualty record is missing due to data quality issues. Left joins preserve all collision events and surface nulls in the FK columns, which can be investigated separately.

**Why Z-Order on `collision_severity` and `speed_limit`?**
These are the most common filter columns for analytical queries — e.g. "fatal collisions on 60mph roads". Z-Order co-locates rows with similar values in the same Delta files, so Spark skips irrelevant files entirely (data skipping). This is meaningfully faster than a full scan on a 100k+ row table.

**Why separate Bronze and Silver layers?**
Bronze is append-only with zero transforms — it preserves the raw source data exactly as received. Silver applies business logic on top. This means if a transformation rule changes, you can re-run Silver from Bronze without re-ingesting from the source system.

**Why surrogate keys in the Gold layer?**
Natural keys like `collision_index` are source-system keys that could theoretically change or be reused. Surrogate keys (`monotonically_increasing_id()`) are stable, integer-based, and join-efficient — following Kimball dimensional modelling best practices.

---

## Tech Stack

| Tool | Usage |
|---|---|
| Databricks | Compute, notebook environment, Delta Lake |
| Apache Spark (PySpark) | Distributed data processing |
| Delta Lake | ACID transactions, time travel, transaction log |
| SQL | Table verification, OPTIMIZE, VACUUM, DESCRIBE HISTORY |
| GitHub | Version control |

---

## How to Reproduce

1. Sign up for [Databricks](https://www.databricks.com/) free trial
2. Download the three CSVs from the [DfT website](https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data)
3. Upload CSVs to a Databricks Volume at `/Volumes/workspace/default/road-safety-data/`
4. Run notebooks in order: `01` → `02` → `03` → `04`
5. All Delta tables will be created in `workspace.default`
