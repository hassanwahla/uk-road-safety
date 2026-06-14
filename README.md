# uk-road-safety
This is a Databricks project, utilizing the UK road safety dataset of over 1 million records.

How to Reproduce:

Sign up for Databricks (free trial)
Download the three CSVs from the DfT website
Upload CSVs to a Databricks Volume at /Volumes/workspace/default/road-safety-data/
Run notebooks in order: 01 → 02 → 03 → 04
All Delta tables will be created in workspace.default

Pipeline Design:
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


Star Schema Design:

                   [gold_dim_date]
                   - date_key (PK)
                   - day, month_name, year
                   - quarter, is_weekend
                           │
[gold_dim_vehicle] ────────┼───────────────  ─[gold_dim_location]
- vehicle_sk (PK)          │                 - location_sk (PK)
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
