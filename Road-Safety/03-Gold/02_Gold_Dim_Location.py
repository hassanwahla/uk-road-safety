# Databricks notebook source
from pyspark.sql.functions import col, when, monotonically_increasing_id

df_silver_collisions = spark.read.table("workspace.default.silver_collisions")

df_location =  (
    df_silver_collisions
    
    .select(
        "collision_index",
        "latitude",
        "longitude",
        "local_authority_district",
        "road_type",
        "speed_limit",
        "urban_or_rural_area"
    )
    .withColumn("location_sk", monotonically_increasing_id())
    .filter(
        (col("latitude").isNotNull())&(col("longitude").isNotNull())
        )
)

(
df_location
.write
.mode("overwrite")
.format("delta")
.saveAsTable("workspace.default.gold_dim_location")
)


# COMMAND ----------

spark.sql("select * from workspace.default.gold_dim_location").display()

