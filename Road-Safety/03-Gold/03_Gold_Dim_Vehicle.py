# Databricks notebook source
from pyspark.sql.functions import col, when, monotonically_increasing_id

df_silver_vehicles = spark.read.table("workspace.default.silver_vehicles")

df_dim_vehicles = (

    df_silver_vehicles

    .select (
    "vehicle_reference",
    "collision_index",
    "vehicle_type",
    "is_large_vehicle",
    "vehicle_age_band",
    "age_of_driver",
    "engine_capacity_cc"
    )
    .distinct()
    .withColumn("vehicle_sk", monotonically_increasing_id())
    .withColumn( "driver_age_band",
                 when(col("age_of_driver").between(16,25), '16-25')
                 .when(col("age_of_driver").between(26,45), '26-45')
                 .when(col("age_of_driver").between(46,65), '46-65')
                 .when(col("age_of_driver")>=65, '65+')
                 .otherwise("Unknown")
    )
    .drop("age_of_driver")
    .filter(col("vehicle_reference").isNotNull())             
)
 
(
    df_dim_vehicles
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_dim_vehicle")
)    