# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import col, when, current_timestamp, to_date

df_vehicles= spark.read.table("workspace.default.bronze_vehicles").select(
    "collision_index",
    "vehicle_reference",
    "collision_year",
    "vehicle_type",
    "skidding_and_overturning",
    "hit_object_in_carriageway",
    "age_of_vehicle",
    "age_of_driver",
    "engine_capacity_cc"
)

df_silver_vehicles =  (

df_vehicles 

.dropDuplicates(["collision_index","vehicle_reference"])

.filter(col("collision_index").isNotNull())

.fillna({"vehicle_type":"Unknown","skidding_and_overturning":"Unknown","hit_object_in_carriageway":"Unknown"})

.withColumn("collision_year", col("collision_year").cast("integer"))

.withColumn("age_of_vehicle", col("age_of_vehicle").cast("integer"))

.withColumn("age_of_driver", col("age_of_driver").cast("integer"))

.withColumn("engine_capacity_cc", col("engine_capacity_cc").cast("integer"))

.withColumn("vehicle_type_int", col("vehicle_type").cast("integer"))
.withColumn("is_large_vehicle", when(col("vehicle_type_int").isin([3,4,5,19,20]), True).otherwise(False))
.drop("vehicle_type_int")

.withColumn("vehicle_age_band", 
             when(col("age_of_vehicle").between(0,3), "0-3 years")
            .when(col("age_of_vehicle").between(4,10), "4-10 years")
            .when(col("age_of_vehicle").between(11,100), "11+ years")
            .otherwise("Unknown"))   

.withColumn("silver_timestamp",current_timestamp())

)

(

  df_silver_vehicles
  .write
  .format("delta")
  .mode("overwrite")
  .partitionBy("collision_year")
  .saveAsTable("workspace.default.silver_vehicles")
)