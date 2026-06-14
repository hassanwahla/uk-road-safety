# Databricks notebook source
from pyspark.sql.functions import col

# Bronze Layer - Raw Ingest
df_casualties = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("/Volumes/workspace/default/road-safety-data/dft-road-casualty-statistics-casualty-provisional-2025.csv")

)

df_collisions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("/Volumes/workspace/default/road-safety-data/dft-road-casualty-statistics-collision-provisional-2025.csv")

)

df_vehicles = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("/Volumes/workspace/default/road-safety-data/dft-road-casualty-statistics-vehicle-provisional-2025.csv")
)


# COMMAND ----------

( df_casualties
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("collision_year")
    .saveAsTable("workspace.default.bronze_casualties")
)

print("Bronze casualties written successfully")

# COMMAND ----------

( df_collisions
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("collision_year")
    .saveAsTable("workspace.default.bronze_collisions")
)

print("Bronze collisions written successfully")

# COMMAND ----------

( df_vehicles
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("collision_year")
    .saveAsTable("workspace.default.bronze_vehicles")
)

print("Bronze vehicles written successfully")

# COMMAND ----------

spark.sql("select count(*) from workspace.default.bronze_collisions").display()

# COMMAND ----------

spark.sql('describe history workspace.default.bronze_collisions').display()