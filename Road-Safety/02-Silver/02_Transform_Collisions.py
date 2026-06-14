# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, when, to_date

df_collisions = spark.read.table("workspace.default.bronze_collisions")

df_silver_collisions = (

df_collisions

.dropDuplicates(["collision_index"])

.filter(col("collision_index").isNotNull())

.fillna({"road_surface_conditions":"Unknown","weather_conditions":"Unknown","light_conditions":"Unknown"})

.withColumn("collision_year", col("collision_year").cast("integer"))

.withColumn("number_of_casualties", col("number_of_casualties").cast("integer"))

.withColumn("number_of_vehicles", col("number_of_vehicles").cast("integer"))

.withColumn("longitude", col("longitude").cast("double"))

.withColumn("latitude", col("latitude").cast("double"))

.withColumn("date", to_date(col("date"), "dd/MM/yyyy"))

.withColumn("is_weekend",
          when(col("day_of_week").isin(1,7),True)
          .otherwise("False")
)

.withColumn("severity_label",
             when(col("collision_severity")==1, "Fatal")
            .when(col("collision_severity")==2, "Serious")
            .when(col("collision_severity")==3, "Slight")
            .otherwise("Unknown")
            )

.drop("source_file","ingestion_timestamp")
.withColumn("silver_timestamp", current_timestamp())

)

spark.sql("drop table if exists workspace.default.silver_collisions")

print("Table dropped")

(

    df_silver_collisions
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("collision_year")
    .saveAsTable("workspace.default.silver_collisions")

)


# COMMAND ----------

spark.sql("select date,* from workspace.default.silver_collisions").limit(5).display()


# COMMAND ----------

df_silver_collisions.select("date").printSchema()