# Databricks notebook source
# DBTITLE 1,Read bronze casualties data
from pyspark.sql.functions import col,when, current_timestamp

df_casualties = spark.read.table("workspace.default.bronze_casualties")

df_silver_casualities = (
    df_casualties
    
.filter(col("collision_index").isNotNull())
.filter(col("casualty_severity").isNotNull())


.withColumn("age_of_casualty",col("age_of_casualty").cast("int"))
.withColumn("collision_year",col("collision_year").cast("int"))

.withColumn(
    "severity_label",
             when(col("casualty_severity") == 1, "Fatal")
            .when(col("casualty_severity") == 2, "Serious")
            .when(col("casualty_severity") == 3, "Slight")
            .otherwise("Unknown")
        )

.withColumn("sex_label",
        when(col("sex_of_casualty")== 1,"Male")
        .when(col("sex_of_casualty")== 2,"Female")
        .otherwise("Unknown")   
            )

.withColumn("is_fatal", col("casualty_severity")==1)

.drop("source_file","ingestion_timestamp")
.withColumn("silver_timestamp", current_timestamp())
)



df_silver_casualities.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 2
(df_silver_casualities
.write
.format("delta")
.mode("overwrite")
.partitionBy("collision_year")
.saveAsTable("workspace.default.silver_casualities")
)

# COMMAND ----------

spark.sql("select * from workspace.default.silver_casualities").limit(5).display()