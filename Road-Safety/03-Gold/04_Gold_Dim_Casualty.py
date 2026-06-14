# Databricks notebook source
from pyspark.sql.functions import col, when, monotonically_increasing_id

df_silver_casualties = spark.read.table("workspace.default.silver_casualities")

df_dim_casualties = (

    df_silver_casualties
    .select (
         "casualty_reference"
        ,"collision_index"
        ,"casualty_class"
        ,"sex_label"
        ,"age_of_casualty"
        ,"severity_label"
        ,"is_fatal"
        ,"casualty_type"
    )
    .withColumn("casualty_sk", monotonically_increasing_id())
    .withColumn("casualty_age_band",
         when(col("age_of_casualty").between(0,15), "0-15")
        .when(col("age_of_casualty").between(16,25), "16-25")
        .when(col("age_of_casualty").between(26,45), "26-45")
        .when(col("age_of_casualty").between(46,65), "46-65")
        .when(col("age_of_casualty")>=65 , "65+")
        .otherwise("unknown")    
    )
    .drop("age_of_casualty")
    .filter(col("casualty_reference").isNotNull())
)

(
    df_dim_casualties
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_dim_casualties")
)
