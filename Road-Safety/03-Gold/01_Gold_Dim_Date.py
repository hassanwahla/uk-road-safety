# Databricks notebook source
from pyspark.sql.functions import col, month, day, when, dayofmonth, date_format

df_silver_collisions = spark.read.table('workspace.default.silver_collisions')

df_dim_date = (
    df_silver_collisions
    .select(["date","is_weekend"])
    .distinct()
    .withColumn("date_sk",date_format("date","yyyyMMdd").cast("integer"))
    .withColumn("day",date_format(col("date"),"dd"))
    .withColumn("month_name",date_format(col("date"),"MMM"))
    .withColumn("year",date_format(col("date"),"yyyy"))
    .withColumn("month_number",month("date"))
    )

df_dim_date = (

        df_dim_date
        .withColumn("quarter",
         when( col("month_number").isin(1,2,3),"Q1")
        .when(col("month_number").isin(4,5,6),"Q2")
        .when(col("month_number").isin(7,8,9),"Q3")
        .when(col("month_number").isin(10,11,12),"Q4")             
)
)

(
df_dim_date
.write
.format("delta")
.mode("overwrite")
.partitionBy("year","month_number")
.saveAsTable("workspace.default.gold_dim_date")
)

