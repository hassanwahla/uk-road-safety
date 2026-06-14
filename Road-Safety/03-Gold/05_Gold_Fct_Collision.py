# Databricks notebook source
from pyspark.sql.functions import col, when

df_silver_collisions = spark.read.table("workspace.default.silver_collisions")
df_dim_date = spark.read.table("workspace.default.gold_dim_date")
df_dim_location = spark.read.table("workspace.default.gold_dim_location")
df_dim_vehicle = spark.read.table("workspace.default.gold_dim_vehicle")
df_dim_casualty = spark.read.table("workspace.default.gold_dim_casualties")

st = df_silver_collisions.alias("st")
dd = df_dim_date.alias("dd")
dl = df_dim_location.alias("dl")
dv = df_dim_vehicle.alias("dv")
dc = df_dim_casualty.alias("dc")

df_fct_collisions = (

    st
    .join(dd, "date" , how = "left")
    .join(dl, "collision_index", how = "left")
    .join(dv, "collision_index" , how = "left")
    .join(dc, "collision_index" , how = "left")
    .select(
        col("st.collision_index"),
        col("dd.date_sk"),
        col("dl.location_sk"),
        col("dv.vehicle_sk"),
        col("dc.casualty_sk"),
        col("st.collision_severity"),
        col("st.number_of_casualties"),
        col("st.number_of_vehicles"),
        col("st.speed_limit"),
        col("dd.year")
    )
)

(
    df_fct_collisions
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema","true")
    .partitionBy("year")
    .saveAsTable("workspace.default.gold_fct_collisions")
)

# COMMAND ----------

#Verifying if all stars are populated

spark.sql("""
    SELECT 'gold_fct_collisions' as table_name, COUNT(*) as row_count FROM workspace.default.gold_fct_collisions
    UNION ALL
    SELECT 'gold_dim_date', COUNT(*) FROM workspace.default.gold_dim_date
    UNION ALL
    SELECT 'gold_dim_location', COUNT(*) FROM workspace.default.gold_dim_location
    UNION ALL
    SELECT 'gold_dim_vehicle', COUNT(*) FROM workspace.default.gold_dim_vehicle
    UNION ALL
    SELECT 'gold_dim_casualties', COUNT(*) FROM workspace.default.gold_dim_casualties
""").display()