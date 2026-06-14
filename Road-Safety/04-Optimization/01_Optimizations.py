# Databricks notebook source
spark.sql("""OPTIMIZE workspace.default.gold_fct_collisions ZORDER BY (collision_severity,speed_limit)
""")

# COMMAND ----------

spark.sql("OPTIMIZE workspace.default.silver_collisions")
spark.sql("OPTIMIZE workspace.default.silver_vehicles")
spark.sql("OPTIMIZE workspace.default.silver_casualities")

# COMMAND ----------

spark.sql("DESCRIBE HISTORY workspace.default.gold_fct_collisions").display()

# COMMAND ----------

spark.sql("DESCRIBE DETAIL workspace.default.gold_fct_collisions").display()