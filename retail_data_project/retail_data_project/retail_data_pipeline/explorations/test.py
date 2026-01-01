# Databricks notebook source
df = spark.sql("SELECT * FROM retail_data_catalog.retail_data_schema.data_load_customer")

# COMMAND ----------

df.display()


# COMMAND ----------

print(df.schema["age"].dataType)

# COMMAND ----------

df = spark.sql("SELECT * FROM retail_data_catalog.retail_data_schema.data_load_transactions" )

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn("total_sales",col("quantity")*col("price")).display()

# COMMAND ----------

df.schema.display()

# COMMAND ----------

df = spark.sql("SELECT * FROM retail_data_catalog.retail_data_schema.gold_product_transactions")
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

