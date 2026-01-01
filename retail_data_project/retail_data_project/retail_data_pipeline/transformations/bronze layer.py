import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table()
def bronze_retail_customers():
    df = dlt.read_stream("data_load_customer")

    df = df.withColumn("updated_time_stamp_customers", current_timestamp())

    return df

@dlt.table()
def bronze_retail_inventory():
    df = dlt.read_stream("data_load_inventory")

    df = df.withColumn("updated_time_stamp_inventory", current_timestamp())

    return df

@dlt.table()
def bronze_retail_products():
    df = dlt.read_stream("data_load_products")

    df = df.withColumn("updated_time_stamp_products", current_timestamp())

    return df

@dlt.table()
def bronze_retail_transactions():
    df = dlt.read_stream("data_load_transactions")

    

    df = df.withColumn("updated_time_stamp_transactions", current_timestamp())

    return df