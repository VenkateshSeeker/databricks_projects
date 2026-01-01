import dlt
from pyspark.sql.functions import *

@dlt.table()
def gold_product_transactions():
    products = dlt.read_stream("silver_retail_products").drop("_rescued_data")

    transactions = dlt.read_stream("silver_retail_transactions").drop("_rescued_data")

    product_transactions = transactions.join(products, on = "product_id", how = "inner")
    return product_transactions

@dlt.table()
def gold_customer_transactions():
    customers = dlt.read_stream("silver_retail_customers").drop("_rescued_data")
    transactions = dlt.read_stream("silver_retail_transactions").drop("_rescued_data")

    customer_transactions = transactions.join(customers, on = "customer_id", how = "inner")
    return customer_transactions




