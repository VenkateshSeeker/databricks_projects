import dlt
from pyspark.sql.functions import col

@dlt.table()
def data_load_customer():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation","/Volumes/retail_data_catalog/retail_data_schema/retail_data/_schemas/customers/_schemas/0")
        .load("/Volumes/retail_data_catalog/retail_data_schema/retail_data/customers")
    )


@dlt.table()
def data_load_inventory():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation","/Volumes/retail_data_catalog/retail_data_schema/retail_data/_schemas/inventory/_schemas/0")
        .load("/Volumes/retail_data_catalog/retail_data_schema/retail_data/inventory")
    )

@dlt.table()
def data_load_products():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation","/Volumes/retail_data_catalog/retail_data_schema/retail_data/_schemas/products/_schemas/0")
        .load("/Volumes/retail_data_catalog/retail_data_schema/retail_data/products")
    )


@dlt.table()
def data_load_transactions():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation","/Volumes/retail_data_catalog/retail_data_schema/retail_data/_schemas/transactions/_schemas/0")
        .load("/Volumes/retail_data_catalog/retail_data_schema/retail_data/transactions")
    )