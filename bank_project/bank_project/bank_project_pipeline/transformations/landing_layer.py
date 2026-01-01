import dlt
from pyspark.sql.functions import *

@dlt.table(
    comment="Landing customers data ingested incrementally using Auto Loader"
)
def landing_customers_incremental():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation","/Volumes/bank_project_catalog/bank_project_schema/bank_project_volume/_schemas/customers")
        .option("header", "true")
        .load("/Volumes/bank_project_catalog/bank_project_schema/bank_project_volume/customers/")
    )
@dlt.table(
    comment="Landing accounts data ingested incrementally using Auto Loader"
)
def landing_accounts_incremental():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/bank_project_catalog/bank_project_schema/bank_project_volume/_schemas/accounts")
        .option("header", "true")
        .load("/Volumes/bank_project_catalog/bank_project_schema/bank_project_volume/accounts/")
    )
