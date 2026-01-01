import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *

@dlt.table()

def silver_customer_transformed():
    df = spark.readStream.table("bronze_layer_cleaned_customers")

    df = df.withColumn("age_of_customer",floor(months_between(current_date(), col("dob")) / 12))
    df = df.withColumn("tenure_period",floor(months_between(current_date(), col("join_date")) /12))

    df = df.withColumn("flag",when(col("age_of_customer")<=0,"invalid").otherwise("valid"))
    df = df.withColumn("transformation_date",current_timestamp())
                       
    return df

#-------------scd1 - Apply changes------------#

dlt.create_streaming_table("silver_customer_transformed_scd1")
dlt.apply_changes(
    target = "silver_customer_transformed_scd1",
    source = "silver_customer_transformed",
    keys = ["customer_id"],
    sequence_by = col("transformation_date"),
    stored_as_scd_type = 1,
    except_column_list = ["transformation_date"],

)


@dlt.table()

def silver_accounts_transformed():
    df = spark.readStream.table("bronze_layer_cleaned_accounts")

    df = df.withColumn("channel_type",when(col("txn_channel").isin("ATM", "BRANCH"), "PHYSICAL").otherwise("DIGITAL"))
    df = df.withColumn("txn_year",year(col("txn_date"))).withColumn("txn_month",month(col("txn_date"))).withColumn("txn_day",dayofmonth(col("txn_date")))
    df = df.withColumn("txn_direction",when(col("txn_type").isin("DEBIT"),"OUT").otherwise("IN"))
    df = df.withColumn("acc_transformation_date",current_timestamp())

    return df




#--------------------SCD2 Auto-CDC----------------#

dlt.create_streaming_table("silver_accounts_transformed_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_accounts_transformed_scd2",
    source = "silver_accounts_transformed",
    keys = ['txn_id'],
    sequence_by = col('acc_transformation_date'),
    except_column_list= ['acc_transformation_date'],
    stored_as_scd_type= 2

)