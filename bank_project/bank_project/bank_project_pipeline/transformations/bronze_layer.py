import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table()

def bronze_layer_cleaned_customers():
    df = dlt.read_stream("landing_customers_incremental")


    df = df.withColumn("name",upper(col("name")))
    df = df.withColumn("email",lower(col("email")))
    df = df.withColumn("city",upper(col("city")))
    df = df.withColumn("income_range",upper(col("income_range")))
    df = df.withColumn("risk_segment",upper(col("risk_segment")))
    df = df.withColumn("preferred_channel",upper(col("preferred_channel")))
    df = df.withColumn("gender",when(col("gender") == "M", "MALE").when(col("gender") == "F", "FEMALE").otherwise("UNKNOWN"))
    df = df.withColumn("status",when(col("status").isNull(),"UNKNOWN").otherwise(col("status")))
    df = df.withColumn("is_valid_phone",col("phone_number").rlike("^[6-9][0-9]{9}$"))
    df = df.filter(col("gender").isin(["MALE","FEMALE"]))
    df = df.filter(col("preferred_channel").isin(["ONLINE","ATM","MOBILE","BRANCH"]))

    return df


@dlt.table()


def bronze_layer_cleaned_accounts():
        df = dlt.read_stream("landing_accounts_incremental")

        df = df.withColumn("txn_channel",upper(col("txn_channel")))
        df = df.withColumn("txn_type",upper(col("txn_type")))
        df = df.withColumn("account_type",upper(col("account_type")))
        df = df.withColumn("txn_type",when(col("txn_type")=="DEBITT","DEBIT").when(col("txn_type")=="CREDIIT","CREDIT").otherwise(col("txn_type")))

        return df