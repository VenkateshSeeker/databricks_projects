import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table()

def gold_cust_acc_trns_m():
    customers = dlt.read("silver_customer_transformed").drop("_rescued_data")
    tx = dlt.read("silver_accounts_transformed").drop("_rescued_data")

    joined = customers.join(
        tx,
        on = "customer_id",
        how = "inner"
    )

    return joined


@dlt.table()

def gold_cust_acc_trans_agg():
    df = dlt.read("gold_cust_acc_trns_m")

    return(
        df.groupBy(
            "customer_id","name","gender","city","status","income_range","risk_segment","age_of_customer","tenure_period"
        )
        .agg(
            countDistinct("account_id").alias("accounts_count"),
            count("*").alias("txn_count"),
            sum(when(col("txn_type")=="CREDIT", col("txn_amount")).otherwise(lit(0.0))).alias("total_credit"),
            sum(when(col("txn_type")=="DEBIT", col("txn_amount")).otherwise(lit(0.0))).alias("total_debit"),
            avg(col("txn_amount")).alias("avg_txn_amount"),
            min("txn_date").alias("first_txn_date"),
            max("txn_date").alias("last_txn_date"),
            countDistinct("txn_channel").alias("channels_used")
        )
    )