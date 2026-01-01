import dlt
from pyspark.sql.functions import *

@dlt.table()

@dlt.expect_or_drop("customer_id_not_null","customer_id IS NOT NULL")
@dlt.expect_or_drop("city_is_not_null","city IS NOT NULL")

def silver_retail_customers():
    df = dlt.read_stream("bronze_retail_customers")

    df = df.withColumn("age_group_code",
                       when(col("age") < 18 , "Teen")
                       .when((col("age") >= 18) & (col("age") <= 35),"Adult")
                       .when((col("age") >= 35) & (col("age") <= 60),"Senior")
                       .when(col("age") > 60,"Elder")
                       .otherwise("Old")
                       )

    return df

dlt.create_streaming_table("silver_retail_customers_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_retail_customers_scd2",
    source = "silver_retail_customers",
    keys = ["customer_id"],
    sequence_by = col("updated_time_stamp_customers"),
    stored_as_scd_type = 2
)


@dlt.table()

@dlt.expect_or_drop("product_id_is_not_null", "product_id IS NOT NULL")
@dlt.expect_or_drop("stock_is_not_null","stock IS NOT NULL")
@dlt.expect_or_drop("warehouse_is_not_null","warehouse IS NOT NULL")


def silver_retail_inventory():
    df = dlt.read_stream("bronze_retail_inventory")

    df = df.withColumn("stock_status",
                       when(col("stock")<=100,"Low")
                       .otherwise("Good")
                       )

    return df

dlt.create_streaming_table("silver_retail_inventory_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_retail_inventory_scd2",
    source = "silver_retail_inventory",
    keys = ["product_id"],
    sequence_by = col("updated_time_stamp_inventory"),
    stored_as_scd_type = 2
)

@dlt.table()

@dlt.expect_or_drop("product_id_is_not_null","product_id IS NOT NULL")
@dlt.expect_or_drop("product_name_is_not_null","product_name IS NOT NULL")
@dlt.expect_or_drop("category_is_not_null","category IS NOT NULL")
@dlt.expect_or_drop("brand_is_not_null","brand IS NOT NULL")
@dlt.expect_or_drop("price_is_not_null","price IS NOT NULL")

def silver_retail_products():
    df = dlt.read_stream("bronze_retail_products")

    df = df.withColumn("price_tier",
                       when(col("price") < 5000,"Budget")
                       .when((col("price") >= 5000) & (col("price") <= 50000),"Expensive")
                       .otherwise("Luxury")
                       )

    return df

dlt.create_streaming_table("silver_retail_products_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_retail_products_scd2",
    source = "silver_retail_products",
    keys = ["product_id"],
    sequence_by = col("updated_time_stamp_products"),
    stored_as_scd_type = 2
)

@dlt.table()

@dlt.expect_or_drop("transaction_id_is_not_null","transaction_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_is_not_null","customer_id IS NOT NULL")
@dlt.expect_or_drop("product_id_is_not_null","product_id IS NOT NULL")
@dlt.expect_or_drop("quantity_is_not_null","quantity IS NOT NULL")
@dlt.expect_or_drop("transaction_date_is_not_null","transaction_date IS NOT NULL")
@dlt.expect_or_drop("price_is_not_null","product_price IS NOT NULL")
@dlt.expect_or_drop("sales_amount_is_not_null","sales_amount IS NOT NULL")

def silver_retail_transactions():
    df = dlt.read_stream("bronze_retail_transactions")

    df = df.withColumnRenamed("price","product_price")

    df = df.withColumn("transaction_time",
                       split(col("transaction_date")," ").getItem(1)
                       )
    df = df.withColumn("day_of_week",
                       to_date(dayofweek(col("transaction_date")))
                       )
    df = df.withColumn("is_weekday",
                       when(col("day_of_week").isin("Saturday", "Sunday"),"Weekend")
                       .otherwise("Weekday")
                       )

    return df

dlt.create_streaming_table("silver_retail_transactions_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_retail_transactions_scd2",
    source = "silver_retail_transactions",
    keys = ["transaction_id"],
    sequence_by = col("updated_time_stamp_transactions"),
    stored_as_scd_type = 2
)
