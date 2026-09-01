from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date, current_timestamp

dp.create_streaming_table(
    name = 'customers'
    , comment = 'loads cleaned and conformed customer information'
    , expect_all = {
        "valid_customer": "customer_id is not null",
        "valid_age": "age is not null"
    }
)

@dp.append_flow(target = 'customers', name = "customer_silver_flow")
def customer_silver_flow() -> DataFrame:
    df = spark.readStream.table("fraud_detection.bronze.customers")
    df = df.withColumn("account_open_date", to_date(col('account_open_date'), 'yyyy-MM-dd'))\
            .withColumn("silver_load_timestamp", current_timestamp())
    return df