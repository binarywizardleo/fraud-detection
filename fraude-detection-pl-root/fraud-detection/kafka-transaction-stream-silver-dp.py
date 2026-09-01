from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType


# Define schema for transaction JSON data
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True)
])


@dp.table(
    name='fraud_detection.silver.transactions_dp',
    comment='loads clean and conformed data from raw transactions'
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")

def transaction_silver() -> DataFrame:
    df = spark.readStream.table('fraud_detection.bronze.transactions_dp')
    
    # Parse the JSON value column using the schema
    parsed_df = df.select(
        col('key').alias('kafka_key'),
        from_json(col('value').cast('string'), transaction_schema).alias('transaction')
    )
    
    # Flatten the nested transaction structure
    return parsed_df.select(
        col('kafka_key'),
        col('transaction.*')
    )
    return parsed_df