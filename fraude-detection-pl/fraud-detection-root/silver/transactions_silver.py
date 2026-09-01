"""
Silver Layer: Transactions
==========================
Cleans and conforms transaction data from bronze layer.
Parses JSON values, validates data quality, and applies business rules.

Dependencies: fraud_detection.bronze.transactions
Target Table: fraud_detection.silver.transactions
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType


# Define schema for transaction JSON data
TRANSACTION_SCHEMA = StructType([
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
    name='fraud_detection.silver.transactions',
    comment='Cleaned and conformed transaction data with data quality expectations enforced'
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_currency", "currency IS NOT NULL")
def transactions_silver() -> DataFrame:
    """
    Parse and clean transaction data from bronze layer.
    
    Quality Rules:
    - DROP if transaction_id is null
    - DROP if customer_id is null
    - DROP if transaction_timestamp is null
    - WARN if amount <= 0
    - WARN if currency is null
    
    Returns:
        DataFrame: Cleaned transaction records
    """
    # Read from bronze layer
    df = spark.readStream.table('fraud_detection.bronze.transactions')
    
    # Parse JSON value column using schema
    parsed_df = df.select(
        col('key').alias('kafka_key'),
        from_json(col('value').cast('string'), TRANSACTION_SCHEMA).alias('transaction'),
        col('ingestion_timestamp')
    )
    
    # Flatten nested transaction structure
    result_df = parsed_df.select(
        col('kafka_key'),
        col('transaction.*'),
        col('ingestion_timestamp')
    )
    
    return result_df
