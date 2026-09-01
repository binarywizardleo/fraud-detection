"""
Bronze Layer: Transactions Stream Ingestion
============================================
Ingests raw transaction data from Kafka topic and stores in bronze table.
Data is minimally transformed - only casting key/value and adding metadata.

Dependencies: None (source system: Kafka)
Target Table: fraud_detection.bronze.transactions
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame


@dp.table(
    name='fraud_detection.bronze.transactions',
    comment='Raw transaction stream data ingested from Kafka topic with minimal transformation'
)
def transactions_bronze() -> DataFrame:
    """
    Ingest raw transaction data from Kafka.
    
    Returns:
        DataFrame: Raw Kafka messages with metadata columns
    """
    # Retrieve Kafka credentials from secrets
    bootstrap_server = dbutils.secrets.get(scope='fraud-detection', key='bootstrap_server')
    topic_name = dbutils.secrets.get(scope='fraud-detection', key='topic_name')
    api_key = dbutils.secrets.get(scope='fraud-detection', key='api_key')
    api_secret = dbutils.secrets.get(scope='fraud-detection', key='api_secret')
    
    # Configure JAAS authentication string
    jaas_string = f'''kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'''
    
    # Read streaming data from Kafka
    df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_server)
        .option('subscribe', topic_name)
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', jaas_string)
        .option('startingOffsets', 'latest')
        .option('maxOffsetsPerTrigger', 100)
        .load()
    )
    
    # Cast binary data to strings and add ingestion timestamp
    df = df.select(
        col('key').cast(StringType()).alias('key'),
        col('value').cast(StringType()).alias('value'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp'),
        current_timestamp().alias('ingestion_timestamp')
    )
    
    return df
