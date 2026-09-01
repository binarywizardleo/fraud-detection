from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType


@dp.table(
    name = 'fraud_detection.bronze.transactions_dp'
    ,comment = 'stores transaction raw stream ingested data'
)
def transaction_bronze() -> DataFrame:
    bootstrap_server = dbutils.secrets.get(scope='fraud-detection', key='bootstrap_server')
    topic_name = dbutils.secrets.get(scope='fraud-detection', key='topic_name')
    api_key = dbutils.secrets.get(scope='fraud-detection', key='api_key')
    api_secret = dbutils.secrets.get(scope='fraud-detection', key='api_secret')

    jaas_string = f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";"""

    df = spark.readStream.format('kafka')\
                .option("kafka.bootstrap.servers", bootstrap_server)\
                .option("subscribe", topic_name)\
                .option("kafka.security.protocol", "SASL_SSL")\
                .option("kafka.sasl.mechanism", "PLAIN")\
                .option("kafka.sasl.jaas.config", jaas_string)\
                .option("startingOffsets", "latest")\
                .option('maxOffsetsPerTrigger', 100)\
                .load()
    df = df.select(col('key').cast(StringType()).alias('key')\
                , col('value').cast(StringType()).alias('value')
                , col('topic'), col('partition'), col('offset')
                , col('timestamp'), lit(current_timestamp()).alias('ingestion_time'))
    return df
