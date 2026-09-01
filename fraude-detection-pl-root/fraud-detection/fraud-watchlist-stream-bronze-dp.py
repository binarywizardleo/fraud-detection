from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name = 'fraud_detection.bronze.fraud_watchlist_dp',
    comment = 'This table contains fraud watchlist information'
)
def fraud_watchlist_bronze_flow() -> DataFrame:
    df = spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format', 'json')\
            .option('cloudFiles.inferColumnTypes', 'true')\
            .option('cloudFiles.schemaEvolutionMode', 'rescue')\
            .option('cloudFiles.schemaLocation', '/Volumes/fraud_detection/source/schema/fraud_watchlist/')\
            .load('/Volumes/fraud_detection/source/fraud_watchlist/fraud/')

    df = df.select('*',
            col('_metadata.file_path').alias('file_path'),
            col('_metadata.file_modification_time').alias('file_modification_time'),
            current_timestamp().alias('ingest_time'))
    return df
