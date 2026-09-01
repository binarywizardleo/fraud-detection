from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, trim, upper


@dp.table(
    name = 'fraud_detection.silver.fraud_watchlist_dp',
    comment = 'This table contains cleaned and conformed fraud watchlist information'
)
def fraud_watchlist_silver_flow() -> DataFrame:
    df = spark.readStream.table('fraud_detection.bronze.fraud_watchlist_dp')

    df = df.select(
        # Convert effective_from from string to timestamp
        to_timestamp(col('effective_from')).alias('effective_from'),
        # Clean string fields
        trim(col('watchlist_id')).alias('watchlist_id'),
        trim(col('entity_id')).alias('entity_id'),
        trim(col('watch_type')).alias('watch_type'),
        trim(upper(col('action'))).alias('action'),
        trim(col('status')).alias('status'),
        trim(col('risk_level')).alias('risk_level'),
        trim(col('reason_code')).alias('reason_code'),
        trim(col('reason_description')).alias('reason_description'),
        trim(col('reported_by')).alias('reported_by'),
        trim(col('reported_source')).alias('reported_source'),
        trim(col('city')).alias('city'),
        trim(upper(col('country'))).alias('country'),
        # Keep metadata columns
        col('file_path'),
        col('file_modification_time'),
        col('ingest_time'),
        # Add processing timestamp
        current_timestamp().alias('processed_time')
    )
    
    return df
