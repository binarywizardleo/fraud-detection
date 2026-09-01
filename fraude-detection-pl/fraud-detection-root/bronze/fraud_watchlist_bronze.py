"""
Bronze Layer: Fraud Watchlist Ingestion
========================================
Ingests fraud watchlist data from cloud files using Auto Loader.
Stores raw watchlist entries with file metadata for tracking.

Dependencies: None (source system: Cloud Files)
Target Table: fraud_detection.bronze.fraud_watchlist
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import DataFrame


@dp.table(
    name='fraud_detection.bronze.fraud_watchlist',
    comment='Raw fraud watchlist data ingested from cloud files using Auto Loader with schema evolution'
)
def fraud_watchlist_bronze() -> DataFrame:
    """
    Ingest fraud watchlist data using Auto Loader.
    
    Returns:
        DataFrame: Raw watchlist data with file metadata
    """
    df = (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'json')
        .option('cloudFiles.inferColumnTypes', 'true')
        .option('cloudFiles.schemaEvolutionMode', 'rescue')
        .option('cloudFiles.schemaLocation', '/Volumes/fraud_detection/source/schema/fraud_watchlist/')
        .load('/Volumes/fraud_detection/source/fraud_watchlist/fraud/')
    )
    
    # Add file metadata and ingestion timestamp
    df = df.select(
        '*',
        col('_metadata.file_path').alias('source_file_path'),
        col('_metadata.file_modification_time').alias('source_file_modified_time'),
        current_timestamp().alias('ingestion_timestamp')
    )
    
    return df
