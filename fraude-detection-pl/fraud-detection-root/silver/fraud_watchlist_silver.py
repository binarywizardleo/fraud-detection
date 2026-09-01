"""
Silver Layer: Fraud Watchlist
==============================
Cleans and conforms fraud watchlist data from bronze layer.
Applies data cleaning, normalization, and quality checks.

Dependencies: fraud_detection.bronze.fraud_watchlist
Target Table: fraud_detection.silver.fraud_watchlist
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, trim, upper


@dp.table(
    name='fraud_detection.silver.fraud_watchlist',
    comment='Cleaned and conformed fraud watchlist with standardized data formats'
)
@dp.expect_or_drop("valid_watchlist_id", "watchlist_id IS NOT NULL")
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_effective_from", "effective_from IS NOT NULL")
def fraud_watchlist_silver() -> DataFrame:
    """
    Clean and conform fraud watchlist data from bronze layer.
    
    Transformations:
    - Convert timestamps from string to proper timestamp type
    - Trim whitespace from all string fields
    - Standardize text fields (uppercase for country, action)
    - Retain file metadata for lineage tracking
    
    Quality Rules:
    - DROP if watchlist_id is null
    - DROP if entity_id is null
    - WARN if effective_from is null
    
    Returns:
        DataFrame: Cleaned fraud watchlist records
    """
    # Read from bronze layer
    df = spark.readStream.table('fraud_detection.bronze.fraud_watchlist')
    
    # Clean and transform data
    result_df = df.select(
        # Clean and standardize identifiers
        trim(col('watchlist_id')).alias('watchlist_id'),
        trim(col('entity_id')).alias('entity_id'),
        trim(col('watch_type')).alias('watch_type'),
        
        # Convert timestamps
        to_timestamp(col('effective_from')).alias('effective_from'),
        
        # Standardize action and status
        trim(upper(col('action'))).alias('action'),
        trim(col('status')).alias('status'),
        
        # Clean risk and reason fields
        trim(col('risk_level')).alias('risk_level'),
        trim(col('reason_code')).alias('reason_code'),
        trim(col('reason_description')).alias('reason_description'),
        
        # Clean metadata fields
        trim(col('reported_by')).alias('reported_by'),
        trim(col('reported_source')).alias('reported_source'),
        
        # Clean location fields
        trim(col('city')).alias('city'),
        trim(upper(col('country'))).alias('country'),
        
        # Preserve file metadata
        col('source_file_path'),
        col('source_file_modified_time'),
        col('ingestion_timestamp'),
        
        # Add processing timestamp
        current_timestamp().alias('processing_timestamp')
    )
    
    return result_df
