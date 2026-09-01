"""
Customer Master Data - Silver Layer
====================================
Single source of truth for customer dimension data.
This table serves ALL downstream pipelines requiring customer information.

Pipeline: customer-master-data-pl
Owner: Data Platform / Customer Data Team
Consumers: fraud_detection, marketing, analytics, customer_360

Data Lineage:
  Source: External operational database (Postgres/MySQL)
  Bronze: customers.bronze.customers (via Lakeflow Connect ingestion)
  Silver: customers.silver.customers (this table) ← YOU ARE HERE
  
Transformations:
  • Data validation (customer_id NOT NULL, age > 0)
  • Data cleansing (trim whitespace, normalize text)
  • Data conforming (standardize date formats, uppercase country codes)
  • Quality enforcement (STRICT - invalid records are dropped)

Quality Expectations:
  • DROP: customer_id IS NULL
  • DROP: age IS NULL OR age <= 0  
  • WARN: email IS NULL
  • WARN: card_number IS NULL

Schema Contract:
  This silver table provides a stable, versioned schema contract for all consumers.
  Breaking schema changes require cross-team coordination and migration planning.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, current_timestamp, trim, upper


@dp.table(
    name='fraud_detection.silver.customers',
    comment='Single source of truth for customer master data. Consumed by fraud_detection, marketing, analytics, and other domain pipelines.'
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_age", "age IS NOT NULL AND age > 0")
@dp.expect("valid_email", "email IS NOT NULL")
@dp.expect("valid_card_number", "card_number IS NOT NULL")
def customers_silver() -> DataFrame:
    """
    Transform customer bronze data into conformed silver layer.
    
    This function creates the authoritative customer dimension consumed by all
    downstream pipelines. Changes to this transformation affect all consumers.
    
    Architecture Note:
    -----------------
    Customer data is MASTER DATA, not domain-specific. This pipeline is
    intentionally separate from domain pipelines (fraud detection, marketing)
    to enable reusability and prevent coupling.
    
    Data Quality:
    -------------
    - STRICT validation: Invalid records are DROPPED (not warned)
    - Rationale: Customer data quality issues cascade to all consumers
    - All consumers can trust that customer_id and age are valid
    
    Transformations Applied:
    ------------------------
    1. Text normalization (trim, uppercase)
    2. Date parsing (account_open_date)
    3. Add processing metadata (bronze_update_timestamp, silver_load_timestamp)
    
    Returns:
        DataFrame: Cleaned, validated, conformed customer records
    """
    # Read from bronze layer (populated by Lakeflow Connect ingestion)
    df = spark.readStream.table('fraud_detection.bronze.customers')
    
    # Apply cleansing and conforming transformations
    result_df = df.select(
        # Core Identifiers
        trim(col('customer_id')).alias('customer_id'),
        trim(col('card_number')).alias('card_number'),
        trim(col('card_type')).alias('card_type'),
        
        # Personal Information
        trim(col('first_name')).alias('first_name'),
        trim(col('last_name')).alias('last_name'),
        trim(col('email')).alias('email'),
        col('age'),
        trim(col('gender')).alias('gender'),
        
        # Location Information
        trim(col('city')).alias('city'),
        trim(upper(col('country'))).alias('country'),  # Standardize to uppercase
        trim(col('state')).alias('state'),
        
        # Account and Financial Information
        to_date(col('account_open_date'), 'yyyy-MM-dd').alias('account_open_date'),
        col('annual_income'),
        col('transaction_limit'),
        col('risk_score'),
        trim(col('customer_segment')).alias('customer_segment'),
        
        # Customer Preferences
        col('preferred_spending_min'),
        col('preferred_spending_max'),
        trim(col('preferred_city')).alias('preferred_city'),
        trim(col('preferred_country')).alias('preferred_country'),
        trim(col('trusted_device_id')).alias('trusted_device_id'),
        
        # Metadata & Lineage
        col('update_timestamp').alias('bronze_update_timestamp'),  # Preserve source timestamp
        current_timestamp().alias('silver_load_timestamp')         # Track transformation time
    )
    
    return result_df
