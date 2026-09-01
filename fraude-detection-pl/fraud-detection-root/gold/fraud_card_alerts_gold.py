"""
Gold Layer: Fraud Card Alerts
==============================
Business-level fraud alerts identifying transactions from cards on the fraud watchlist.
Joins transaction, fraud watchlist, and customer data to generate comprehensive alerts.

Dependencies: 
- fraud_detection.silver.transactions
- fraud_detection.silver.fraud_watchlist
- fraud_detection.silver.customers

Target Table: fraud_detection.gold.fraud_card_alerts
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit


@dp.table(
    name='fraud_detection.gold.fraud_card_alerts',
    comment='Fraud alerts for transactions involving cards on the fraud watchlist'
)
def fraud_card_alerts_gold() -> DataFrame:
    """
    Generate fraud card alerts by joining transactions with fraud watchlist.
    
    Business Logic:
    - Identify transactions where card_number matches watchlist entity_id
    - Join with customer data to enrich alert information
    - Calculate amount over limit and percentage metrics
    - Use stream-stream join with watermarks for bounded state
    
    Returns:
        DataFrame: Fraud card alert records with full context
    """
    # Read from silver layer
    transactions_df = spark.readStream.table('fraud_detection.silver.transactions')
    watchlist_df = spark.readStream.table('fraud_detection.silver.fraud_watchlist')
    customers_df = spark.read.table('fraud_detection.silver.customers')
    
    # Apply watermarks for stream-stream join (10 minutes)
    transactions_wm = transactions_df.withWatermark('transaction_timestamp', '10 minutes')
    watchlist_wm = watchlist_df.withWatermark('effective_from', '10 minutes')
    
    # Join transactions with fraud watchlist on card number
    alert_df = (
        transactions_wm.alias('t')
        .join(
            watchlist_wm.alias('w'),
            col('t.card_number') == col('w.entity_id'),
            'inner'
        )
        .join(
            customers_df.alias('c'),
            col('t.customer_id') == col('c.customer_id'),
            'left'
        )
    )
    
    # Select and enrich alert fields
    result_df = alert_df.select(
        # Transaction details
        col('t.transaction_id').alias('transaction_id'),
        col('t.customer_id').alias('customer_id'),
        col('t.card_number').alias('card_number'),
        col('t.amount').alias('amount'),
        col('t.currency').alias('currency'),
        col('t.merchant_name').alias('merchant_name'),
        col('t.merchant_category').alias('merchant_category'),
        col('t.transaction_timestamp').alias('transaction_timestamp'),
        col('t.city').alias('transaction_city'),
        col('t.country').alias('transaction_country'),
        col('t.payment_channel').alias('payment_channel'),
        col('t.device_id').alias('device_id'),
        col('t.is_international').alias('is_international'),
        
        # Customer details
        concat_ws(' ', col('c.first_name'), col('c.last_name')).alias('customer_full_name'),
        col('c.email').alias('customer_email'),
        col('c.city').alias('customer_city'),
        col('c.country').alias('customer_country'),
        col('c.transaction_limit').alias('transaction_limit'),
        col('c.risk_score').alias('customer_risk_score'),
        
        # Watchlist details
        col('w.watchlist_id').alias('watchlist_id'),
        col('w.risk_level').alias('watchlist_risk_level'),
        col('w.reason_code').alias('watchlist_reason_code'),
        col('w.reason_description').alias('watchlist_reason_description'),
        col('w.action').alias('watchlist_action'),
        col('w.effective_from').alias('watchlist_effective_from'),
        col('w.status').alias('watchlist_status'),
        
        # Calculated alert metrics
        (col('t.amount') - col('c.transaction_limit')).alias('amount_over_limit'),
        (((col('t.amount') - col('c.transaction_limit')) / col('c.transaction_limit')) * lit(100)).alias('percent_over_limit'),
        
        # Alert metadata
        current_timestamp().alias('alert_timestamp')
    )
    
    return result_df
