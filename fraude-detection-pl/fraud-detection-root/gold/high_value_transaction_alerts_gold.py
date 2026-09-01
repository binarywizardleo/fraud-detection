"""
Gold Layer: High-Value Transaction Alerts
==========================================
Business-level alerts identifying transactions that exceed customer transaction limits.
Joins transaction and customer data to detect and flag high-value transactions.

Dependencies:
- fraud_detection.silver.transactions
- fraud_detection.silver.customers

Target Table: fraud_detection.gold.high_value_transaction_alerts
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, md5, concat, current_timestamp


@dp.table(
    name='fraud_detection.gold.high_value_transaction_alerts',
    comment='Alerts for transactions exceeding customer transaction limits'
)
def high_value_transaction_alerts_gold() -> DataFrame:
    """
    Generate high-value transaction alerts.
    
    Business Logic:
    - Identify transactions where amount >= customer transaction_limit
    - Join with customer data to enrich alert information
    - Calculate amount over limit and percentage metrics
    - Generate unique alert ID for tracking
    
    Returns:
        DataFrame: High-value transaction alert records
    """
    # Read from silver layer
    transactions_df = spark.readStream.table('fraud_detection.silver.transactions')
    customers_df = spark.read.table('fraud_detection.silver.customers')
    
    # Join transactions with customers
    joined_df = (
        transactions_df.alias('t')
        .join(
            customers_df.alias('c'),
            col('t.customer_id') == col('c.customer_id'),
            'left'
        )
    )
    
    # Filter for high-value transactions (amount >= transaction_limit)
    high_value_df = joined_df.filter(col('t.amount') >= col('c.transaction_limit'))
    
    # Select and enrich alert fields
    result_df = high_value_df.select(
        # Generate unique alert ID
        md5(
            concat(
                col('t.transaction_id'),
                col('t.transaction_timestamp').cast('string')
            )
        ).alias('alert_id'),
        
        # Transaction details
        col('t.transaction_id').alias('transaction_id'),
        col('t.customer_id').alias('customer_id'),
        col('t.card_number').alias('transaction_card_number'),
        col('t.amount').alias('amount'),
        col('t.currency').alias('currency'),
        col('t.transaction_timestamp').alias('transaction_timestamp'),
        col('t.transaction_type').alias('transaction_type'),
        col('t.status').alias('transaction_status'),
        
        # Merchant details
        col('t.merchant_id').alias('merchant_id'),
        col('t.merchant_name').alias('merchant_name'),
        col('t.merchant_category').alias('merchant_category'),
        
        # Transaction location
        col('t.city').alias('transaction_city'),
        col('t.country').alias('transaction_country'),
        col('t.is_international').alias('is_international'),
        col('t.payment_channel').alias('payment_channel'),
        col('t.device_id').alias('device_id'),
        
        # Customer details
        concat_ws(' ', col('c.first_name'), col('c.last_name')).alias('customer_full_name'),
        col('c.first_name').alias('customer_first_name'),
        col('c.last_name').alias('customer_last_name'),
        col('c.email').alias('customer_email'),
        col('c.city').alias('customer_city'),
        col('c.country').alias('customer_country'),
        col('c.card_number').alias('customer_card_number'),
        col('c.risk_score').alias('customer_risk_score'),
        col('c.transaction_limit').alias('transaction_limit'),
        col('c.annual_income').alias('annual_income'),
        
        # Alert metrics
        (col('t.amount') - col('c.transaction_limit')).alias('amount_over_limit'),
        ((col('t.amount') / col('c.transaction_limit')) * 100).alias('percent_over_limit'),
        
        # Alert metadata
        current_timestamp().alias('alert_timestamp')
    )
    
    return result_df
