"""
Gold Layer: Customer Risk Profile
==================================
Comprehensive per-customer risk profile combining demographics, transaction behavior,
and alert history for reporting and dashboards.

Dependencies:
- fraud_detection.silver.customers
- fraud_detection.silver.transactions
- fraud_detection.gold.high_value_transaction_alerts
- fraud_detection.gold.fraud_card_alerts

Target Table: fraud_detection.gold.customer_risk_profile
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, max as _max,
    concat_ws, datediff, current_date, when, lit
)


@dp.materialized_view(
    name='fraud_detection.gold.customer_risk_profile',
    comment='Per-customer risk profile with demographics, transaction behavior, and alert history',
    cluster_by=['customer_id']
)
def customer_risk_profile() -> DataFrame:
    """
    Build a comprehensive risk profile for each customer.

    Aggregates transaction volume, fraud alerts, and high-value alerts per customer,
    then joins with customer demographics and computes an overall risk tier.

    Returns:
        DataFrame: One row per customer with risk metrics and demographics
    """
    # --- Source reads (batch reads from streaming/batch tables for MV) ---
    customers_df = spark.read.table('fraud_detection.silver.customers')
    transactions_df = spark.read.table('fraud_detection.silver.transactions')
    high_value_alerts_df = spark.read.table('fraud_detection.gold.high_value_transaction_alerts')
    fraud_alerts_df = spark.read.table('fraud_detection.gold.fraud_card_alerts')

    # --- Pre-aggregate transactions per customer ---
    tx_agg = (
        transactions_df.groupBy('customer_id')
        .agg(
            count('*').alias('total_transactions'),
            _sum('amount').alias('total_transaction_amount'),
            avg('amount').alias('avg_transaction_amount'),
            _max('amount').alias('max_transaction_amount'),
            countDistinct('merchant_name').alias('unique_merchants'),
            countDistinct('country').alias('unique_countries'),
            _sum(when(col('is_international') == lit(True), 1).otherwise(0)).alias('international_transaction_count')
        )
    )

    # --- Pre-aggregate high-value alerts per customer ---
    hv_agg = (
        high_value_alerts_df.groupBy('customer_id')
        .agg(
            count('*').alias('high_value_alert_count'),
            _sum('amount').alias('high_value_total_amount'),
            _max('amount').alias('high_value_max_amount')
        )
    )

    # --- Pre-aggregate fraud alerts per customer ---
    fa_agg = (
        fraud_alerts_df.groupBy('customer_id')
        .agg(
            count('*').alias('fraud_alert_count'),
            _sum('amount').alias('total_fraud_amount'),
            countDistinct('watchlist_id').alias('unique_watchlist_hits')
        )
    )

    # --- Join all aggregates with customer demographics ---
    result_df = (
        customers_df.alias('c')
        .join(tx_agg.alias('t'), col('c.customer_id') == col('t.customer_id'), 'left')
        .join(hv_agg.alias('hv'), col('c.customer_id') == col('hv.customer_id'), 'left')
        .join(fa_agg.alias('fa'), col('c.customer_id') == col('fa.customer_id'), 'left')
        .select(
            # Customer identity
            col('c.customer_id').alias('customer_id'),
            concat_ws(' ', col('c.first_name'), col('c.last_name')).alias('customer_full_name'),
            col('c.email').alias('customer_email'),
            col('c.customer_segment').alias('customer_segment'),

            # Demographics
            col('c.age').alias('age'),
            col('c.gender').alias('gender'),
            col('c.city').alias('city'),
            col('c.country').alias('country'),
            col('c.card_type').alias('card_type'),
            datediff(current_date(), col('c.account_open_date')).alias('account_age_days'),

            # Financial profile
            col('c.annual_income').alias('annual_income'),
            col('c.transaction_limit').alias('transaction_limit'),
            col('c.risk_score').alias('risk_score'),

            # Transaction behavior
            col('t.total_transactions').alias('total_transactions'),
            col('t.total_transaction_amount').alias('total_transaction_amount'),
            col('t.avg_transaction_amount').alias('avg_transaction_amount'),
            col('t.max_transaction_amount').alias('max_transaction_amount'),
            col('t.unique_merchants').alias('unique_merchants'),
            col('t.unique_countries').alias('unique_countries'),
            col('t.international_transaction_count').alias('international_transaction_count'),

            # Alert history
            col('hv.high_value_alert_count').alias('high_value_alert_count'),
            col('hv.high_value_total_amount').alias('high_value_total_amount'),
            col('hv.high_value_max_amount').alias('high_value_max_amount'),
            col('fa.fraud_alert_count').alias('fraud_alert_count'),
            col('fa.total_fraud_amount').alias('total_fraud_amount'),
            col('fa.unique_watchlist_hits').alias('unique_watchlist_hits'),

            # Computed risk tier
            when(
                (col('c.risk_score') >= 80) | (col('fa.fraud_alert_count') > 0), lit('HIGH')
            ).when(
                (col('c.risk_score') >= 50) | (col('hv.high_value_alert_count') > 2), lit('MEDIUM')
            ).otherwise(lit('LOW')).alias('risk_tier')
        )
    )

    return result_df