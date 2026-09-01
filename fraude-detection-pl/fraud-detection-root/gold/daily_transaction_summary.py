"""
Gold Layer: Daily Transaction Summary
======================================
Daily transaction volume, counts, and channel breakdowns enriched with
fraud and high-value alert counts for operational reporting dashboards.

Dependencies:
- fraud_detection.silver.transactions
- fraud_detection.gold.fraud_card_alerts
- fraud_detection.gold.high_value_transaction_alerts

Target Table: fraud_detection.gold.daily_transaction_summary
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, count, countDistinct, sum as _sum, avg, max as _max,
    when, lit
)


@dp.materialized_view(
    name='fraud_detection.gold.daily_transaction_summary',
    comment='Daily transaction summary with volume, channel breakdown, and alert counts',
    partition_cols=['transaction_date']
)
def daily_transaction_summary() -> DataFrame:
    """
    Aggregate transactions by day with volume, count, and channel breakdowns,
    then enrich with fraud and high-value alert counts for the same day.

    Returns:
        DataFrame: One row per transaction_date with summary metrics
    """
    # --- Source reads ---
    transactions_df = spark.read.table('fraud_detection.silver.transactions')
    fraud_alerts_df = spark.read.table('fraud_detection.gold.fraud_card_alerts')
    high_value_alerts_df = spark.read.table('fraud_detection.gold.high_value_transaction_alerts')

    # --- Pre-aggregate daily transactions ---
    tx_daily = (
        transactions_df
        .withColumn('transaction_date', to_date(col('transaction_timestamp')))
        .groupBy('transaction_date')
        .agg(
            # Volume metrics
            count('*').alias('total_transactions'),
            _sum('amount').alias('total_volume'),
            avg('amount').alias('avg_transaction_amount'),
            _max('amount').alias('max_transaction_amount'),

            # Customer / merchant diversity
            countDistinct('customer_id').alias('unique_customers'),
            countDistinct('merchant_name').alias('unique_merchants'),

            # International vs domestic
            _sum(when(col('is_international') == lit(True), 1).otherwise(0)).alias('international_transaction_count'),
            _sum(when(col('is_international') == lit(True), col('amount')).otherwise(lit(0))).alias('international_transaction_volume'),
            _sum(when(col('is_international') == lit(False), 1).otherwise(0)).alias('domestic_transaction_count'),
            _sum(when(col('is_international') == lit(False), col('amount')).otherwise(lit(0))).alias('domestic_transaction_volume'),

            # Status breakdown
            _sum(when(col('status') == lit('completed'), 1).otherwise(0)).alias('completed_transaction_count'),
            _sum(when(col('status') == lit('failed'), 1).otherwise(0)).alias('failed_transaction_count'),

            # Channel breakdown
            _sum(when(col('payment_channel') == lit('online'), 1).otherwise(0)).alias('online_transaction_count'),
            _sum(when(col('payment_channel') == lit('pos'), 1).otherwise(0)).alias('pos_transaction_count'),
            _sum(when(col('payment_channel') == lit('mobile'), 1).otherwise(0)).alias('mobile_transaction_count'),
            _sum(when(col('payment_channel') == lit('ATM'), 1).otherwise(0)).alias('atm_transaction_count')
        )
    )

    # --- Pre-aggregate daily fraud alerts ---
    fa_daily = (
        fraud_alerts_df
        .withColumn('transaction_date', to_date(col('transaction_timestamp')))
        .groupBy('transaction_date')
        .agg(
            count('*').alias('fraud_alert_count'),
            _sum('amount').alias('fraud_transaction_volume'),
            countDistinct('customer_id').alias('fraud_affected_customers')
        )
    )

    # --- Pre-aggregate daily high-value alerts ---
    hv_daily = (
        high_value_alerts_df
        .withColumn('transaction_date', to_date(col('transaction_timestamp')))
        .groupBy('transaction_date')
        .agg(
            count('*').alias('high_value_alert_count'),
            _sum('amount').alias('high_value_transaction_volume')
        )
    )

    # --- Join all daily aggregates ---
    result_df = (
        tx_daily.alias('t')
        .join(fa_daily.alias('fa'), col('t.transaction_date') == col('fa.transaction_date'), 'left')
        .join(hv_daily.alias('hv'), col('t.transaction_date') == col('hv.transaction_date'), 'left')
        .select(
            # Date
            col('t.transaction_date').alias('transaction_date'),

            # Transaction volume
            col('t.total_transactions').alias('total_transactions'),
            col('t.total_volume').alias('total_volume'),
            col('t.avg_transaction_amount').alias('avg_transaction_amount'),
            col('t.max_transaction_amount').alias('max_transaction_amount'),

            # Diversity
            col('t.unique_customers').alias('unique_customers'),
            col('t.unique_merchants').alias('unique_merchants'),

            # International vs domestic
            col('t.international_transaction_count').alias('international_transaction_count'),
            col('t.international_transaction_volume').alias('international_transaction_volume'),
            col('t.domestic_transaction_count').alias('domestic_transaction_count'),
            col('t.domestic_transaction_volume').alias('domestic_transaction_volume'),

            # Status breakdown
            col('t.completed_transaction_count').alias('completed_transaction_count'),
            col('t.failed_transaction_count').alias('failed_transaction_count'),

            # Channel breakdown
            col('t.online_transaction_count').alias('online_transaction_count'),
            col('t.pos_transaction_count').alias('pos_transaction_count'),
            col('t.mobile_transaction_count').alias('mobile_transaction_count'),
            col('t.atm_transaction_count').alias('atm_transaction_count'),

            # Alert counts
            col('fa.fraud_alert_count').alias('fraud_alert_count'),
            col('fa.fraud_transaction_volume').alias('fraud_transaction_volume'),
            col('fa.fraud_affected_customers').alias('fraud_affected_customers'),
            col('hv.high_value_alert_count').alias('high_value_alert_count'),
            col('hv.high_value_transaction_volume').alias('high_value_transaction_volume')
        )
        .orderBy('transaction_date')
    )

    return result_df