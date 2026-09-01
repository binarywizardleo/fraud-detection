"""
Gold Layer: Fraud Trends by Category
====================================
Daily fraud alert trends broken down by merchant category for reporting.
Shows which merchant categories attract the most fraud and at what risk levels.

Dependencies:
- fraud_detection.gold.fraud_card_alerts

Target Table: fraud_detection.gold.fraud_trends_by_category
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, count, countDistinct, sum as _sum, avg, max as _max,
    when, lit
)


@dp.materialized_view(
    name='fraud_detection.gold.fraud_trends_by_category',
    comment='Daily fraud alert trends by merchant category with risk level breakdowns',
    partition_cols=['alert_date']
)
def fraud_trends_by_category() -> DataFrame:
    """
    Aggregate fraud card alerts by date and merchant category.

    Provides per-day, per-category metrics including alert counts, fraud amounts,
    affected customers and cards, and a breakdown by watchlist risk level.

    Returns:
        DataFrame: One row per (alert_date, merchant_category) with trend metrics
    """
    fraud_alerts_df = spark.read.table('fraud_detection.gold.fraud_card_alerts')

    result_df = (
        fraud_alerts_df
        .withColumn('alert_date', to_date(col('transaction_timestamp')))
        .groupBy('alert_date', 'merchant_category')
        .agg(
            # Volume metrics
            count('*').alias('fraud_alert_count'),
            _sum('amount').alias('total_fraud_amount'),
            avg('amount').alias('avg_fraud_amount'),
            _max('amount').alias('max_fraud_amount'),

            # Impact metrics
            countDistinct('customer_id').alias('unique_customers_affected'),
            countDistinct('card_number').alias('unique_cards_flagged'),
            countDistinct('transaction_country').alias('unique_countries'),

            # Risk level breakdowns
            _sum(when(col('watchlist_risk_level') == lit('HIGH'), 1).otherwise(0)).alias('high_risk_count'),
            _sum(when(col('watchlist_risk_level') == lit('MEDIUM'), 1).otherwise(0)).alias('medium_risk_count'),
            _sum(when(col('watchlist_risk_level') == lit('LOW'), 1).otherwise(0)).alias('low_risk_count'),

            # Amount over limit metrics
            _sum('amount_over_limit').alias('total_amount_over_limit'),
            avg('percent_over_limit').alias('avg_percent_over_limit')
        )
        .orderBy('alert_date', 'merchant_category')
    )

    return result_df