from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, md5, concat, current_timestamp, lit
from pyspark.sql import DataFrame


dp.create_streaming_table(
    name = 'fraud_detection.gold.fraud_card_alert'
    , comment = 'this table stores fraud card alert data'
)

@dp.append_flow(
    target = 'fraud_detection.gold.fraud_card_alert'
    , name = 'fraud_card_alert_fn'
)
def fraud_card_alert_fn() -> DataFrame:
    tdf = spark.readStream.table("fraud_detection.silver.transactions_dp")
    fdf = spark.readStream.table("fraud_detection.silver.fraud_watchlist_dp")
    cdf = spark.read.table("fraud_detection.silver.customers")

    tdf_watermark = tdf.withWatermark('transaction_timestamp', '10 minutes')
    fdf_watermark = fdf.withWatermark('effective_from', '10 minutes')

    
    return tdf_watermark.alias('tdf')\
        .join(fdf_watermark.alias('fdf'), col('tdf.card_number') == col('fdf.entity_id'), 'inner')\
        .join(cdf.alias('cdf'), on='customer_id', how='left') \
        .select(
            # Transaction details
            col('tdf.transaction_id').alias('transaction_id'),
            col('tdf.customer_id').alias('customer_id'),
            col('tdf.card_number').alias('card_number'),
            col('tdf.amount').alias('amount'),
            col('tdf.currency').alias('currency'),
            col('tdf.merchant_name').alias('merchant_name'),
            col('tdf.transaction_timestamp').alias('transaction_timestamp'),
            col('tdf.city').alias('transaction_city'),
            col('tdf.country').alias('transaction_country'),
            col('tdf.payment_channel').alias('payment_channel'),
            col('tdf.device_id').alias('device_id'),
            
            # Customer details
            concat_ws(' ', col('cdf.first_name'), col('cdf.last_name')).alias('customer_full_name'),
            lit('binary.wizard.leo@gmail.com').alias('email'),
            col('cdf.transaction_limit').alias('transaction_limit'),
            
            # Watchlist details
            col('fdf.watchlist_id').alias('watchlist_id'),
            col('fdf.risk_level').alias('risk_level'),
            col('fdf.reason_code').alias('reason_code'),
            col('fdf.reason_description').alias('reason_description'),
            col('fdf.action').alias('watchlist_action'),
            col('fdf.effective_from').alias('watchlist_effective_from'),
            
            # Calculated alert metrics
            (col('tdf.amount') - col('cdf.transaction_limit')).alias('amount_over_limit'),
            (((col('tdf.amount') - col('cdf.transaction_limit')) / col('cdf.transaction_limit')) * lit(100)).alias('percent_over_limit'),
            
            # Processing metadata
            current_timestamp().alias('alert_timestamp')
        )
