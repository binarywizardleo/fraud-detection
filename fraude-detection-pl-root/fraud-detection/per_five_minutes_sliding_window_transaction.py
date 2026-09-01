from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, window, col

@dp.table(
    name = 'fraud_detection.gold.transactions_per_five_minute_sliding'
    , comment = 'stores count of transactions per five minutes with one minutes of sliding window'
)
def transactions_per_minute() -> DataFrame:
    df = spark.readStream.table('fraud_detection.silver.transactions_dp')
    df = df.withWatermark('transaction_timestamp', '15 minutes')\
            .groupBy(window('transaction_timestamp', "5 minute", "1 minute"))\
            .agg(col('window.start').alias("start")
                 , col('window.end').alias("end")
                  , count("*").alias("transactions_per_minute"))
    return df