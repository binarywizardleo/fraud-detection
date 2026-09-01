-- Databricks notebook source
select count(*) from fraud_detection.bronze.transactions_dp

-- COMMAND ----------

select count(*) from fraud_detection.silver.transactions_dp

-- COMMAND ----------

select * from fraud_detection.gold.high_value_transaction_alert

-- COMMAND ----------

select * from fraud_detection.bronze.fraud_watchlist_dp

-- COMMAND ----------

