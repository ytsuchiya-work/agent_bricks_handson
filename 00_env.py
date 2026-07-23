# Databricks notebook source
# MAGIC %md
# MAGIC # 環境設定
# MAGIC
# MAGIC **以下の2行を自分の環境に合わせて変更してください。**
# MAGIC
# MAGIC このノートブックは他のノートブックから `%run ./00_env` で呼び出されます。
# MAGIC ここを1回変更すれば、全てのノートブックに反映されます。

# COMMAND ----------

catalog = "ytcy_azure_east2classic_stable"
schema  = "ec_site_demo"

volume_path = f"/Volumes/{catalog}/{schema}/raw_data"
manual_path = f"/Volumes/{catalog}/{schema}/manuals"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"カタログ: {catalog} / スキーマ: {schema}")
print(f"Volume:  {volume_path}")
print(f"マニュアル: {manual_path}")
