# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 環境設定
# MAGIC
# MAGIC **以下の2行を自分の環境に合わせて変更してください。**
# MAGIC
# MAGIC `catalog = "ytcy_azure_east2classic_stable"`<br>
# MAGIC `schema  = f"ec_site_demo_{_safe_user}"`
# MAGIC
# MAGIC このノートブックは他のノートブックから `%run ./00_env` で呼び出されます。
# MAGIC ここを1回変更すれば、全てのノートブックに反映されます。

# COMMAND ----------

import re
_user = spark.sql("SELECT current_user()").collect()[0][0]
_safe_user = re.sub(r"[^a-zA-Z0-9]", "_", _user.split("@")[0])

catalog = "ytcy_azure_east2classic_stable"
schema  = f"ec_site_demo_{_safe_user}"

volume_path = f"/Volumes/{catalog}/{schema}/raw_data"
manual_path = f"/Volumes/{catalog}/{schema}/manuals"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"カタログ: {catalog} / スキーマ: {schema}")
print(f"Volume:  {volume_path}")
print(f"マニュアル: {manual_path}")

# COMMAND ----------

