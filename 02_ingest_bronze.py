# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC このノートブックは、01_setupから呼ばれるため、個別実行は不要です。
# MAGIC # Task 1: Bronze -- 生データ取り込み（CSV -> Delta）
# MAGIC
# MAGIC Unity Catalog Volume に格納された CSV ファイルを読み込み、
# MAGIC **Bronze テーブル** として保存します。
# MAGIC
# MAGIC ```
# MAGIC Medallion Architecture:
# MAGIC   [CSV] --> [Bronze] --> Silver --> Gold
# MAGIC              ^^^ 今ここ
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

print(f"入力: {volume_path}")
print(f"出力: {catalog}.{schema}.bronze_*")

# COMMAND ----------

from pyspark.sql.types import *

users_schema = StructType([
    StructField("user_id",           IntegerType()),
    StructField("name",              StringType()),
    StructField("age",               IntegerType()),
    StructField("gender",            StringType()),
    StructField("prefecture",        StringType()),
    StructField("registration_date", StringType()),
    StructField("email",             StringType()),
])

products_schema = StructType([
    StructField("product_id",   IntegerType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("price",        IntegerType()),
    StructField("cost",         IntegerType()),
])

orders_schema = StructType([
    StructField("order_id",     IntegerType()),
    StructField("user_id",      IntegerType()),
    StructField("order_date",   StringType()),
    StructField("status",       StringType()),
    StructField("total_amount", IntegerType()),
    StructField("channel",      StringType()),
    StructField("category",     StringType()),
])

order_items_schema = StructType([
    StructField("item_id",    IntegerType()),
    StructField("order_id",   IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity",   IntegerType()),
    StructField("unit_price", IntegerType()),
    StructField("subtotal",   IntegerType()),
])

sessions_schema = StructType([
    StructField("session_id",   IntegerType()),
    StructField("user_id",      IntegerType()),
    StructField("product_id",   IntegerType()),
    StructField("category",     StringType()),
    StructField("event_type",   StringType()),
    StructField("session_date", StringType()),
])

access_logs_schema = StructType([
    StructField("access_id",   IntegerType()),
    StructField("user_id",     IntegerType()),
    StructField("timestamp",   StringType()),
    StructField("ip_address",  StringType()),
    StructField("user_agent",  StringType()),
    StructField("success",     IntegerType()),
    StructField("event_type",  StringType()),
    StructField("threat_type", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV -> Bronze テーブルへ取り込み

# COMMAND ----------

def ingest_csv(name, schema_def):
    """CSVを読み込んでBronzeテーブルとして保存"""
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("encoding", "UTF-8")
          .schema(schema_def)
          .load(f"{volume_path}/{name}.csv"))
    table_name = f"{catalog}.{schema}.bronze_{name}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))
    count = spark.table(table_name).count()
    print(f"bronze_{name}: {count:,} rows")
    return count

ingest_csv("users",       users_schema)
ingest_csv("products",    products_schema)
ingest_csv("orders",      orders_schema)
ingest_csv("order_items", order_items_schema)
ingest_csv("sessions",    sessions_schema)
ingest_csv("access_logs", access_logs_schema)

print("\nTask 1 完了: 6つの Bronze テーブルを作成しました")