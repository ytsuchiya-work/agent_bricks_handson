# Databricks notebook source
# MAGIC %md
# MAGIC このノートブックは、01_setupから呼ばれるため、個別実行は不要です。
# MAGIC # Task 2: Silver -- データ変換・クレンジング
# MAGIC
# MAGIC Bronze テーブルをクレンジングし、分析に使いやすい **Silver テーブル** に変換します。
# MAGIC
# MAGIC ```
# MAGIC Medallion Architecture:
# MAGIC   CSV --> Bronze --> [Silver] --> Gold
# MAGIC                      ^^^ 今ここ
# MAGIC ```
# MAGIC
# MAGIC **変換内容:**
# MAGIC - 日付型変換（文字列 -> DateType）
# MAGIC - 年齢層カラム（age_group）追加
# MAGIC - 年月カラム（ym）追加

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

print(f"対象: {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# -- silver_users: 年齢層を追加 --

users = (
    spark.table(f"{catalog}.{schema}.bronze_users")
    .withColumn("registration_date", F.col("registration_date").cast(DateType()))
    .withColumn("age_group",
        F.when(F.col("age") < 30, "10-20代")
         .when(F.col("age") < 40, "30代")
         .when(F.col("age") < 50, "40代")
         .otherwise("50代以上"))
)

(users.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_users"))
print(f"silver_users: {users.count()} rows")

# -- silver_products: そのまま --

products = spark.table(f"{catalog}.{schema}.bronze_products")
(products.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_products"))
print(f"silver_products: {products.count()} rows")

# -- silver_orders: 日付型変換 + 年月カラム追加 --

orders = (
    spark.table(f"{catalog}.{schema}.bronze_orders")
    .withColumn("order_date", F.col("order_date").cast(DateType()))
    .withColumn("year",  F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("ym",    F.date_format("order_date", "yyyy-MM"))
)

(orders.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_orders"))
print(f"silver_orders: {orders.count()} rows")

# -- silver_order_items: そのまま --

order_items = spark.table(f"{catalog}.{schema}.bronze_order_items")
(order_items.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_order_items"))
print(f"silver_order_items: {order_items.count()} rows")

# -- silver_sessions: 日付型変換 + 年月カラム追加 --

sessions = (
    spark.table(f"{catalog}.{schema}.bronze_sessions")
    .withColumn("session_date", F.col("session_date").cast(DateType()))
    .withColumn("ym", F.date_format("session_date", "yyyy-MM"))
)

(sessions.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_sessions"))
print(f"silver_sessions: {sessions.count()} rows")

# -- silver_access_logs: タイムスタンプ変換 + 時間帯・年月カラム追加 --

from pyspark.sql.types import TimestampType

access_logs = (
    spark.table(f"{catalog}.{schema}.bronze_access_logs")
    .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
    .withColumn("access_date", F.to_date("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("ym", F.date_format("timestamp", "yyyy-MM"))
    .withColumn("time_zone",
        F.when(F.col("hour").between(2, 5), "深夜帯")
         .when(F.col("hour").between(6, 11), "午前")
         .when(F.col("hour").between(12, 17), "午後")
         .otherwise("夜間"))
)

(access_logs.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.silver_access_logs"))
print(f"silver_access_logs: {access_logs.count()} rows")

# COMMAND ----------

print("\nTask 2 完了: 6つの Silver テーブルを作成しました")
print(f"  bronze_* (生データそのまま) --> silver_* (型変換・カラム追加済み)")