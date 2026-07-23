# Databricks notebook source
# MAGIC %md
# MAGIC このノートブックは、01_setupから呼ばれるため、個別実行は不要です。
# MAGIC # Task 3: Gold -- マートテーブル作成
# MAGIC
# MAGIC Silver テーブルから、ビジネス分析・ダッシュボード・Genie 用の
# MAGIC **Gold テーブル**（集計マート）を作成します。
# MAGIC
# MAGIC ```
# MAGIC Medallion Architecture:
# MAGIC   CSV --> Bronze --> Silver --> [Gold]
# MAGIC                                 ^^^ 今ここ
# MAGIC ```
# MAGIC
# MAGIC | テーブル | 内容 |
# MAGIC |---|---|
# MAGIC | gold_monthly_sales | 月別・カテゴリ別 売上サマリー |
# MAGIC | gold_customer_rfm | 顧客RFMセグメント |
# MAGIC | gold_cart_abandonment | カテゴリ x 月別 カゴ落ち率 |
# MAGIC | gold_access_summary | 月別アクセス成功/失敗/脅威集計 |
# MAGIC | gold_threat_events | 脅威イベント検出サマリー |
# MAGIC | gold_user_risk | ユーザーリスクスコア |

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

print(f"対象: {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(f"{catalog}.{schema}.silver_orders")
users = spark.table(f"{catalog}.{schema}.silver_users")
sessions = spark.table(f"{catalog}.{schema}.silver_sessions")

print(f"silver_orders: {orders.count():,}件 / silver_users: {users.count():,}件 / silver_sessions: {sessions.count():,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1: 月別カテゴリ別売上（gold_monthly_sales）
# MAGIC
# MAGIC ダッシュボードの売上トレンドグラフに使用します。

# COMMAND ----------

monthly_sales = (
    orders
    .filter(F.col("status") == "completed")
    .groupBy("ym", "category")
    .agg(
        F.sum("total_amount").alias("sales_amount"),
        F.count("order_id").alias("order_count"),
        F.countDistinct("user_id").alias("unique_buyers"),
        F.avg("total_amount").alias("avg_order_value"),
    )
    .orderBy("ym", "category")
)

(monthly_sales.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_monthly_sales"))
print(f"gold_monthly_sales: {monthly_sales.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2: 顧客RFMスコア（gold_customer_rfm）
# MAGIC
# MAGIC | 指標 | 意味 |
# MAGIC |---|---|
# MAGIC | Recency | 最終購入からの経過日数 |
# MAGIC | Frequency | 購入回数 |
# MAGIC | Monetary | 合計購入金額 |

# COMMAND ----------

latest_date = orders.filter(F.col("status") == "completed").agg(F.max("order_date")).collect()[0][0]

rfm_base = (
    orders
    .filter(F.col("status") == "completed")
    .groupBy("user_id")
    .agg(
        F.datediff(F.lit(latest_date), F.max("order_date")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.sum("total_amount").alias("monetary"),
    )
)

rfm = (
    rfm_base
    .withColumn("r_score",
        F.when(F.col("recency_days") <= 30, 5)
         .when(F.col("recency_days") <= 90, 4)
         .when(F.col("recency_days") <= 180, 3)
         .when(F.col("recency_days") <= 365, 2)
         .otherwise(1))
    .withColumn("f_score",
        F.when(F.col("frequency") >= 10, 5)
         .when(F.col("frequency") >= 6, 4)
         .when(F.col("frequency") >= 3, 3)
         .when(F.col("frequency") >= 2, 2)
         .otherwise(1))
    .withColumn("m_score",
        F.when(F.col("monetary") >= 100000, 5)
         .when(F.col("monetary") >= 50000, 4)
         .when(F.col("monetary") >= 20000, 3)
         .when(F.col("monetary") >= 10000, 2)
         .otherwise(1))
    .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    .withColumn("segment",
        F.when(F.col("rfm_total") >= 13, "優良顧客")
         .when(F.col("rfm_total") >= 10, "成長顧客")
         .when(F.col("rfm_total") >= 7,  "休眠予備軍")
         .otherwise("離脱リスク"))
    .join(users.select("user_id", "age", "age_group", "gender", "prefecture"), "user_id")
)

(rfm.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_customer_rfm"))
print(f"gold_customer_rfm: {rfm.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3: カゴ落ち率（gold_cart_abandonment）

# COMMAND ----------

cart_aband = (
    sessions
    .groupBy("ym", "category")
    .agg(
        F.count("session_id").alias("total_sessions"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum(F.when(F.col("event_type") == "abandon", 1).otherwise(0)).alias("abandons"),
    )
    .withColumn("abandonment_rate",
        F.round(F.col("abandons") / F.col("total_sessions") * 100, 1))
    .withColumn("purchase_rate",
        F.round(F.col("purchases") / F.col("total_sessions") * 100, 1))
    .orderBy("ym", "category")
)

(cart_aband.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_cart_abandonment"))
print(f"gold_cart_abandonment: {cart_aband.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 4: アクセスサマリー（gold_access_summary）
# MAGIC
# MAGIC セキュリティ監視ダッシュボード用。月別・脅威タイプ別のアクセス集計。

# COMMAND ----------

access_logs = spark.table(f"{catalog}.{schema}.silver_access_logs")

access_summary = (
    access_logs
    .groupBy("ym", "threat_type", "time_zone")
    .agg(
        F.count("access_id").alias("evt_cnt"),
        F.sum(F.when(F.col("success") == 1, 1).otherwise(0)).alias("succ_cnt"),
        F.sum(F.when(F.col("success") == 0, 1).otherwise(0)).alias("fail_cnt"),
        F.countDistinct("ip_address").alias("uniq_src"),
        F.countDistinct("user_id").alias("uniq_usr"),
    )
    .withColumn("fail_pct",
        F.round(F.col("fail_cnt") / F.col("evt_cnt") * 100, 1))
    .withColumnRenamed("ym", "period")
    .withColumnRenamed("threat_type", "thr_cls")
    .withColumnRenamed("time_zone", "tz_band")
    .orderBy("period", "thr_cls")
)

(access_summary.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_access_summary"))
print(f"gold_access_summary: {access_summary.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 5: 脅威イベント検出（gold_threat_events）
# MAGIC
# MAGIC 月別の脅威タイプ別イベント数と、攻撃元IP情報。

# COMMAND ----------

threat_events = (
    access_logs
    .filter(F.col("threat_type") != "normal")
    .groupBy("ym", "threat_type", "ip_address")
    .agg(
        F.count("access_id").alias("evt_cnt"),
        F.sum(F.when(F.col("success") == 1, 1).otherwise(0)).alias("succ_cnt"),
        F.sum(F.when(F.col("success") == 0, 1).otherwise(0)).alias("fail_cnt"),
        F.countDistinct("user_id").alias("tgt_usr"),
        F.min("timestamp").alias("first_dt"),
        F.max("timestamp").alias("last_dt"),
    )
    .withColumnRenamed("ym", "period")
    .withColumnRenamed("threat_type", "thr_cls")
    .withColumnRenamed("ip_address", "src_addr")
    .orderBy("period", F.desc("evt_cnt"))
)

(threat_events.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_threat_events"))
print(f"gold_threat_events: {threat_events.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 6: ユーザーリスクスコア（gold_user_risk）
# MAGIC
# MAGIC ユーザーごとの異常行動スコア。ログイン失敗回数・BOT疑い・不正購入試行をスコアリング。

# COMMAND ----------

user_risk = (
    access_logs
    .groupBy("user_id")
    .agg(
        F.count("access_id").alias("acc_cnt"),
        F.sum(F.when(F.col("success") == 0, 1).otherwise(0)).alias("fail_cnt"),
        F.sum(F.when(F.col("threat_type") == "brute_force", 1).otherwise(0)).alias("bf_cnt"),
        F.sum(F.when(F.col("threat_type") == "bot", 1).otherwise(0)).alias("bot_cnt"),
        F.sum(F.when(F.col("threat_type") == "fraud", 1).otherwise(0)).alias("frd_cnt"),
        F.countDistinct("ip_address").alias("uniq_src"),
        F.max("timestamp").alias("last_ts"),
    )
    .withColumn("rsk_val",
        F.col("fail_cnt") * 1
        + F.col("bf_cnt") * 3
        + F.col("bot_cnt") * 5
        + F.col("frd_cnt") * 10
    )
    .withColumn("rsk_cat",
        F.when(F.col("rsk_val") >= 50, "critical")
         .when(F.col("rsk_val") >= 20, "high")
         .when(F.col("rsk_val") >= 5, "medium")
         .otherwise("low"))
    .withColumnRenamed("user_id", "usr_ref")
    .join(
        spark.table(f"{catalog}.{schema}.silver_users").select(
            F.col("user_id").alias("usr_ref"), "name", "age_group", "prefecture"
        ),
        "usr_ref",
        "left"
    )
    .orderBy(F.desc("rsk_val"))
)

(user_risk.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.{schema}.gold_user_risk"))
print(f"gold_user_risk: {user_risk.count()} rows")

# COMMAND ----------

print("\nTask 3 完了: 6つの Gold テーブルを作成しました")
print()
print("売上分析:        gold_monthly_sales, gold_customer_rfm, gold_cart_abandonment")
print("セキュリティ監視: gold_access_summary, gold_threat_events, gold_user_risk")