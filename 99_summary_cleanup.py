# Databricks notebook source
# MAGIC %md
# MAGIC # まとめ & クリーンアップ

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 今日作ったもの
# MAGIC
# MAGIC | リソース | 役割 |
# MAGIC |---|---|
# MAGIC | **Genie Space（売上分析）** | 構造データ（テーブル）に自然言語で質問 |
# MAGIC | **Genie Space（セキュリティ監視）** | 構造データ（アクセスログ）に自然言語で質問 |
# MAGIC | **Knowledge Assistant** | 非構造データ（PDF）からRAGで回答 |
# MAGIC | **Supervisor Agent** | 上記3つを束ねて、組織横断的な判断を支援 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 全4回の振り返り
# MAGIC
# MAGIC | 回 | テーマ | 学んだこと |
# MAGIC |---|---|---|
# MAGIC | 第1回 | 全体像 | Databricksプラットフォームの概要 |
# MAGIC | 第2回 | 基礎 | ノートブック、テーブル、Genie Code |
# MAGIC | 第3回 | 自動化 | Lakeflow Jobs、AI/BI Dashboard、Genie Space |
# MAGIC | **第4回** | **AIエージェント** | **Knowledge Assistant、Supervisor Agent、トレーシング、LLM Judge** |
# MAGIC
# MAGIC ### キーメッセージ
# MAGIC
# MAGIC - **データの民主化**: 構造データ（SQL）も非構造データ（PDF）も、AIエージェントを通じて誰もがアクセス可能に
# MAGIC - **マルチエージェント**: 専門家エージェントが連携し、人間が気づかなかったインサイトや判断材料を提示
# MAGIC - **ガバナンス**: Unity Catalogで全データ・全エージェントを一元管理。アクセス制御・監査・トレーシング

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## クリーンアップ（ハンズオン終了後）
# MAGIC
# MAGIC ハンズオンで作成したエージェントのエンドポイントは、放置すると課金が継続する可能性があります。
# MAGIC **ハンズオン終了後、以下の手順で不要なリソースを削除してください。**
# MAGIC
# MAGIC ### 削除が必要なもの
# MAGIC
# MAGIC | リソース | 削除方法 | 理由 |
# MAGIC |---|---|---|
# MAGIC | **Supervisor Agent** | エージェント → 該当SVAの「…」→ 削除 | サービングエンドポイント停止 |
# MAGIC | **Knowledge Assistant** | エージェント → 該当KAの「…」→ 削除 | Vector Search Index + エンドポイント停止 |
# MAGIC
# MAGIC ### 削除しなくてよいもの
# MAGIC
# MAGIC | リソース | 理由 |
# MAGIC |---|---|
# MAGIC | Genie Space | 質問時のみWarehouse起動。放置OK |
# MAGIC | テーブル（Gold等） | ストレージのみ。微小 |
# MAGIC | Volume（CSV/PDF） | ストレージのみ。微小 |
# MAGIC
# MAGIC ### 手順
# MAGIC
# MAGIC 1. 左サイドバー → **エージェント**
# MAGIC 2. 自分が作成した **ecsite_supervisor_xxx** を見つける
# MAGIC 3. 右上の「**…**」→ **削除** → 確認
# MAGIC 4. 同様に **ecsite_knowledge_xxx** も削除
# MAGIC 5. 確認: 左サイドバー → **サービング** で、関連エンドポイントが消えていることを確認
# MAGIC
# MAGIC > **Note:** KAを削除すると、関連するVector Searchエンドポイントも24時間後に自動停止します。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC お疲れ様でした！
