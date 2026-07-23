# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Step 2: Knowledge Assistant（KA）の作成
# MAGIC
# MAGIC 社内マニュアルPDFを知識ソースとして、質問応答ができるRAGエージェントを構築します。
# MAGIC
# MAGIC | 項目 | 内容 |
# MAGIC |---|---|
# MAGIC | 知識ソース | 社内マニュアルPDF 10文書（Volume に配置済み） |
# MAGIC | 機能 | 自然言語で質問 → PDF内の該当箇所を引用して回答 |
# MAGIC | 作成方法 | **ノーコード（UI操作のみ）** |
# MAGIC
# MAGIC **所要時間:** 約20分

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前確認: マニュアルPDFがVolumeにあること

# COMMAND ----------

files = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/manuals/")
print(f"マニュアルPDF一覧（{manual_path}）:\n")
for f in files:
    print(f"  {f.name}  ({f.size:,} bytes)")

assert len([f for f in files if f.name.endswith(".pdf")]) >= 10, "❌ PDFが10件未満です。01_setupを先に実行してください。"
print(f"\n✅ {len(files)}件のPDFを確認しました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2-1: Knowledge Assistant を作成（UI操作）
# MAGIC
# MAGIC ### 手順
# MAGIC
# MAGIC 1. 左サイドバー → **エージェント** → 右上の **エージェントを作成** → **ナレッジアシスタント**
# MAGIC 2. 以下のセルで表示される内容を、一画面でまとめて入力します:

# COMMAND ----------

print(f"""=== 基本情報 ===

名前: ecsite_knowledge_{schema}

説明: ECサイト「ブリックスマート」の社内マニュアルに基づいて質問に回答するナレッジアシスタントです。

=== ナレッジソースを設定 ===

タイプ: ボリューム内ファイル
ソース（ツリーから選択）: /Volumes/{catalog}/{schema}/manuals
名前: ECサイト運用ナレッジ
コンテンツを説明: ECサイト「ブリックスマート」の社内マニュアル10文書。顧客対応ポリシー、アカウント管理ガイドライン、マーケティング施策テンプレート、インシデント対応BCP、返品返金ポリシー、運用ハンドブック、配送物流管理、個人情報保護規程、新入社員オンボーディングガイド、商品撮影掲載ガイドラインを含む。
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **エージェントを作成** をクリック → 同期が開始されます
# MAGIC 4. 同期が完了するまで少し待ちます（通常15分程度）
# MAGIC
# MAGIC > 同期中は「インデックス作成中...」と表示されます。
# MAGIC > 完了するとチャットが可能になります。
# MAGIC
# MAGIC ---
# MAGIC ## Step 2-2: Knowledge Assistant をテスト
# MAGIC
# MAGIC チャットパネルで以下の質問を試してみましょう。
# MAGIC
# MAGIC ### 基本質問（単一文書から回答）
# MAGIC
# MAGIC | # | 質問 | 回答に使われるPDF | 期待される回答 |
# MAGIC |---|---|---|---|
# MAGIC | 1 | VIP顧客への特別補償はどのようなものがありますか？ | 01_顧客対応ポリシー | 5,000円ポイント, 特別クーポン, 役員名義お詫びメール等 |
# MAGIC | 2 | アカウント凍結の基準を教えてください | 02_アカウント管理ガイドライン | リスクスコア50以上は即凍結、20-49は審査等 |
# MAGIC | 3 | 電子機器の返品期限は？ | 05_返品返金ポリシー | 到着後14日以内（開封後は不可） |
# MAGIC
# MAGIC ### 横断質問（複数文書から回答）
# MAGIC
# MAGIC | # | 質問 | 回答に使われるPDF | 期待される回答 |
# MAGIC |---|---|---|---|
# MAGIC | 4 | BOTが検知された場合の対応手順を最初から最後まで教えて | 02 + 04 | 検知→確認→遮断→凍結→在庫復元→正規顧客救済 |
# MAGIC | 5 | システム障害で10人以上に影響が出た場合の救済施策は？ | 01 + 03 | 一括救済キャンペーン（TierA/TierB）+ テンプレートB-4 |
# MAGIC
# MAGIC > **ポイント**: 回答の下に **「ソースを表示」** をクリックすると、
# MAGIC > どのPDFのどの箇所を引用したかが表示されます。
# MAGIC
# MAGIC ---
# MAGIC ## Step 2-3: トレーシングを確認
# MAGIC
# MAGIC テスト質問の後、チャットパネルで:
# MAGIC
# MAGIC 1. 回答の下にある **「トレースを表示」** をクリック
# MAGIC 2. KAがどのようにPDFを検索し、回答を生成したかのトレースが表示されます
# MAGIC 3. **「ソースを表示」** でどのPDFが引用されたかも確認できます
# MAGIC
# MAGIC > トレーシングは LLMOps の基本です。
# MAGIC > エージェントが「なぜその回答をしたか」を追跡できることで、
# MAGIC > 品質の改善やデバッグが可能になります。
# MAGIC
# MAGIC ---
# MAGIC ## 次のステップ
# MAGIC
# MAGIC `20_supervisor_agent` を開いて、Supervisor Agent を作成します。