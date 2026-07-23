# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: トレーシングと評価
# MAGIC
# MAGIC Supervisor Agentの「思考過程」をトレースで可視化し、品質改善につなげる体験をします。
# MAGIC
# MAGIC | 機能 | 内容 |
# MAGIC |---|---|
# MAGIC | トレース | SVAがどのサブエージェントを呼び、どう統合したかを可視化 |
# MAGIC | LLM Judge | AIが回答品質を自動スコアリング（合格/失敗） |
# MAGIC
# MAGIC **所要時間:** 約15分

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4-1: SVAのエクスペリメントを開く
# MAGIC
# MAGIC Step 3の統合テストで既にSVAにいくつか質問を投げています。
# MAGIC そのトレースを確認しましょう。
# MAGIC
# MAGIC 1. SVAのエージェント画面 → 上部の **「エクスペリメント」** をクリック
# MAGIC 2. 左サイドバーの **「トレース」** をクリック
# MAGIC 3. 統合テストで投げた質問のトレースが一覧表示されます
# MAGIC
# MAGIC > トレースが表示されない場合は、ビルドタブに戻ってシナリオ1の質問を再度投げてください。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4-2: トレースの中身を読み解く
# MAGIC
# MAGIC シナリオ1（ユーザーID 42）のトレースをクリックして開いてください。
# MAGIC
# MAGIC ### 見るポイント
# MAGIC
# MAGIC | 観点 | 確認すること |
# MAGIC |---|---|
# MAGIC | **呼び出されたサブエージェント** | 3つ全部？それとも一部だけ？ |
# MAGIC | **呼び出し順序** | 並列に呼ばれた？直列？ |
# MAGIC | **各サブエージェントのレイテンシー** | どれが一番遅い？ボトルネックは？ |
# MAGIC | **統合プロセス** | サブエージェントの回答をどう組み合わせて最終回答にしたか |
# MAGIC
# MAGIC ### トレースの構造（期待）
# MAGIC
# MAGIC ```
# MAGIC SVA（Supervisor Agent）
# MAGIC  ├── 売上分析 Genie Space → ユーザー42のLTV・セグメント取得
# MAGIC  ├── セキュリティ監視 Genie Space → ユーザー42のリスクスコア取得
# MAGIC  ├── ナレッジアシスタント → VIP顧客への補償ポリシー検索
# MAGIC  └── 統合・回答生成 → 3つの結果を統合して最終判断
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4-3: LLM Judge を作成して評価する
# MAGIC
# MAGIC エクスペリメント画面からLLM Judgeを作成し、トレースを自動評価します。
# MAGIC **コード不要、GUI操作のみ** で実行できます。
# MAGIC
# MAGIC ### LLM Judge を作成する
# MAGIC
# MAGIC 1. 左サイドバーの **「審査」** をクリック
# MAGIC 2. **「+ 新しいLLM判定」** をクリック
# MAGIC 3. 以下を設定:
# MAGIC    - **範囲**: トレース（デフォルトのまま）
# MAGIC    - **名前**: `response_quality`
# MAGIC    - **評価基準** を展開 → **「+ カスタム判定」** ドロップダウンから **「クエリーとの関連性」** を選択
# MAGIC 4. **自動評価** を展開 → **「今後のすべてのトレースで実行」** がONになっていることを確認
# MAGIC
# MAGIC > **「+ カスタム判定」** ドロップダウンには多数の組み込みJudgeがあります:
# MAGIC >
# MAGIC > | Judge | 観点 |
# MAGIC > |---|---|
# MAGIC > | クエリーとの関連性 | ユーザーの質問に直接回答しているか |
# MAGIC > | 安全性 | 有害コンテンツを含んでいないか |
# MAGIC > | ツール呼び出しの正確性 | サブエージェントの呼び出しが適切か |
# MAGIC > | 完全性 | プロンプトの要望にすべて対応しているか |
# MAGIC >
# MAGIC > 余裕があれば **「安全性」** や **「ツール呼び出しの正確性」** も追加してみましょう。
# MAGIC
# MAGIC 5. **「判定を作成」** をクリックして保存
# MAGIC
# MAGIC ### 既存トレースに対して判定を実行する
# MAGIC
# MAGIC 1. 審査一覧に戻り、作成した判定の **「編集」** をクリック
# MAGIC 2. 右パネルの **「トレースを選択する」** をクリック
# MAGIC 3. ポップアップで統合テストのトレースを全て選択 → **「選択 (n)」**
# MAGIC 4. **「ラン判定」** をクリック → LLM Judgeが各トレースを即時評価します
# MAGIC 5. 右パネルに結果（合格 / いいえ）と **判定理由** が表示されます
# MAGIC 6. **「前へ」「次のページ」** で各トレースの結果を確認
# MAGIC 7. **「保存」** をクリック
# MAGIC
# MAGIC ### トレース一覧で結果を確認する
# MAGIC
# MAGIC 1. 左サイドバーの **「トレース」** に戻る
# MAGIC 2. 右端の列に判定結果（合格 / 失敗 / 空値）が表示されます（されない場合は、更新ボタンを押す）
# MAGIC 3. 合格率が上部にパーセンテージで表示されます
# MAGIC
# MAGIC > **自動評価について:** 「今後のすべてのトレースで実行」をONにすると、
# MAGIC > 今後の新規トレースはバックグラウンドで自動評価されます（初回処理に15〜20分かかります）。
# MAGIC > ハンズオンでは上記の **「ラン判定」で即時実行** する方法を使います。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 参考: 手動定期評価の仕組み
# MAGIC
# MAGIC ここからは今回実行しませんが、本番運用で使うエージェント品質管理の機能を紹介します。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 参考①: 評価ラン（リグレッションテスト）
# MAGIC
# MAGIC エージェントの指示やサブエージェントを変更した際に、**同じ質問セットで品質が劣化していないか**を確認するための仕組みです。
# MAGIC
# MAGIC #### 1. 評価データセットを作成する（GUI）
# MAGIC
# MAGIC 1. 左サイドバーの **「トレース」** をクリック
# MAGIC 2. テストに使いたいトレースの **チェックボックス** を選択
# MAGIC 3. **「アクション」** → **「評価データセットに追加」**
# MAGIC 4. 新しいデータセットを作成（カタログ/スキーマ/名前を指定）
# MAGIC 5. **「エクスポート」** → **「完了」**
# MAGIC
# MAGIC 作成したデータセットは左サイドバーの **「データセット」** で確認できます。
# MAGIC
# MAGIC #### 2. 評価ランを実行する（コード）
# MAGIC
# MAGIC 評価データセットに対してLLM Judgeを一括実行するには、ノートブックでコードを実行します:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC from mlflow.genai.scorers import RelevanceToQuery, Safety
# MAGIC
# MAGIC eval_dataset = mlflow.genai.datasets.get_dataset(
# MAGIC     uc_table_name="catalog.schema.sva_eval_dataset"
# MAGIC )
# MAGIC
# MAGIC eval_results = mlflow.genai.evaluate(
# MAGIC     data=eval_dataset,
# MAGIC     scorers=[RelevanceToQuery(), Safety()],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### 3. 結果を比較する（GUI）
# MAGIC
# MAGIC 1. 左サイドバーの **「評価ラン」** をクリック
# MAGIC 2. 各ランのPass/Fail率が一覧表示されます
# MAGIC 3. 複数の評価ランを選択 → **「アクション」** → **「比較」** でバージョン間の品質推移を確認
# MAGIC
# MAGIC > **活用シーン:** エージェントの指示を変更するたびに評価ランを実行し、
# MAGIC > 「変更前 vs 変更後」の品質を定量比較します。

# COMMAND ----------

# MAGIC %pip install mlflow "typing_extensions>=4.6.0" --ignore-installed -q
# MAGIC
# MAGIC import mlflow
# MAGIC from mlflow.genai.scorers import (
# MAGIC     Safety,
# MAGIC     RelevanceToQuery,
# MAGIC     Guidelines,
# MAGIC )
# MAGIC from mlflow.genai import evaluate
# MAGIC
# MAGIC mlflow.set_experiment(experiment_id="2993680828284129")
# MAGIC
# MAGIC # Step 1: Define evaluation dataset
# MAGIC eval_dataset = [{
# MAGIC   "inputs": {
# MAGIC     "query": "What is MLflow?",
# MAGIC   }
# MAGIC }]
# MAGIC
# MAGIC # Step 2: Define predict_fn
# MAGIC def predict(query, **kwargs):
# MAGIC   return query + " an answer"
# MAGIC
# MAGIC # Step 3: Run evaluation
# MAGIC evaluate(
# MAGIC   data=eval_dataset,
# MAGIC   predict_fn=predict,
# MAGIC   scorers=[
# MAGIC     Safety(),
# MAGIC     RelevanceToQuery(),
# MAGIC     Guidelines(name="conciseness", guidelines="Responses must be concise."),
# MAGIC   ],
# MAGIC )
# MAGIC
# MAGIC # Results will appear back in this UI

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 参考②: ラベル付けセッション（人間による審査）
# MAGIC
# MAGIC ドメインエキスパート（業務の専門家）にエージェントの回答を審査してもらう仕組みです。
# MAGIC
# MAGIC #### 1. ラベリングスキーマを作成する
# MAGIC
# MAGIC ラベリングスキーマは「レビュアーに何を聞くか」を定義するテンプレートです。
# MAGIC
# MAGIC 1. 左サイドバーの **「ラベリングスキーマ」** をクリック
# MAGIC 2. **「ラベルスキーマを追加」** をクリック
# MAGIC 3. 以下を設定:
# MAGIC    - **名前**: 例 `response_accuracy`
# MAGIC    - **タイプ**: フィードバック（主観評価）or エクスペクテーション（正解データ）
# MAGIC    - **入力タイプ**: カテゴリ選択 / フリーテキスト / 数値範囲 / 複数テキスト 等
# MAGIC 4. プレビューで表示を確認 → **「保存」**
# MAGIC
# MAGIC 組み込みスキーマも用意されています:
# MAGIC
# MAGIC | スキーマ | タイプ | 用途 |
# MAGIC |---|---|---|
# MAGIC | EXPECTED_RESPONSE | エクスペクテーション | 期待される正解回答を記録 |
# MAGIC | EXPECTED_FACTS | エクスペクテーション | 回答に含まれるべき事実のリスト |
# MAGIC | GUIDELINES | エクスペクテーション | 回答が従うべきガイドライン |
# MAGIC
# MAGIC #### 2. ラベル付けセッションを作成する
# MAGIC
# MAGIC 1. 左サイドバーの **「ラベル付けセッション」** をクリック
# MAGIC 2. **「セッションを作成」** → セッション名を入力
# MAGIC 3. 使用するラベリングスキーマを選択
# MAGIC 4. **「作成」**
# MAGIC
# MAGIC #### 3. トレースを追加して審査する
# MAGIC
# MAGIC 1. **「トレース」** に戻り、レビュー対象のトレースを選択
# MAGIC 2. **「アクション」** → **「ラベル付けセッションに追加」**
# MAGIC 3. セッションを開くと、レビュアーが各トレースに対してスキーマに沿ったフィードバックを入力できます
# MAGIC 4. 審査結果は評価データセットに同期可能（`session.sync(to_dataset=...)`）
# MAGIC
# MAGIC > **活用シーン:** リリース前にドメインエキスパートが回答品質を最終確認する。
# MAGIC > セッションを共有してチームで分担してレビューできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 参考③: レイテンシーの分析
# MAGIC
# MAGIC トレース一覧の **「実行時間」** 列でパフォーマンスを確認できます。
# MAGIC
# MAGIC | サブエージェント | 予想されるレイテンシー | 理由 |
# MAGIC |---|---|---|
# MAGIC | Genie Space（売上） | 数秒〜十数秒 | SQL生成 + Warehouse実行 |
# MAGIC | Genie Space（セキュリティ） | 数秒〜十数秒 | SQL生成 + Warehouse実行 |
# MAGIC | Knowledge Assistant | 数秒 | Vector Search + LLM生成 |
# MAGIC
# MAGIC > 本番運用では、レイテンシーの大きいサブエージェントを特定し、
# MAGIC > SQLの最適化やインデックスの改善でパフォーマンスを向上させます。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC | やったこと | 学び |
# MAGIC |---|---|
# MAGIC | SVAのトレース確認 | どのサブエージェントが呼ばれたか、処理の流れを可視化 |
# MAGIC | LLM Judge 作成・実行 | AIが回答品質を自動スコアリング。GUIのみで完結 |
# MAGIC | 自動評価 | 新規トレースを自動で評価し続ける仕組み |
# MAGIC
# MAGIC ### LLMOpsの全体像
# MAGIC
# MAGIC | フェーズ | 手法 | 今回の体験 |
# MAGIC |---|---|---|
# MAGIC | **開発時** | トレース確認 + LLM Judge | 体験済み |
# MAGIC | **リリース前** | 評価ラン（リグレッションテスト） | 参考① |
# MAGIC | **リリース前** | ラベル付けセッション（人間の審査） | 参考② |
# MAGIC | **本番運用** | 自動評価（継続モニタリング） | 設定済み |
# MAGIC
# MAGIC > **これがLLMOpsです。**
# MAGIC > 「トレースで可視化 → LLM Judgeで評価 → 問題を特定 → 改善 → 再評価」
# MAGIC > このサイクルを、開発から本番運用まで一貫して回せるのがDatabricksの強みです。
# MAGIC
# MAGIC 次のステップ: `99_summary_cleanup` でまとめとクリーンアップを行います。