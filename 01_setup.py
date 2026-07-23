# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # セットアップ — 第4回 AIエージェント開発
# MAGIC
# MAGIC このノートブックを **最初に1回だけ** 実行してください。
# MAGIC 以下を自動で構築します。
# MAGIC
# MAGIC | # | 内容 | 所要時間 |
# MAGIC |---|---|---|
# MAGIC | 1 | CSVデータ生成 | 約30sec |
# MAGIC | 2 | パイプライン実行（Bronze→Silver→Gold） | 約2分 |
# MAGIC | 3 | テーブルコメント追加 | 約10sec |
# MAGIC | 4 | Genie Space 2つ自動作成 | 約10sec |
# MAGIC | 5 | 社内マニュアルPDFダウンロード | 約1分 |
# MAGIC
# MAGIC **合計所要時間:** 約3分

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.manuals")

print(f"Volume準備完了")
print(f"  raw_data: {volume_path}")
print(f"  manuals:  {manual_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: CSVデータ生成
# MAGIC
# MAGIC 以下は変更不要です。そのまま実行してください。

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

# -- ユーザーマスタ（1,000人）--

LAST_NAMES = ["佐藤","鈴木","高橋","田中","伊藤","渡辺","山本","中村","小林","加藤",
              "吉田","山田","佐々木","松本","井上","木村","林","斉藤","清水","山口"]
FIRST_NAMES_M = ["太郎","次郎","健太","翔太","大輝","拓海","陸","蓮","悠真","大和"]
FIRST_NAMES_F = ["花子","さくら","美咲","陽菜","結衣","凛","芽依","心春","杏","莉子"]
PREFECTURES = ["東京都","大阪府","神奈川県","愛知県","埼玉県","千葉県","福岡県","北海道","兵庫県","京都府"]
GENDERS = ["M", "F"]

users_data = []
for uid in range(1, 1001):
    gender = random.choice(GENDERS)
    name = random.choice(LAST_NAMES) + " " + random.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
    age = random.randint(18, 65)
    pref = random.choice(PREFECTURES)
    reg_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 700))
    email = f"user{uid}@example.com"
    users_data.append((uid, name, age, gender, pref, str(reg_date), email))

# -- 商品マスタ（80商品）--

CATEGORIES = {
    "電子機器":     [("スマートフォン",60000,35000),("ノートPC",90000,55000),("タブレット",45000,28000),
                     ("イヤホン",12000,6000),("スマートウォッチ",30000,18000),("モニター",35000,22000),
                     ("キーボード",8000,4000),("マウス",5000,2500),("USBハブ",3000,1500),("Webカメラ",7000,3500)],
    "ファッション": [("Tシャツ",3000,1200),("デニムパンツ",6000,2800),("スニーカー",8000,4000),
                     ("ジャケット",12000,6000),("ワンピース",7000,3200),("リュック",5000,2500),
                     ("キャップ",2500,1000),("サングラス",4000,1800),("ベルト",3500,1500),("ストール",4500,2000)],
    "食品":         [("コーヒー豆",1500,700),("チョコレート",800,350),("ナッツ詰合せ",1200,500),
                     ("オリーブオイル",2000,900),("パスタセット",1000,450),("はちみつ",1800,800),
                     ("お茶ギフト",2500,1100),("ドライフルーツ",1400,600),("グラノーラ",900,400),("ジャム",700,300)],
    "家具・インテリア": [("デスク",25000,15000),("チェア",18000,10000),("本棚",15000,9000),
                         ("ラグ",8000,4000),("クッション",3000,1200),("照明スタンド",6000,3000),
                         ("収納ボックス",4000,2000),("カーテン",5000,2500),("ミラー",7000,3500),("時計",4500,2200)],
    "スポーツ":     [("ランニングシューズ",10000,5000),("ヨガマット",3000,1500),("ダンベル",5000,2500),
                     ("プロテイン",4000,1800),("スポーツウェア",6000,3000),("水筒",2000,900),
                     ("サイクルグローブ",2500,1200),("テニスラケット",15000,8000),("バランスボール",3500,1700),("縄跳び",1500,600)],
    "本・メディア": [("ビジネス書",1500,700),("小説",800,350),("技術書",3000,1500),
                     ("漫画セット",4000,2000),("雑誌定期便",1200,600),("洋書",2000,900),
                     ("絵本",1000,450),("参考書",2500,1200),("写真集",3500,1800),("辞書",2800,1400)],
    "美容・健康":   [("化粧水",2500,1000),("日焼け止め",1500,600),("シャンプー",1800,800),
                     ("サプリメント",3000,1200),("フェイスマスク",800,350),("ハンドクリーム",1200,500),
                     ("美容液",4000,1800),("ボディソープ",1000,450),("歯ブラシセット",600,250),("アロマオイル",2000,900)],
    "ホビー・ゲーム": [("ボードゲーム",4000,2000),("プラモデル",3500,1800),("パズル",2000,900),
                       ("トレカパック",500,200),("フィギュア",6000,3000),("画材セット",5000,2500),
                       ("ラジコン",8000,4000),("ミニ四駆",1500,700),("手芸キット",3000,1400),("天体望遠鏡",15000,8000)],
}

products_data = []
pid = 1
for cat, items in CATEGORIES.items():
    for pname, price, cost in items:
        products_data.append((pid, pname, cat, price, cost))
        pid += 1

# -- 注文・セッションデータ --

CATEGORY_MULTIPLIER = {}
for cat in CATEGORIES:
    mults = []
    for m in range(24):
        if cat == "電子機器":
            mult = max(0.4, 1.0 - m * 0.03) if m >= 6 else 1.0
        elif cat == "ファッション":
            mult = 1.0 + m * 0.01
        else:
            mult = 1.0
        mults.append(mult)
    CATEGORY_MULTIPLIER[cat] = mults

CHANNELS = ["web", "mobile", "app"]
STATUSES = ["completed", "completed", "completed", "completed", "cancelled", "returned"]
teen20s_ids = {uid for uid, name, age, gender, pref, reg, email in users_data if 18 <= age <= 29}
BOT_USER_IDS = {991, 992, 993, 994, 995, 996, 997, 998}  # BOTユーザーは注文を生成しない
VIP_USER_ID = 42  # シナリオ用VIPユーザー（確実に優良顧客にする）

orders_data, items_data, sessions_data = [], [], []
oid, iid, sid = 1, 1, 1

for month_idx in range(24):
    year  = 2024 + month_idx // 12
    month = month_idx % 12 + 1
    days_in_month = 28 if month == 2 else 30 if month in (4,6,9,11) else 31
    cat_products = {cat: [(p[0], p[3], p[4]) for p in products_data if p[2] == cat] for cat in CATEGORIES}

    for cat in CATEGORIES:
        mult = CATEGORY_MULTIPLIER[cat][month_idx]
        n_orders = max(1, int(random.gauss(30, 5) * mult))
        prods = cat_products[cat]
        for _ in range(n_orders):
            uid = random.randint(1, 1000)
            if uid in BOT_USER_IDS:
                continue  # BOTユーザーは注文しない
            if uid in teen20s_ids:
                if month_idx >= 18:
                    if random.random() < 0.95: continue
                elif month_idx >= 12:
                    if random.random() < 0.75: continue
                elif month_idx >= 6:
                    if random.random() < 0.35: continue
            day = random.randint(1, days_in_month)
            odate = f"{year}-{month:02d}-{day:02d}"
            status = random.choice(STATUSES)
            channel = random.choice(CHANNELS)
            n_items = random.randint(1, 3)
            total = 0
            for _ in range(n_items):
                ppid, price, cost = random.choice(prods)
                qty = random.randint(1, 2)
                subtotal = price * qty
                total += subtotal
                items_data.append((iid, oid, ppid, qty, price, subtotal))
                iid += 1
            orders_data.append((oid, uid, odate, status, total, channel, cat))
            oid += 1

    for cat in CATEGORIES:
        mult = CATEGORY_MULTIPLIER[cat][month_idx]
        n_sessions = max(1, int(random.gauss(50, 10) * mult))
        for _ in range(n_sessions):
            uid = random.randint(1, 1000)
            ppid = random.choice(products_data)[0]
            day = random.randint(1, days_in_month)
            sdate = f"{year}-{month:02d}-{day:02d}"
            evt = random.choices(["view","add_to_cart","purchase","abandon"], weights=[40,25,20,15])[0]
            sessions_data.append((sid, uid, ppid, cat, evt, sdate))
            sid += 1

# -- アクセスログ --

NORMAL_IPS = [f"203.0.113.{i}" for i in range(1, 201)]
ATTACK_IPS = [f"198.51.100.{i}" for i in range(1, 11)]
BOT_IPS = [f"192.0.2.{i}" for i in range(1, 6)]
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120","Mozilla/5.0 (Macintosh; Intel Mac OS X) Safari/17","Mozilla/5.0 (iPhone; CPU iPhone OS 17) Mobile Safari","Mozilla/5.0 (Linux; Android 14) Chrome/120 Mobile"]
BOT_USER_AGENTS = ["python-requests/2.31","curl/8.4","Go-http-client/2.0"]

access_data = []
aid = 1
for month_idx in range(24):
    year  = 2024 + month_idx // 12
    month = month_idx % 12 + 1
    days_in_month = 28 if month == 2 else 30 if month in (4,6,9,11) else 31
    n_normal = random.randint(300, 500)
    for _ in range(n_normal):
        uid = random.randint(1, 1000)
        day = random.randint(1, days_in_month)
        hour = random.choices(range(24), weights=[1,1,1,1,1,2,5,8,10,10,10,10,10,10,10,8,8,6,5,4,3,2,1,1])[0]
        ts = f"{year}-{month:02d}-{day:02d} {hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        ip = random.choice(NORMAL_IPS)
        ua = random.choice(USER_AGENTS)
        success = 1 if random.random() < 0.95 else 0
        access_data.append((aid, uid, ts, ip, ua, success, "login", "normal"))
        aid += 1
    if month_idx >= 19: n_brute = random.randint(80, 150)
    elif month_idx >= 16: n_brute = random.randint(10, 30)
    else: n_brute = random.randint(0, 5)
    for _ in range(n_brute):
        uid = random.randint(1, 1000)
        day = random.randint(1, days_in_month)
        hour = random.choices(range(24), weights=[5,5,8,8,8,5,2,1,1,1,1,1,1,1,1,1,2,3,4,5,6,7,6,5])[0]
        ts = f"{year}-{month:02d}-{day:02d} {hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        ip = random.choice(ATTACK_IPS)
        ua = random.choice(USER_AGENTS)
        success = 1 if random.random() < 0.05 else 0
        access_data.append((aid, uid, ts, ip, ua, success, "login", "brute_force"))
        aid += 1
    if month_idx >= 17: n_bot = random.randint(50, 100)
    elif month_idx >= 12: n_bot = random.randint(5, 15)
    else: n_bot = random.randint(0, 3)
    for _ in range(n_bot):
        uid = random.choice([991, 992, 993, 994, 995, 996, 997, 998])
        day = random.randint(1, days_in_month)
        hour = random.randint(0, 23)
        ts = f"{year}-{month:02d}-{day:02d} {hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        ip = random.choice(BOT_IPS)
        ua = random.choice(BOT_USER_AGENTS)
        event = random.choice(["page_view", "page_view", "page_view", "add_to_cart"])
        access_data.append((aid, uid, ts, ip, ua, 1, event, "bot"))
        aid += 1
    if month_idx >= 20: n_fraud = random.randint(15, 30)
    elif month_idx >= 18: n_fraud = random.randint(3, 8)
    else: n_fraud = 0
    for _ in range(n_fraud):
        uid = random.randint(1, 1000)
        day = random.randint(1, days_in_month)
        hour = random.choice([2, 3, 4, 5])
        ts = f"{year}-{month:02d}-{day:02d} {hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        ip = random.choice(ATTACK_IPS[:5])
        ua = random.choice(USER_AGENTS)
        success = 1 if random.random() < 0.1 else 0
        access_data.append((aid, uid, ts, ip, ua, success, "purchase_attempt", "fraud"))
        aid += 1

# -- VIPユーザー42の注文を追加（確実に優良顧客にする）--
# 直近で高額・高頻度の注文を挿入
for m in range(24):
    year_v = 2024 + m // 12
    month_v = m % 12 + 1
    day_v = random.randint(1, 28)
    odate_v = f"{year_v}-{month_v:02d}-{day_v:02d}"
    cat_v = random.choice(list(CATEGORIES.keys()))
    prods_v = [(p[0], p[3], p[4]) for p in products_data if p[2] == cat_v]
    ppid_v, price_v, cost_v = random.choice(prods_v)
    qty_v = random.randint(1, 3)
    subtotal_v = price_v * qty_v
    orders_data.append((oid, VIP_USER_ID, odate_v, "completed", subtotal_v, "web", cat_v))
    items_data.append((iid, oid, ppid_v, qty_v, price_v, subtotal_v))
    oid += 1
    iid += 1

print(f"データ生成完了:")
print(f"  users={len(users_data)}, products={len(products_data)}, orders={len(orders_data)}")
print(f"  items={len(items_data)}, sessions={len(sessions_data)}, access_logs={len(access_data)}")

# COMMAND ----------

import csv, io

def write_csv_to_volume(volume_path, filename, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    path = f"/Volumes/{catalog}/{schema}/raw_data/{filename}"
    dbutils.fs.put(path, buf.getvalue(), overwrite=True)
    print(f"  {filename}: {len(rows):,} rows")

write_csv_to_volume(volume_path, "users.csv",
    ["user_id","name","age","gender","prefecture","registration_date","email"], users_data)
write_csv_to_volume(volume_path, "products.csv",
    ["product_id","product_name","category","price","cost"], products_data)
write_csv_to_volume(volume_path, "orders.csv",
    ["order_id","user_id","order_date","status","total_amount","channel","category"], orders_data)
write_csv_to_volume(volume_path, "order_items.csv",
    ["item_id","order_id","product_id","quantity","unit_price","subtotal"], items_data)
write_csv_to_volume(volume_path, "sessions.csv",
    ["session_id","user_id","product_id","category","event_type","session_date"], sessions_data)
write_csv_to_volume(volume_path, "access_logs.csv",
    ["access_id","user_id","timestamp","ip_address","user_agent","success","event_type","threat_type"], access_data)

print(f"\n✅ Step 1 完了: CSVファイルをVolumeに書き込み完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: パイプライン実行（Bronze → Silver → Gold）

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1: CSV → Bronze テーブル

# COMMAND ----------

# MAGIC %run ./02_ingest_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2: Bronze → Silver テーブル

# COMMAND ----------

# MAGIC %run ./03_transform_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3: Silver → Gold テーブル

# COMMAND ----------

# MAGIC %run ./04_create_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: テーブルコメント追加

# COMMAND ----------

# -- 売上分析テーブル（FQDN: {catalog}.{schema}.テーブル名）--
fqdn = f"{catalog}.{schema}"
comments = [
    f"COMMENT ON TABLE {fqdn}.gold_monthly_sales IS '月別・カテゴリ別の売上集計テーブル。completed（完了）注文のみを集計。ECサイトの売上分析に使用。'",
    f"ALTER TABLE {fqdn}.gold_monthly_sales ALTER COLUMN ym COMMENT '年月（yyyy-MM形式、例: 2024-01）'",
    f"ALTER TABLE {fqdn}.gold_monthly_sales ALTER COLUMN category COMMENT '商品カテゴリ（電子機器、ファッション、食品、家具・インテリア、スポーツ、本・メディア、美容・健康、ホビー・ゲーム）'",
    f"ALTER TABLE {fqdn}.gold_monthly_sales ALTER COLUMN sales_amount COMMENT '売上金額（日本円）。「売上」「revenue」と同義。'",
    f"ALTER TABLE {fqdn}.gold_monthly_sales ALTER COLUMN order_count COMMENT '注文件数'",
    f"ALTER TABLE {fqdn}.gold_monthly_sales ALTER COLUMN avg_order_value COMMENT '平均注文単価（日本円）'",

    f"COMMENT ON TABLE {fqdn}.gold_customer_rfm IS '顧客のRFM分析結果。Recency（最終購入からの日数）、Frequency（購入回数）、Monetary（累計購入金額）に基づくセグメント分類。'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN user_id COMMENT '顧客ID'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN age_group COMMENT '年齢層（10-20代、30代、40代、50代以上）'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN recency_days COMMENT '最終購入からの経過日数。小さいほど最近購入。'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN frequency COMMENT '購入回数（多いほどリピーター）'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN monetary COMMENT '累計購入金額（日本円）。LTVと同義。'",
    f"ALTER TABLE {fqdn}.gold_customer_rfm ALTER COLUMN segment COMMENT '顧客セグメント（優良顧客、成長顧客、休眠予備軍、離脱リスク）。「ゴールド会員」は優良顧客の社内呼称。'",

    f"COMMENT ON TABLE {fqdn}.gold_cart_abandonment IS '月別・カテゴリ別のカゴ落ち率。カートに入れたが購入しなかった割合。'",
    f"ALTER TABLE {fqdn}.gold_cart_abandonment ALTER COLUMN abandonment_rate COMMENT 'カゴ落ち率（%）。高いほど離脱が多い。「バスケット落ち」と同義。'",

    # -- セキュリティ監視テーブル --
    f"COMMENT ON TABLE {fqdn}.gold_access_summary IS '月別・脅威タイプ別のアクセス集計。ログイン成功/失敗、攻撃元IP数を確認できる。セキュリティ監視に使用。'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN period COMMENT '年月（yyyy-MM形式）'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN thr_cls COMMENT '脅威タイプ（normal=通常、brute_force=総当たり攻撃、bot=BOTアクセス、fraud=不正購入試行）'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN tz_band COMMENT '時間帯（深夜帯=2-5時、午前=6-11時、午後=12-17時、夜間=18-1時）'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN evt_cnt COMMENT 'イベント総数'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN succ_cnt COMMENT '成功したアクセス数'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN fail_cnt COMMENT '失敗したアクセス数。「失敗」「ログイン失敗」と同義。'",
    f"ALTER TABLE {fqdn}.gold_access_summary ALTER COLUMN fail_pct COMMENT '失敗率（%）。高いほど攻撃の可能性。'",

    f"COMMENT ON TABLE {fqdn}.gold_threat_events IS '脅威イベントの詳細。攻撃タイプ別・IP別のイベント数と対象ユーザー数。normalを除く異常アクセスのみ。'",
    f"ALTER TABLE {fqdn}.gold_threat_events ALTER COLUMN thr_cls COMMENT '脅威タイプ（brute_force=総当たり攻撃、bot=BOTアクセス、fraud=不正購入試行）'",
    f"ALTER TABLE {fqdn}.gold_threat_events ALTER COLUMN src_addr COMMENT '攻撃元IPアドレス'",
    f"ALTER TABLE {fqdn}.gold_threat_events ALTER COLUMN evt_cnt COMMENT 'イベント件数。「攻撃回数」と同義。'",
    f"ALTER TABLE {fqdn}.gold_threat_events ALTER COLUMN tgt_usr COMMENT '攻撃対象となったユニークユーザー数'",

    f"COMMENT ON TABLE {fqdn}.gold_user_risk IS 'ユーザーごとのリスクスコア。ログイン失敗・BOT活動・不正購入試行を重み付けしてスコアリング。高スコアほど要注意。'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN usr_ref COMMENT 'ユーザーID'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN fail_cnt COMMENT 'ログイン失敗回数'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN bf_cnt COMMENT '総当たり攻撃に関連したイベント数'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN bot_cnt COMMENT 'BOT活動イベント数'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN frd_cnt COMMENT '不正購入試行回数'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN rsk_val COMMENT 'リスクスコア（高いほど危険）。失敗×1 + 総当たり×3 + BOT×5 + 不正購入×10 で算出。'",
    f"ALTER TABLE {fqdn}.gold_user_risk ALTER COLUMN rsk_cat COMMENT 'リスクレベル（critical=50以上、high=20以上、medium=5以上、low=5未満）'",
]

for sql in comments:
    try:
        spark.sql(sql)
    except Exception as e:
        print(f"  WARN: {sql[:60]}... → {e}")

print(f"✅ Step 3 完了: テーブルコメント追加完了（{len(comments)}件）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Genie Space 自動作成

# COMMAND ----------

import requests, json, uuid

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# 既存Genie Spaceを検索するヘルパー
def find_existing_genie_space(title_contains):
    try:
        r = requests.get(f"https://{host}/api/2.0/genie/spaces", headers=headers)
        if r.ok:
            for s in r.json().get("spaces", []):
                if title_contains in s.get("title", ""):
                    return s.get("space_id")
    except:
        pass
    return None

# 利用可能なSQL Warehouseを取得
try:
    wh_resp = requests.get(f"https://{host}/api/2.0/sql/warehouses", headers=headers)
    warehouses = [w for w in wh_resp.json().get("warehouses", []) if w["state"] in ("RUNNING", "STOPPED")]
    wh_id = warehouses[0]["id"] if warehouses else ""
    if wh_id:
        print(f"使用するSQL Warehouse: {warehouses[0]['name']} ({wh_id})")
    else:
        raise Exception("not found")
except:
    wh_id = input("⚠️ Warehouse自動取得に失敗。Warehouse IDを入力: ").strip()

# --- 売上分析 Genie Space ---
sales_serialized = {
    "version": 2,
    "config": {"sample_questions": [
        {"id": uuid.uuid4().hex, "question": ["電子機器の月別売上推移を教えて"]},
        {"id": uuid.uuid4().hex, "question": ["離脱リスクが多い年代は？"]},
        {"id": uuid.uuid4().hex, "question": ["カゴ落ち率が一番高いカテゴリは？"]},
    ]},
    "data_sources": {
        "tables": sorted([
            {"identifier": f"{catalog}.{schema}.gold_cart_abandonment"},
            {"identifier": f"{catalog}.{schema}.gold_customer_rfm"},
            {"identifier": f"{catalog}.{schema}.gold_monthly_sales"},
        ], key=lambda t: t["identifier"]),
        "metric_views": []
    },
    "instructions": {
        "text_instructions": [{
            "id": uuid.uuid4().hex,
            "content": ["あなたはECサイトのデータアナリストです。日本語で回答してください。", "「売上」と聞かれたら gold_monthly_sales の sales_amount を使うこと", "金額は日本円で表示し、3桁カンマ区切りにすること", "年月の表示は yyyy年MM月 形式にすること", "「離脱リスク」は gold_customer_rfm の segment='離脱リスク' を指す", "「サイレント離脱」= 離脱リスク（segment='離脱リスク'）かつ90日以上購入なし（recency_days > 90）の顧客", "「ゴールド会員」= 優良顧客セグメント（segment='優良顧客'）", "カテゴリ名は日本語をそのまま使うこと"]
        }],
        "example_question_sqls": [],
        "join_specs": [],
        "sql_snippets": {"filters": [], "expressions": [], "measures": []}
    },
    "benchmarks": {}
}

sales_title = f"売上分析エージェント_{schema}"
sales_space_id = find_existing_genie_space(sales_title)
if sales_space_id:
    print(f"✅ 売上分析 Genie Space（既存）: https://{host}/genie/rooms/{sales_space_id}")
else:
    resp = requests.post(f"https://{host}/api/2.0/genie/spaces", headers=headers, json={
        "title": sales_title,
        "description": "ECサイトの売上・顧客・カゴ落ちデータを分析できます。",
        "warehouse_id": wh_id,
        "serialized_space": json.dumps(sales_serialized)
    })
    if resp.ok:
        sales_space_id = resp.json().get("space_id", "")
        print(f"✅ 売上分析 Genie Space: https://{host}/genie/rooms/{sales_space_id}")
    else:
        print(f"❌ 売上分析 Genie Space エラー: {resp.status_code} {resp.text}")
        sales_space_id = ""

# --- セキュリティ監視 Genie Space ---
sec_serialized = {
    "version": 2,
    "config": {"sample_questions": [
        {"id": uuid.uuid4().hex, "question": ["2025年10月以降の脅威イベントの推移を教えて"]},
        {"id": uuid.uuid4().hex, "question": ["リスクスコアが高いユーザー上位10人は？"]},
        {"id": uuid.uuid4().hex, "question": ["深夜帯の不審アクセスの状況は？"]},
    ]},
    "data_sources": {
        "tables": sorted([
            {"identifier": f"{catalog}.{schema}.gold_access_summary"},
            {"identifier": f"{catalog}.{schema}.gold_threat_events"},
            {"identifier": f"{catalog}.{schema}.gold_user_risk"},
        ], key=lambda t: t["identifier"]),
        "metric_views": []
    },
    "instructions": {
        "text_instructions": [{
            "id": uuid.uuid4().hex,
            "content": ["あなたはセキュリティアナリストです。日本語で回答してください。", "脅威タイプは日本語で表示すること（brute_force=総当たり攻撃、bot=BOTアクセス、fraud=不正購入試行、normal=通常）", "リスクレベルは日本語で表示すること（critical=危険、high=高、medium=中、low=低）", "時間帯は日本語をそのまま使うこと（深夜帯、午前、午後、夜間）", "件数は3桁カンマ区切りで表示すること"]
        }],
        "example_question_sqls": [],
        "join_specs": [],
        "sql_snippets": {"filters": [], "expressions": [], "measures": []}
    },
    "benchmarks": {}
}

sec_title = f"セキュリティ監視エージェント_{schema}"
sec_space_id = find_existing_genie_space(sec_title)
if sec_space_id:
    print(f"✅ セキュリティ監視 Genie Space（既存）: https://{host}/genie/rooms/{sec_space_id}")
else:
    resp = requests.post(f"https://{host}/api/2.0/genie/spaces", headers=headers, json={
        "title": sec_title,
        "description": "ECサイトのセキュリティ監視用。アクセスログ・脅威イベント・ユーザーリスクを分析。",
        "warehouse_id": wh_id,
        "serialized_space": json.dumps(sec_serialized)
    })
    if resp.ok:
        sec_space_id = resp.json().get("space_id", "")
        print(f"✅ セキュリティ監視 Genie Space: https://{host}/genie/rooms/{sec_space_id}")
    else:
        print(f"❌ セキュリティ監視 Genie Space エラー: {resp.status_code} {resp.text}")
        sec_space_id = ""

if sales_space_id and sec_space_id:
    print(f"\n✅ Step 4 完了: Genie Space 2つ作成完了")
elif sales_space_id or sec_space_id:
    print(f"\n⚠️ Step 4 部分完了: 一部のGenie Spaceでエラーが発生しました。上のエラーを確認してください。")
else:
    raise RuntimeError("Step 4 失敗: Genie Spaceが1つも作成できませんでした。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: 社内マニュアルPDFのダウンロード

# COMMAND ----------

import os
import shutil

# Databricksユーザー名から自動取得
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
MANUALS_SRC_DIR = f"/Workspace/Users/{current_user}/EC-site-demo/session4/manuals"
MANUALS_DST_DIR = f"/Volumes/{catalog}/{schema}/manuals"

PDF_FILES = [
    ("01_customer_policy.pdf", "01_顧客対応ポリシー.pdf"),
    ("02_account_management.pdf", "02_アカウント管理ガイドライン.pdf"),
    ("03_marketing_templates.pdf", "03_マーケティング施策テンプレート集.pdf"),
    ("04_incident_bcp.pdf", "04_インシデント対応BCPマニュアル.pdf"),
    ("05_return_policy.pdf", "05_返品返金ポリシー.pdf"),
    ("06_operations_handbook.pdf", "06_ECサイト運用ハンドブック.pdf"),
    ("07_logistics_manual.pdf", "07_配送物流管理マニュアル.pdf"),
    ("08_privacy_policy.pdf", "08_個人情報保護規程.pdf"),
    ("09_onboarding_guide.pdf", "09_新入社員オンボーディングガイド.pdf"),
    ("10_product_photo_guide.pdf", "10_商品撮影掲載ガイドライン.pdf"),
]

os.makedirs(MANUALS_DST_DIR, exist_ok=True)
success_count = 0
for src_name, dst_name in PDF_FILES:
    src_path = os.path.join(MANUALS_SRC_DIR, dst_name)
    dst_path = os.path.join(MANUALS_DST_DIR, dst_name)
    try:
        shutil.copyfile(src_path, dst_path)
        print(f"  ✅ {dst_name}")
        success_count += 1
    except Exception as e:
        print(f"  ❌ {dst_name}: {e}")

if success_count == len(PDF_FILES):
    print(f"\n✅ Step 5 完了: マニュアルPDF {success_count}件をダウンロード")
else:
    print(f"\n⚠️ Step 5 部分完了: {success_count}/{len(PDF_FILES)}件のみダウンロード成功")
print(f"保存先: {MANUALS_DST_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## セットアップ完了！
# MAGIC
# MAGIC | リソース | 状態 |
# MAGIC |---|---|
# MAGIC | CSVデータ（6ファイル） | ✅ Volume に作成済み |
# MAGIC | パイプライン | ✅ 実行完了（Bronze→Silver→Gold） |
# MAGIC | テーブルコメント | ✅ 全テーブルに追加済み |
# MAGIC | Genie Space（売上分析） | ✅ 作成済み |
# MAGIC | Genie Space（セキュリティ監視） | ✅ 作成済み |
# MAGIC | 社内マニュアルPDF（10件） | ✅ Volume にダウンロード済み |
# MAGIC
# MAGIC ### 次のステップ
# MAGIC
# MAGIC `10_knowledge_assistant` を開いて、Knowledge Assistantを作成します。

# COMMAND ----------


