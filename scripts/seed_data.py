"""
seed_data.py — Generate realistic sample data and write to HDFS as Parquet.

Generates:
  - dim_user:        1,000 simulated users with reward tiers and signup dates
  - dim_asset:       10 crypto assets (BTC, ETH, BNB, SOL, etc.)
  - dim_date:        365 days of date dimension
  - raw prices:      Daily OHLCV prices per asset (365 days × 10 assets)
  - raw transactions: ~50,000 buy/sell trades across users
  - raw rewards:     ~20,000 cashback + staking reward events

Usage:
  spark-submit --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/scripts/seed_data.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, DateType, LongType
)
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

# =============================================================================
# Constants
# =============================================================================

NUM_USERS = 1000
NUM_DAYS = 365
START_DATE = datetime(2024, 1, 1)
HDFS_BASE = "hdfs://namenode:9000/data"

ASSETS = [
    {"symbol": "BTC",  "name": "Bitcoin",       "category": "layer1", "base_price": 42000.0},
    {"symbol": "ETH",  "name": "Ethereum",      "category": "layer1", "base_price": 2200.0},
    {"symbol": "BNB",  "name": "BNB",           "category": "layer1", "base_price": 310.0},
    {"symbol": "SOL",  "name": "Solana",        "category": "layer1", "base_price": 95.0},
    {"symbol": "ADA",  "name": "Cardano",       "category": "layer1", "base_price": 0.55},
    {"symbol": "DOT",  "name": "Polkadot",      "category": "layer1", "base_price": 7.80},
    {"symbol": "AVAX", "name": "Avalanche",     "category": "layer1", "base_price": 35.0},
    {"symbol": "LINK", "name": "Chainlink",     "category": "oracle",  "base_price": 14.50},
    {"symbol": "MATIC","name": "Polygon",       "category": "layer2", "base_price": 0.85},
    {"symbol": "CRO",  "name": "Cronos",        "category": "exchange","base_price": 0.088},
]

REWARD_TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]
TIER_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]
TIER_CASHBACK_RATES = {
    "bronze": 0.01, "silver": 0.02, "gold": 0.03,
    "platinum": 0.05, "diamond": 0.08
}

# =============================================================================
# Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("CryptoLambda_SeedData")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("  Crypto Lambda Analytics — Seed Data Generator")
print("=" * 60)

# =============================================================================
# 1. Dimension: Users
# =============================================================================

print("\n[1/6] Generating dim_user...")

users = []
for i in range(NUM_USERS):
    user_id = f"user_{i:05d}"
    tier = random.choices(REWARD_TIERS, weights=TIER_WEIGHTS, k=1)[0]
    signup_date = START_DATE - timedelta(days=random.randint(1, 730))
    kyc_status = random.choices(["verified", "pending", "unverified"], weights=[0.8, 0.1, 0.1], k=1)[0]
    country = random.choice(["US", "UK", "SG", "HK", "DE", "FR", "JP", "AU", "CA", "BR"])
    users.append((user_id, tier, signup_date.strftime("%Y-%m-%d"), kyc_status, country))

user_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("tier", StringType(), False),
    StructField("signup_date", StringType(), False),
    StructField("kyc_status", StringType(), False),
    StructField("country", StringType(), False),
])

df_users = spark.createDataFrame(users, schema=user_schema)
df_users.write.mode("overwrite").parquet(f"{HDFS_BASE}/warehouse/dim_user")
print(f"  → {df_users.count()} users written to {HDFS_BASE}/warehouse/dim_user")

# =============================================================================
# 2. Dimension: Assets
# =============================================================================

print("\n[2/6] Generating dim_asset...")

asset_rows = [
    (a["symbol"], a["name"], a["category"], a["base_price"])
    for a in ASSETS
]

asset_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("base_price", DoubleType(), False),
])

df_assets = spark.createDataFrame(asset_rows, schema=asset_schema)
df_assets.write.mode("overwrite").parquet(f"{HDFS_BASE}/warehouse/dim_asset")
print(f"  → {df_assets.count()} assets written to {HDFS_BASE}/warehouse/dim_asset")

# =============================================================================
# 3. Dimension: Date
# =============================================================================

print("\n[3/6] Generating dim_date...")

dates = []
for i in range(NUM_DAYS):
    d = START_DATE + timedelta(days=i)
    dates.append((
        d.strftime("%Y-%m-%d"),
        d.year,
        d.month,
        d.day,
        (d.month - 1) // 3 + 1,        # quarter
        d.strftime("%A"),                # day_of_week
        1 if d.weekday() >= 5 else 0,   # is_weekend
    ))

date_schema = StructType([
    StructField("date_id", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("day_of_week", StringType(), False),
    StructField("is_weekend", IntegerType(), False),
])

df_dates = spark.createDataFrame(dates, schema=date_schema)
df_dates.write.mode("overwrite").parquet(f"{HDFS_BASE}/warehouse/dim_date")
print(f"  → {df_dates.count()} dates written to {HDFS_BASE}/warehouse/dim_date")

# =============================================================================
# 4. Raw Prices — Daily OHLCV per asset
# =============================================================================

print("\n[4/6] Generating raw prices (daily OHLCV)...")

prices = []
for asset in ASSETS:
    price = asset["base_price"]
    for i in range(NUM_DAYS):
        d = START_DATE + timedelta(days=i)
        # Random walk with slight upward drift
        daily_return = random.gauss(0.0005, 0.03)
        price = price * (1 + daily_return)
        price = max(price, asset["base_price"] * 0.1)  # floor at 10% of base

        high = price * (1 + abs(random.gauss(0, 0.02)))
        low = price * (1 - abs(random.gauss(0, 0.02)))
        open_price = price * (1 + random.gauss(0, 0.01))
        volume = random.uniform(1_000_000, 500_000_000) * (asset["base_price"] / 100)

        prices.append((
            asset["symbol"],
            d.strftime("%Y-%m-%d"),
            round(open_price, 8),
            round(high, 8),
            round(low, 8),
            round(price, 8),         # close
            round(volume, 2),
        ))

price_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("date", StringType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
])

df_prices = spark.createDataFrame(prices, schema=price_schema)
df_prices.write.mode("overwrite").partitionBy("date").parquet(f"{HDFS_BASE}/raw/prices")
print(f"  → {df_prices.count()} price records written to {HDFS_BASE}/raw/prices")

# =============================================================================
# 5. Raw Transactions — Buys and Sells
# =============================================================================

print("\n[5/6] Generating raw transactions...")

# Build a price lookup for transaction valuation
price_lookup = {}
for row in prices:
    price_lookup[(row[0], row[1])] = row[5]  # (symbol, date) -> close price

transactions = []
tx_count = 50000

for _ in range(tx_count):
    user_id = f"user_{random.randint(0, NUM_USERS - 1):05d}"
    asset = random.choice(ASSETS)
    day_offset = random.randint(0, NUM_DAYS - 1)
    tx_date = START_DATE + timedelta(days=day_offset)
    date_str = tx_date.strftime("%Y-%m-%d")
    tx_type = random.choices(["buy", "sell"], weights=[0.6, 0.4], k=1)[0]

    price = price_lookup.get((asset["symbol"], date_str), asset["base_price"])

    # Quantity varies by asset price (more units of cheaper coins)
    if asset["base_price"] > 10000:
        quantity = round(random.uniform(0.001, 0.5), 8)
    elif asset["base_price"] > 100:
        quantity = round(random.uniform(0.01, 5.0), 8)
    elif asset["base_price"] > 1:
        quantity = round(random.uniform(1, 500), 8)
    else:
        quantity = round(random.uniform(100, 50000), 8)

    total_value = round(price * quantity, 2)
    fee = round(total_value * 0.001, 2)  # 0.1% fee
    tx_id = str(uuid.uuid4())[:12]

    transactions.append((
        tx_id,
        user_id,
        asset["symbol"],
        tx_type,
        quantity,
        price,
        total_value,
        fee,
        date_str,
    ))

tx_schema = StructType([
    StructField("tx_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("tx_type", StringType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("price", DoubleType(), False),
    StructField("total_value", DoubleType(), False),
    StructField("fee", DoubleType(), False),
    StructField("date", StringType(), False),
])

df_tx = spark.createDataFrame(transactions, schema=tx_schema)
df_tx.write.mode("overwrite").partitionBy("date").parquet(f"{HDFS_BASE}/raw/transactions")
print(f"  → {df_tx.count()} transactions written to {HDFS_BASE}/raw/transactions")

# =============================================================================
# 6. Raw Rewards — Cashback and Staking
# =============================================================================

print("\n[6/6] Generating raw rewards...")

# Map users to their tiers for cashback rate lookup
user_tier_map = {u[0]: u[1] for u in users}

rewards = []
reward_count = 20000

for _ in range(reward_count):
    user_id = f"user_{random.randint(0, NUM_USERS - 1):05d}"
    tier = user_tier_map[user_id]
    day_offset = random.randint(0, NUM_DAYS - 1)
    reward_date = START_DATE + timedelta(days=day_offset)
    date_str = reward_date.strftime("%Y-%m-%d")

    reward_type = random.choices(["cashback", "staking"], weights=[0.7, 0.3], k=1)[0]

    if reward_type == "cashback":
        # Cashback on a purchase — amount based on tier rate
        spend_amount = round(random.uniform(10, 5000), 2)
        cashback_rate = TIER_CASHBACK_RATES[tier]
        reward_amount = round(spend_amount * cashback_rate, 4)
        reward_asset = "CRO"
    else:
        # Staking reward — periodic payout
        staked_asset = random.choice(["CRO", "ETH", "SOL", "DOT", "ADA"])
        apy = random.uniform(0.02, 0.14)
        staked_value = round(random.uniform(100, 50000), 2)
        # Daily reward = staked_value * (apy / 365)
        reward_amount = round(staked_value * (apy / 365), 6)
        reward_asset = staked_asset
        spend_amount = staked_value

    reward_id = str(uuid.uuid4())[:12]

    rewards.append((
        reward_id,
        user_id,
        reward_type,
        reward_asset,
        reward_amount,
        spend_amount,
        tier,
        date_str,
    ))

reward_schema = StructType([
    StructField("reward_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("reward_type", StringType(), False),
    StructField("reward_asset", StringType(), False),
    StructField("reward_amount", DoubleType(), False),
    StructField("spend_amount", DoubleType(), False),
    StructField("tier_at_time", StringType(), False),
    StructField("date", StringType(), False),
])

df_rewards = spark.createDataFrame(rewards, schema=reward_schema)
df_rewards.write.mode("overwrite").partitionBy("date").parquet(f"{HDFS_BASE}/raw/rewards")
print(f"  → {df_rewards.count()} rewards written to {HDFS_BASE}/raw/rewards")

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 60)
print("  Seed data generation complete!")
print("=" * 60)
print(f"""
  Dimensions:
    dim_user:   {NUM_USERS} users
    dim_asset:  {len(ASSETS)} assets
    dim_date:   {NUM_DAYS} days

  Raw Data:
    prices:       {len(prices):,} records  (partitioned by date)
    transactions: {tx_count:,} records  (partitioned by date)
    rewards:      {reward_count:,} records  (partitioned by date)

  HDFS Location: {HDFS_BASE}/
  
  Run 'make hdfs-ls' to verify.
""")

spark.stop()
