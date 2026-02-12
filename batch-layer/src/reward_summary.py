"""
reward_summary.py — Batch Layer Job 2/3

Computes daily reward summaries per user:
  - Total cashback earned
  - Total staking rewards earned
  - Rolling 30-day spend (for tier qualification)
  - Rolling 30-day total rewards
  - Tier at time of each reward
  - Reward breakdown by asset

Reads from:
  - /data/raw/rewards        (partitioned by date)
  - /data/raw/transactions   (partitioned by date)
  - /data/warehouse/dim_user

Writes to:
  - /data/warehouse/fact_reward_daily (partitioned by date)

Optimizations demonstrated:
  - Window functions for rolling aggregations
  - Broadcast joins for dimension tables
  - Coalesce to reduce small output files

Usage:
  spark-submit --master spark://spark-master:7077 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark-jobs/reward_summary.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("CryptoLambda_RewardSummary")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://namenode:9000/data"

print("=" * 60)
print("  Job 2/3: Daily Reward Summary")
print("=" * 60)

# =============================================================================
# Load Data
# =============================================================================

print("\n[1/4] Loading source data...")

df_rewards = spark.read.parquet(f"{HDFS_BASE}/raw/rewards")
df_transactions = spark.read.parquet(f"{HDFS_BASE}/raw/transactions")
df_users = spark.read.parquet(f"{HDFS_BASE}/warehouse/dim_user")

print(f"  Rewards:      {df_rewards.count():,} rows")
print(f"  Transactions: {df_transactions.count():,} rows")
print(f"  Users:        {df_users.count():,} rows")

# =============================================================================
# Step 1: Daily Reward Aggregation Per User
# =============================================================================

print("\n[2/4] Aggregating daily rewards per user...")

# Cashback totals
df_cashback = (
    df_rewards
    .filter(F.col("reward_type") == "cashback")
    .groupBy("user_id", "date")
    .agg(
        F.round(F.sum("reward_amount"), 4).alias("cashback_earned"),
        F.round(F.sum("spend_amount"), 2).alias("cashback_spend"),
        F.count("reward_id").alias("cashback_count"),
    )
)

# Staking totals
df_staking = (
    df_rewards
    .filter(F.col("reward_type") == "staking")
    .groupBy("user_id", "date")
    .agg(
        F.round(F.sum("reward_amount"), 6).alias("staking_earned"),
        F.round(F.sum("spend_amount"), 2).alias("staked_value"),
        F.count("reward_id").alias("staking_count"),
    )
)

# Combine cashback + staking
df_daily_rewards = (
    df_cashback
    .join(df_staking, on=["user_id", "date"], how="full_outer")
    .fillna(0.0)
)

# Total rewards for the day
df_daily_rewards = df_daily_rewards.withColumn(
    "total_reward_value",
    F.round(F.coalesce(F.col("cashback_earned"), F.lit(0.0)) +
            F.coalesce(F.col("staking_earned"), F.lit(0.0)), 4)
)

# =============================================================================
# Step 2: Rolling 30-Day Spend from Transactions (for tier qualification)
# =============================================================================

print("[3/4] Computing rolling 30-day spend for tier qualification...")

# Daily spend per user from buy transactions
df_daily_spend = (
    df_transactions
    .filter(F.col("tx_type") == "buy")
    .groupBy("user_id", "date")
    .agg(
        F.round(F.sum("total_value"), 2).alias("daily_buy_spend")
    )
)

# Rolling 30-day window on spend
window_30d = (
    Window
    .partitionBy("user_id")
    .orderBy(F.col("date").cast("date").cast("long"))
    .rangeBetween(-30 * 86400, 0)  # 30 days in seconds
)

df_daily_spend = df_daily_spend.withColumn(
    "date_long", F.col("date").cast("date").cast("long")
)

df_rolling_spend = (
    df_daily_spend
    .withColumn("rolling_30d_spend", F.round(F.sum("daily_buy_spend").over(window_30d), 2))
    .drop("date_long")
)

# =============================================================================
# Step 3: Join Everything and Enrich
# =============================================================================

# Join daily rewards with rolling spend
df_enriched = (
    df_daily_rewards
    .join(df_rolling_spend, on=["user_id", "date"], how="left")
    .fillna(0.0, subset=["daily_buy_spend", "rolling_30d_spend"])
)

# Join with user dimension for tier info (broadcast — small table)
df_enriched = (
    df_enriched
    .join(
        F.broadcast(df_users.select("user_id", F.col("tier").alias("user_tier"), "country")),
        on="user_id",
        how="left"
    )
)

# Compute a dynamic tier suggestion based on rolling 30-day spend
df_enriched = df_enriched.withColumn(
    "suggested_tier",
    F.when(F.col("rolling_30d_spend") >= 50000, "diamond")
     .when(F.col("rolling_30d_spend") >= 25000, "platinum")
     .when(F.col("rolling_30d_spend") >= 10000, "gold")
     .when(F.col("rolling_30d_spend") >= 2500, "silver")
     .otherwise("bronze")
)

# Flag if user qualifies for an upgrade
df_enriched = df_enriched.withColumn(
    "tier_upgrade_eligible",
    F.when(F.col("suggested_tier") > F.col("user_tier"), True)
     .otherwise(False)
)

# Rolling 30-day rewards
window_30d_rewards = (
    Window
    .partitionBy("user_id")
    .orderBy(F.col("date").cast("date").cast("long"))
    .rangeBetween(-30 * 86400, 0)
)

df_enriched = df_enriched.withColumn(
    "rolling_30d_rewards",
    F.round(F.sum("total_reward_value").over(window_30d_rewards), 4)
)

# =============================================================================
# Step 4: Write Output
# =============================================================================

print("[4/4] Writing fact_reward_daily to HDFS...")

df_output = (
    df_enriched
    .select(
        F.monotonically_increasing_id().alias("reward_summary_id"),
        "user_id",
        "user_tier",
        "country",
        "date",
        "cashback_earned",
        "cashback_spend",
        "cashback_count",
        "staking_earned",
        "staked_value",
        "staking_count",
        "total_reward_value",
        "daily_buy_spend",
        "rolling_30d_spend",
        "rolling_30d_rewards",
        "suggested_tier",
        "tier_upgrade_eligible",
    )
)

# Coalesce to avoid too many small files
df_output.coalesce(8).write.mode("overwrite").partitionBy("date").parquet(
    f"{HDFS_BASE}/warehouse/fact_reward_daily"
)

row_count = df_output.count()

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 60)
print("  Reward Summary Complete")
print("=" * 60)
print(f"  Rows written:       {row_count:,}")
print(f"  Unique users:       {df_output.select('user_id').distinct().count():,}")

# Tier distribution
print("\n  Reward distribution by user tier:")
(
    df_output
    .groupBy("user_tier")
    .agg(
        F.round(F.sum("cashback_earned"), 2).alias("total_cashback"),
        F.round(F.sum("staking_earned"), 2).alias("total_staking"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    .orderBy("user_tier")
    .show(truncate=False)
)

# Users eligible for tier upgrade
upgrade_count = df_output.filter(F.col("tier_upgrade_eligible") == True).select("user_id").distinct().count()
print(f"  Users eligible for tier upgrade: {upgrade_count:,}")
print(f"  Output: {HDFS_BASE}/warehouse/fact_reward_daily\n")

spark.stop()
