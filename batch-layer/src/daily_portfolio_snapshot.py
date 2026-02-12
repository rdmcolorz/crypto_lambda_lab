"""
daily_portfolio_snapshot.py — Batch Layer Job 1/3

Computes daily portfolio snapshots for every user:
  - Current holdings per asset (net of buys and sells)
  - Average cost basis
  - Market value at day's close price
  - Unrealized P&L
  - Realized P&L from sells

Reads from:
  - /data/raw/transactions  (partitioned by date)
  - /data/raw/prices        (partitioned by date)
  - /data/warehouse/dim_user
  - /data/warehouse/dim_asset

Writes to:
  - /data/warehouse/fact_portfolio_daily (partitioned by date)

Optimizations demonstrated:
  - Broadcast join for small dimension tables
  - Partition pruning on date column
  - Window functions for running cost basis
  - Adaptive Query Execution (AQE)

Usage:
  spark-submit --master spark://spark-master:7077 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark-jobs/daily_portfolio_snapshot.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("CryptoLambda_DailyPortfolioSnapshot")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://namenode:9000/data"

print("=" * 60)
print("  Job 1/3: Daily Portfolio Snapshot")
print("=" * 60)

# =============================================================================
# Load Source Data
# =============================================================================

print("\n[1/5] Loading source data...")

df_transactions = spark.read.parquet(f"{HDFS_BASE}/raw/transactions")
df_prices = spark.read.parquet(f"{HDFS_BASE}/raw/prices")
df_users = spark.read.parquet(f"{HDFS_BASE}/warehouse/dim_user")
df_assets = spark.read.parquet(f"{HDFS_BASE}/warehouse/dim_asset")

print(f"  Transactions: {df_transactions.count():,} rows")
print(f"  Prices:       {df_prices.count():,} rows")
print(f"  Users:        {df_users.count():,} rows")
print(f"  Assets:       {df_assets.count():,} rows")

# =============================================================================
# Step 1: Compute Signed Quantities (buys positive, sells negative)
# =============================================================================

print("\n[2/5] Computing signed quantities per transaction...")

df_signed = df_transactions.withColumn(
    "signed_quantity",
    F.when(F.col("tx_type") == "buy", F.col("quantity"))
     .otherwise(-F.col("quantity"))
).withColumn(
    "signed_cost",
    F.when(F.col("tx_type") == "buy", F.col("total_value"))
     .otherwise(-F.col("total_value"))
)

# =============================================================================
# Step 2: Compute Cumulative Holdings Per User-Asset-Date
# =============================================================================

print("[3/5] Computing cumulative holdings with running cost basis...")

# Aggregate transactions per user-asset-date
df_daily_tx = (
    df_signed
    .groupBy("user_id", "symbol", "date")
    .agg(
        F.sum("signed_quantity").alias("daily_net_quantity"),
        F.sum("signed_cost").alias("daily_net_cost"),
        F.sum("fee").alias("daily_fees"),
        F.count("tx_id").alias("num_trades"),
        F.sum(
            F.when(F.col("tx_type") == "sell", F.col("total_value")).otherwise(0)
        ).alias("daily_sell_value"),
    )
)

# Running cumulative sum using window function
window_cum = (
    Window
    .partitionBy("user_id", "symbol")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_cumulative = (
    df_daily_tx
    .withColumn("quantity_held", F.sum("daily_net_quantity").over(window_cum))
    .withColumn("total_cost_basis", F.sum("daily_net_cost").over(window_cum))
    .withColumn("total_fees_paid", F.sum("daily_fees").over(window_cum))
    .withColumn("cumulative_sell_value", F.sum("daily_sell_value").over(window_cum))
)

# Compute average cost basis (total cost / quantity held, avoiding div by zero)
df_cumulative = df_cumulative.withColumn(
    "avg_cost_basis",
    F.when(F.col("quantity_held") > 0,
           F.col("total_cost_basis") / F.col("quantity_held"))
     .otherwise(0.0)
)

# =============================================================================
# Step 3: Join with Prices to Get Market Value
# =============================================================================

print("[4/5] Joining with market prices and computing P&L...")

# Broadcast join — prices per date is small enough per symbol
df_portfolio = (
    df_cumulative
    .join(
        F.broadcast(df_prices.select("symbol", "date", F.col("close").alias("market_price"))),
        on=["symbol", "date"],
        how="inner"
    )
)

# Compute market value and unrealized P&L
df_portfolio = (
    df_portfolio
    .withColumn(
        "market_value",
        F.when(F.col("quantity_held") > 0,
               F.col("quantity_held") * F.col("market_price"))
         .otherwise(0.0)
    )
    .withColumn(
        "unrealized_pnl",
        F.when(F.col("quantity_held") > 0,
               F.col("market_value") - F.col("total_cost_basis"))
         .otherwise(0.0)
    )
    .withColumn(
        "unrealized_pnl_pct",
        F.when((F.col("total_cost_basis") > 0) & (F.col("quantity_held") > 0),
               (F.col("unrealized_pnl") / F.col("total_cost_basis")) * 100)
         .otherwise(0.0)
    )
)

# Estimated realized P&L (simplified: sell_value - proportional cost basis)
df_portfolio = df_portfolio.withColumn(
    "realized_pnl",
    F.col("cumulative_sell_value") - (
        F.col("cumulative_sell_value") / F.greatest(F.col("market_value") + F.col("cumulative_sell_value"), F.lit(1.0))
    ) * F.col("total_cost_basis")
)

# =============================================================================
# Step 4: Select Final Columns and Write
# =============================================================================

print("[5/5] Writing fact_portfolio_daily to HDFS...")

df_output = (
    df_portfolio
    .select(
        F.monotonically_increasing_id().alias("snapshot_id"),
        "user_id",
        "symbol",
        "date",
        F.round("quantity_held", 8).alias("quantity_held"),
        F.round("avg_cost_basis", 4).alias("avg_cost_basis"),
        F.round("market_price", 4).alias("market_price"),
        F.round("market_value", 2).alias("market_value"),
        F.round("unrealized_pnl", 2).alias("unrealized_pnl"),
        F.round("unrealized_pnl_pct", 2).alias("unrealized_pnl_pct"),
        F.round("realized_pnl", 2).alias("realized_pnl"),
        F.round("total_fees_paid", 2).alias("total_fees_paid"),
        "num_trades",
    )
    # Filter out rows where the user has zero or negative holdings
    .filter(F.col("quantity_held") > 0.0)
)

df_output.write.mode("overwrite").partitionBy("date").parquet(
    f"{HDFS_BASE}/warehouse/fact_portfolio_daily"
)

row_count = df_output.count()

# =============================================================================
# Summary Stats
# =============================================================================

print("\n" + "=" * 60)
print("  Portfolio Snapshot Complete")
print("=" * 60)
print(f"  Rows written:      {row_count:,}")
print(f"  Unique users:      {df_output.select('user_id').distinct().count():,}")
print(f"  Unique assets:     {df_output.select('symbol').distinct().count():,}")
print(f"  Date range:        {df_output.agg(F.min('date')).collect()[0][0]} → {df_output.agg(F.max('date')).collect()[0][0]}")

# Top 5 portfolios by market value
print("\n  Top 5 portfolios by total market value (latest date):")
latest_date = df_output.agg(F.max("date")).collect()[0][0]
(
    df_output
    .filter(F.col("date") == latest_date)
    .groupBy("user_id")
    .agg(F.round(F.sum("market_value"), 2).alias("total_value"))
    .orderBy(F.desc("total_value"))
    .limit(5)
    .show(truncate=False)
)

print(f"  Output: {HDFS_BASE}/warehouse/fact_portfolio_daily\n")

spark.stop()
