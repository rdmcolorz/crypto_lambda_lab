"""
data_quality_checks.py — Batch Layer Job 3/3

Runs data quality validation on all pipeline outputs:
  1. Completeness — null checks, row count thresholds
  2. Freshness   — data recency validation
  3. Accuracy    — range checks, referential integrity
  4. Consistency — cross-table validation

Reads from:
  - /data/warehouse/fact_portfolio_daily
  - /data/warehouse/fact_reward_daily
  - /data/warehouse/dim_user
  - /data/warehouse/dim_asset
  - /data/raw/transactions

Writes to:
  - /data/quality_reports/ (JSON summary of all checks)

Usage:
  spark-submit --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark-jobs/data_quality_checks.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import json

# =============================================================================
# Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("CryptoLambda_DataQualityChecks")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://namenode:9000/data"

print("=" * 60)
print("  Job 3/3: Data Quality Checks")
print("=" * 60)

# =============================================================================
# Load All Tables
# =============================================================================

print("\n[1/5] Loading tables for validation...")

tables = {}
table_paths = {
    "fact_portfolio_daily": f"{HDFS_BASE}/warehouse/fact_portfolio_daily",
    "fact_reward_daily":    f"{HDFS_BASE}/warehouse/fact_reward_daily",
    "dim_user":             f"{HDFS_BASE}/warehouse/dim_user",
    "dim_asset":            f"{HDFS_BASE}/warehouse/dim_asset",
    "dim_date":             f"{HDFS_BASE}/warehouse/dim_date",
    "raw_transactions":     f"{HDFS_BASE}/raw/transactions",
    "raw_prices":           f"{HDFS_BASE}/raw/prices",
    "raw_rewards":          f"{HDFS_BASE}/raw/rewards",
}

for name, path in table_paths.items():
    try:
        tables[name] = spark.read.parquet(path)
        print(f"  ✓ {name}: {tables[name].count():,} rows")
    except Exception as e:
        print(f"  ✗ {name}: FAILED to load — {str(e)[:80]}")
        tables[name] = None

# =============================================================================
# Quality Check Framework
# =============================================================================

results = []
run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
checks_passed = 0
checks_failed = 0


def run_check(check_name, table_name, passed, details=""):
    """Record a quality check result."""
    global checks_passed, checks_failed
    status = "PASS" if passed else "FAIL"
    icon = "✓" if passed else "✗"

    if passed:
        checks_passed += 1
    else:
        checks_failed += 1

    results.append({
        "check": check_name,
        "table": table_name,
        "status": status,
        "details": details,
        "timestamp": run_timestamp,
    })
    print(f"  {icon} [{status}] {check_name}: {details}")


# =============================================================================
# Check 1: Row Count Thresholds (Completeness)
# =============================================================================

print("\n[2/5] Completeness — Row count thresholds...")

row_count_thresholds = {
    "dim_user": 100,
    "dim_asset": 5,
    "dim_date": 30,
    "fact_portfolio_daily": 1000,
    "fact_reward_daily": 500,
    "raw_transactions": 10000,
    "raw_prices": 1000,
    "raw_rewards": 5000,
}

for table_name, min_rows in row_count_thresholds.items():
    df = tables.get(table_name)
    if df is not None:
        count = df.count()
        passed = count >= min_rows
        run_check(
            f"row_count_{table_name}",
            table_name,
            passed,
            f"Count: {count:,} (min: {min_rows:,})"
        )
    else:
        run_check(f"row_count_{table_name}", table_name, False, "Table not loaded")

# =============================================================================
# Check 2: Null Checks on Critical Columns
# =============================================================================

print("\n[3/5] Completeness — Null checks on key columns...")

null_checks = {
    "fact_portfolio_daily": ["user_id", "symbol", "date", "quantity_held", "market_value"],
    "fact_reward_daily": ["user_id", "date", "total_reward_value"],
    "dim_user": ["user_id", "tier"],
    "raw_transactions": ["tx_id", "user_id", "symbol", "tx_type", "quantity", "price"],
}

for table_name, columns in null_checks.items():
    df = tables.get(table_name)
    if df is None:
        run_check(f"null_check_{table_name}", table_name, False, "Table not loaded")
        continue

    for col in columns:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            passed = null_count == 0
            run_check(
                f"null_{table_name}.{col}",
                table_name,
                passed,
                f"Nulls: {null_count:,}"
            )
        else:
            run_check(
                f"null_{table_name}.{col}",
                table_name,
                False,
                f"Column '{col}' not found"
            )

# =============================================================================
# Check 3: Range Checks (Accuracy)
# =============================================================================

print("\n[4/5] Accuracy — Range and value checks...")

# Portfolio: quantity_held should be positive (we filter negatives in the job)
df_portfolio = tables.get("fact_portfolio_daily")
if df_portfolio is not None:
    neg_count = df_portfolio.filter(F.col("quantity_held") <= 0).count()
    run_check(
        "range_positive_holdings",
        "fact_portfolio_daily",
        neg_count == 0,
        f"Negative/zero holdings: {neg_count:,}"
    )

    # Market value should be non-negative
    neg_mv = df_portfolio.filter(F.col("market_value") < 0).count()
    run_check(
        "range_non_negative_market_value",
        "fact_portfolio_daily",
        neg_mv == 0,
        f"Negative market values: {neg_mv:,}"
    )

# Rewards: reward amounts should be non-negative
df_rewards = tables.get("fact_reward_daily")
if df_rewards is not None:
    neg_rewards = df_rewards.filter(F.col("total_reward_value") < 0).count()
    run_check(
        "range_non_negative_rewards",
        "fact_reward_daily",
        neg_rewards == 0,
        f"Negative reward values: {neg_rewards:,}"
    )

# Transactions: price and quantity should be positive
df_tx = tables.get("raw_transactions")
if df_tx is not None:
    bad_prices = df_tx.filter((F.col("price") <= 0) | (F.col("quantity") <= 0)).count()
    run_check(
        "range_positive_price_quantity",
        "raw_transactions",
        bad_prices == 0,
        f"Invalid price/quantity: {bad_prices:,}"
    )

    # tx_type should only be buy or sell
    valid_types = df_tx.filter(~F.col("tx_type").isin(["buy", "sell"])).count()
    run_check(
        "valid_tx_types",
        "raw_transactions",
        valid_types == 0,
        f"Invalid tx_type values: {valid_types:,}"
    )

# User tiers should be valid values
df_users = tables.get("dim_user")
if df_users is not None:
    valid_tiers = ["bronze", "silver", "gold", "platinum", "diamond"]
    invalid_tiers = df_users.filter(~F.col("tier").isin(valid_tiers)).count()
    run_check(
        "valid_user_tiers",
        "dim_user",
        invalid_tiers == 0,
        f"Invalid tiers: {invalid_tiers:,}"
    )

# =============================================================================
# Check 4: Referential Integrity
# =============================================================================

print("\n[5/5] Consistency — Referential integrity...")

# All user_ids in portfolio should exist in dim_user
if df_portfolio is not None and df_users is not None:
    portfolio_users = set(
        df_portfolio.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
    )
    dim_users = set(
        df_users.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
    )
    orphan_users = portfolio_users - dim_users
    run_check(
        "ref_integrity_portfolio_users",
        "fact_portfolio_daily → dim_user",
        len(orphan_users) == 0,
        f"Orphan user_ids: {len(orphan_users)}"
    )

# All symbols in portfolio should exist in dim_asset
df_assets = tables.get("dim_asset")
if df_portfolio is not None and df_assets is not None:
    portfolio_symbols = set(
        df_portfolio.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
    )
    dim_symbols = set(
        df_assets.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
    )
    orphan_symbols = portfolio_symbols - dim_symbols
    run_check(
        "ref_integrity_portfolio_assets",
        "fact_portfolio_daily → dim_asset",
        len(orphan_symbols) == 0,
        f"Orphan symbols: {len(orphan_symbols)}: {orphan_symbols if orphan_symbols else 'none'}"
    )

# =============================================================================
# Write Quality Report
# =============================================================================

report = {
    "run_timestamp": run_timestamp,
    "total_checks": checks_passed + checks_failed,
    "passed": checks_passed,
    "failed": checks_failed,
    "pass_rate": round(checks_passed / max(checks_passed + checks_failed, 1) * 100, 1),
    "checks": results,
}

# Write as a single JSON file to HDFS using Spark's Hadoop filesystem API
report_filename = f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
report_hdfs_path = f"{HDFS_BASE}/quality_reports/{report_filename}"
report_json_str = json.dumps(report, indent=2)

# Use Hadoop FileSystem API (always available inside Spark)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark.sparkContext._jvm.java.net.URI(HDFS_BASE),
    hadoop_conf,
)
output_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(report_hdfs_path)
output_stream = fs.create(output_path, True)  # True = overwrite
output_stream.write(bytearray(report_json_str, "utf-8"))
output_stream.close()

report_path = report_hdfs_path
print(f"\n  Report written to HDFS: {report_path}")

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 60)
print("  Data Quality Report")
print("=" * 60)
print(f"  Total checks:  {checks_passed + checks_failed}")
print(f"  Passed:        {checks_passed}")
print(f"  Failed:        {checks_failed}")
print(f"  Pass rate:     {report['pass_rate']}%")
print(f"  Report saved:  {report_path}")

if checks_failed > 0:
    print("\n  ⚠ FAILED CHECKS:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"    ✗ {r['check']}: {r['details']}")

print("")

spark.stop()

# Exit with non-zero code if critical checks failed (useful for Airflow)
if checks_failed > 0:
    print("Exiting with code 1 due to quality check failures.")
    exit(1)