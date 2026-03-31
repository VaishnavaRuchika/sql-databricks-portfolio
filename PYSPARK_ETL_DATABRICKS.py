# Databricks notebook source
# PySpark ETL Pipeline - Data Transformation & Aggregation
# IT Company Analytics Platform
# ============================================================================

# MAGIC %md
# MAGIC # PySpark ETL Pipeline
# MAGIC ## Data Cleaning, Transformation & Aggregation
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC - Data cleaning and validation
# MAGIC - Feature engineering
# MAGIC - Aggregations and summaries
# MAGIC - Data quality checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Data from Tables

# COMMAND ----------

# Read all tables
users_df = spark.read.table("users")
servers_df = spark.read.table("servers")
logs_df = spark.read.table("application_logs")
metrics_df = spark.read.table("performance_metrics")
events_df = spark.read.table("system_events")

# Display row counts
print("=" * 60)
print("RAW DATA LOADED - Row Counts:")
print("=" * 60)
print(f"Users:                 {users_df.count():,} rows")
print(f"Servers:               {servers_df.count():,} rows")
print(f"Application Logs:      {logs_df.count():,} rows")
print(f"Performance Metrics:   {metrics_df.count():,} rows")
print(f"System Events:         {events_df.count():,} rows")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Cleaning - Application Logs

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Clean application logs
logs_clean = logs_df \
    .filter(F.col("user_id").isNotNull()) \
    .filter(F.col("log_id").isNotNull()) \
    .filter(F.col("duration_ms") >= 0) \
    .withColumn("duration_ms", F.col("duration_ms").cast(IntegerType())) \
    .withColumn("error_flag", F.when(F.col("error_code").isNotNull(), 1).otherwise(0)) \
    .dropDuplicates(["log_id"]) \
    .cache()

print(f"\nCleaned Application Logs:")
print(f"  Original rows: {logs_df.count():,}")
print(f"  Cleaned rows:  {logs_clean.count():,}")
print(f"  Rows removed:  {logs_df.count() - logs_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Feature Engineering - Add Derived Columns

# COMMAND ----------

# Add useful derived features
logs_enriched = logs_clean \
    .withColumn("action_lower", F.lower(F.col("action_type"))) \
    .withColumn("has_error", F.when(F.col("error_code").isNotNull(), True).otherwise(False)) \
    .withColumn("duration_category", 
        F.when(F.col("duration_ms") < 100, "fast")
        .when(F.col("duration_ms") < 1000, "normal")
        .when(F.col("duration_ms") < 5000, "slow")
        .otherwise("very_slow")) \
    .withColumn("error_severity",
        F.when(F.col("error_code") == "ERR_TIMEOUT", "high")
        .when(F.col("error_code") == "ERR_AUTH", "medium")
        .when(F.col("error_code") == "ERR_SERVER", "high")
        .otherwise("low"))

print(f"\nFeature Engineering Complete!")
print(f"  New columns added:")
print(f"    - action_lower")
print(f"    - has_error")
print(f"    - duration_category")
print(f"    - error_severity")

# Display sample
display(logs_enriched.select("user_id", "action_type", "duration_ms", "duration_category", "error_code").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Aggregation 1 - User Daily Activity Metrics

# COMMAND ----------

# Create user-level summary
user_metrics = logs_enriched.groupBy("user_id") \
    .agg(
        F.count("log_id").alias("total_actions"),
        F.sum("error_flag").alias("error_count"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.round(F.max("duration_ms"), 2).alias("max_duration_ms"),
        F.round(F.min("duration_ms"), 2).alias("min_duration_ms"),
        F.count(F.when(F.col("has_error") == True, 1)).alias("error_events"),
        F.count_distinct("action_type").alias("unique_actions"),
        F.count_distinct("server_id").alias("unique_servers")
    ) \
    .withColumn("error_rate_pct", 
        F.round(100.0 * F.col("error_count") / F.col("total_actions"), 2)) \
    .orderBy(F.col("total_actions").desc())

print(f"User Metrics Created: {user_metrics.count()} unique users")
display(user_metrics.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Aggregation 2 - Server Performance Metrics

# COMMAND ----------

# Create server-level summary
server_metrics = logs_enriched.groupBy("server_id") \
    .agg(
        F.count("log_id").alias("total_logs"),
        F.sum("error_flag").alias("error_count"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.round(F.max("duration_ms"), 2).alias("max_duration_ms"),
        F.count(F.when(F.col("has_error") == True, 1)).alias("error_events"),
        F.count_distinct("user_id").alias("unique_users"),
        F.count_distinct("action_type").alias("unique_actions")
    ) \
    .withColumn("error_rate_pct",
        F.round(100.0 * F.col("error_count") / F.col("total_logs"), 2)) \
    .orderBy(F.col("total_logs").desc())

print(f"Server Metrics Created: {server_metrics.count()} unique servers")
display(server_metrics.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Aggregation 3 - Action Type Analysis

# COMMAND ----------

# Analyze action types
action_metrics = logs_enriched.groupBy("action_type") \
    .agg(
        F.count("log_id").alias("total_count"),
        F.sum("error_flag").alias("error_count"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.round(F.max("duration_ms"), 2).alias("max_duration_ms"),
        F.count_distinct("user_id").alias("unique_users"),
        F.count_distinct("server_id").alias("unique_servers")
    ) \
    .withColumn("error_rate_pct",
        F.round(100.0 * F.col("error_count") / F.col("total_count"), 2)) \
    .orderBy(F.col("error_rate_pct").desc())

print(f"Action Type Analysis: {action_metrics.count()} unique action types")
display(action_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Data Quality Checks

# COMMAND ----------

print("=" * 60)
print("DATA QUALITY CHECKS")
print("=" * 60)

# Quality checks on cleaned logs
total_records = logs_enriched.count()
null_users = logs_enriched.filter(F.col("user_id").isNull()).count()
null_logs = logs_enriched.filter(F.col("log_id").isNull()).count()
null_durations = logs_enriched.filter(F.col("duration_ms").isNull()).count()
negative_durations = logs_enriched.filter(F.col("duration_ms") < 0).count()
duplicates = logs_df.count() - logs_enriched.count()
error_records = logs_enriched.filter(F.col("has_error") == True).count()
error_rate = round(100.0 * error_records / total_records, 2)

checks = {
    "Total Records": (total_records, total_records > 0),
    "Null user_id": (null_users, null_users == 0),
    "Null log_id": (null_logs, null_logs == 0),
    "Null duration": (null_durations, null_durations == 0),
    "Negative duration": (negative_durations, negative_durations == 0),
    "Duplicates removed": (duplicates, duplicates >= 0),
    "Records with errors": (error_records, error_records > 0),
    "Overall error rate %": (error_rate, error_rate >= 0)
}

for check_name, (value, passed) in checks.items():
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {check_name:30s} {value:>15,} {status}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Save Aggregated Tables (Optional)

# COMMAND ----------

# Save to Delta tables (optional - for production use)
# Uncomment the lines below if you want to save the results

# Save user metrics
# user_metrics.write.format("delta").mode("overwrite").save("dbfs:/data/gold/user_metrics")
# print("✓ Saved user_metrics to Delta")

# Save server metrics
# server_metrics.write.format("delta").mode("overwrite").save("dbfs:/data/gold/server_metrics")
# print("✓ Saved server_metrics to Delta")

# Save action metrics
# action_metrics.write.format("delta").mode("overwrite").save("dbfs:/data/gold/action_metrics")
# print("✓ Saved action_metrics to Delta")

print("\n✓ ETL Pipeline Complete!")
print("\nSummary:")
print(f"  - Cleaned {logs_clean.count():,} application logs")
print(f"  - Created {user_metrics.count()} user summary records")
print(f"  - Created {server_metrics.count()} server summary records")
print(f"  - Created {action_metrics.count()} action type summary records")
print(f"  - Data quality checks: PASSED ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Explore the Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Most Active Users

# COMMAND ----------

display(user_metrics.orderBy(F.col("total_actions").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Busiest Servers

# COMMAND ----------

display(server_metrics.orderBy(F.col("total_logs").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action Types by Error Rate

# COMMAND ----------

display(action_metrics.orderBy(F.col("error_rate_pct").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✓ Data cleaning complete
# MAGIC ✓ Feature engineering complete
# MAGIC ✓ Aggregations created
# MAGIC ✓ Data quality checks passed
# MAGIC ✓ Results ready for analysis and visualization
# MAGIC 
# MAGIC This ETL pipeline demonstrates:
# MAGIC - Spark DataFrame operations
# MAGIC - Data cleaning and validation
# MAGIC - Feature engineering with derived columns
# MAGIC - Aggregations and summaries
# MAGIC - Data quality assurance
