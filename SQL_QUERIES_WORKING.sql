-- ============================================================================
-- SQL & Databricks Portfolio Project - 8 Working SQL Queries
-- IT Company Analytics Platform
-- ============================================================================
-- These queries are optimized for the actual data structure
-- No timestamp column - uses actual available columns
-- ============================================================================

-- ============================================================================
-- QUERY 1: User Activity Ranking with Window Functions
-- Shows top users ranked by activity with window functions (ROW_NUMBER, RANK)
-- ============================================================================

SELECT
  user_id,
  COUNT(*) as total_actions,
  COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) as error_count,
  ROUND(100.0 * COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) / COUNT(*), 2) as error_rate_pct,
  ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as activity_rank,
  RANK() OVER (ORDER BY COUNT(*) DESC) as rank_with_ties
FROM application_logs
GROUP BY user_id
ORDER BY total_actions DESC
LIMIT 10;

-- ============================================================================
-- QUERY 2: Error Rate by Action Type
-- Shows which action types have the highest error rates
-- ============================================================================

SELECT
  action_type,
  COUNT(*) as total_actions,
  COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) as error_count,
  ROUND(100.0 * COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) / COUNT(*), 2) as error_rate_pct,
  ROUND(AVG(duration_ms), 2) as avg_duration_ms,
  ROUND(MAX(duration_ms), 2) as max_duration_ms,
  ROUND(MIN(duration_ms), 2) as min_duration_ms
FROM application_logs
GROUP BY action_type
ORDER BY error_rate_pct DESC;

-- ============================================================================
-- QUERY 3: User Activity by Company (Multi-Table JOIN)
-- Correlates users with their activity and company information
-- ============================================================================

SELECT
  u.company_id,
  COUNT(DISTINCT u.user_id) as user_count,
  COUNT(al.log_id) as total_logs,
  ROUND(AVG(al.duration_ms), 2) as avg_duration_ms,
  COUNT(CASE WHEN al.error_code IS NOT NULL THEN 1 END) as total_errors,
  ROUND(100.0 * COUNT(CASE WHEN al.error_code IS NOT NULL THEN 1 END) / COUNT(al.log_id), 2) as error_rate_pct
FROM users u
LEFT JOIN application_logs al ON u.user_id = al.user_id
GROUP BY u.company_id
ORDER BY total_logs DESC
LIMIT 10;

-- ============================================================================
-- QUERY 4: Top Servers by Activity and Errors
-- Identifies which servers are busiest and have the most errors
-- ============================================================================

SELECT
  al.server_id,
  COUNT(*) as total_logs,
  COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) as error_count,
  ROUND(100.0 * COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) / COUNT(*), 2) as error_rate,
  ROUND(AVG(duration_ms), 2) as avg_duration_ms,
  ROUND(MAX(duration_ms), 2) as max_duration_ms,
  ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as activity_rank
FROM application_logs al
GROUP BY al.server_id
ORDER BY total_logs DESC
LIMIT 10;

-- ============================================================================
-- QUERY 5: Server Performance Metrics Summary
-- Shows CPU, memory, and latency metrics for each server
-- ============================================================================

SELECT
  pm.server_id,
  COUNT(*) as metric_readings,
  ROUND(AVG(pm.cpu_percent), 2) as avg_cpu,
  ROUND(MAX(pm.cpu_percent), 2) as peak_cpu,
  ROUND(MIN(pm.cpu_percent), 2) as min_cpu,
  ROUND(AVG(pm.memory_percent), 2) as avg_memory,
  ROUND(MAX(pm.memory_percent), 2) as peak_memory,
  ROUND(AVG(pm.network_latency_ms), 2) as avg_latency_ms
FROM performance_metrics pm
GROUP BY pm.server_id
ORDER BY peak_cpu DESC
LIMIT 10;

-- ============================================================================
-- QUERY 6: User Plan Type Analysis
-- Segments users by plan tier and shows their activity patterns
-- ============================================================================

SELECT
  u.plan_tier,
  COUNT(DISTINCT u.user_id) as user_count,
  COUNT(al.log_id) as total_actions,
  ROUND(AVG(al.duration_ms), 2) as avg_duration_ms,
  COUNT(CASE WHEN al.error_code IS NOT NULL THEN 1 END) as total_errors,
  ROUND(100.0 * COUNT(CASE WHEN al.error_code IS NOT NULL THEN 1 END) / COUNT(al.log_id), 2) as error_rate_pct,
  ROUND(AVG(al.duration_ms) / NULLIF(STDDEV(al.duration_ms), 0), 2) as consistency_score
FROM users u
LEFT JOIN application_logs al ON u.user_id = al.user_id
GROUP BY u.plan_tier
ORDER BY total_actions DESC;

-- ============================================================================
-- QUERY 7: Regional Server Performance
-- Shows performance metrics grouped by server region
-- ============================================================================

SELECT
  s.region,
  COUNT(DISTINCT s.server_id) as server_count,
  COUNT(pm.metric_id) as metric_readings,
  ROUND(AVG(pm.cpu_percent), 2) as avg_cpu,
  ROUND(AVG(pm.memory_percent), 2) as avg_memory,
  ROUND(MAX(pm.cpu_percent), 2) as peak_cpu,
  ROUND(MAX(pm.memory_percent), 2) as peak_memory,
  ROUND(AVG(pm.network_latency_ms), 2) as avg_latency_ms,
  ROUND(MAX(pm.network_latency_ms), 2) as peak_latency_ms
FROM servers s
LEFT JOIN performance_metrics pm ON s.server_id = pm.server_id
GROUP BY s.region
ORDER BY avg_cpu DESC;

-- ============================================================================
-- QUERY 8: Active User Engagement Summary
-- Shows detailed user engagement with ranking and segmentation
-- ============================================================================

SELECT
  u.user_id,
  u.plan_tier,
  u.company_id,
  COUNT(al.log_id) as total_actions,
  COUNT(CASE WHEN al.error_code IS NOT NULL THEN 1 END) as error_count,
  COUNT(DISTINCT al.action_type) as action_types_used,
  COUNT(DISTINCT al.server_id) as servers_used,
  ROUND(AVG(al.duration_ms), 2) as avg_duration_ms,
  CASE
    WHEN COUNT(al.log_id) > 100 THEN 'Very Active'
    WHEN COUNT(al.log_id) > 50 THEN 'Active'
    WHEN COUNT(al.log_id) > 10 THEN 'Moderate'
    ELSE 'Low Activity'
  END as engagement_level,
  ROW_NUMBER() OVER (ORDER BY COUNT(al.log_id) DESC) as engagement_rank
FROM users u
LEFT JOIN application_logs al ON u.user_id = al.user_id
GROUP BY u.user_id, u.plan_tier, u.company_id
HAVING COUNT(al.log_id) > 0
ORDER BY total_actions DESC
LIMIT 20;

-- ============================================================================
-- BONUS: Data Verification Query
-- Verify all tables are loaded and working
-- ============================================================================

SELECT
  'users' as table_name,
  COUNT(*) as row_count
FROM users

UNION ALL

SELECT
  'servers' as table_name,
  COUNT(*) as row_count
FROM servers

UNION ALL

SELECT
  'application_logs' as table_name,
  COUNT(*) as row_count
FROM application_logs

UNION ALL

SELECT
  'performance_metrics' as table_name,
  COUNT(*) as row_count
FROM performance_metrics

UNION ALL

SELECT
  'system_events' as table_name,
  COUNT(*) as row_count
FROM system_events;
