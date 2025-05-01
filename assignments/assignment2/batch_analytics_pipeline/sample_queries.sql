--  Hive SQL with sample analytical queries (e.g., monthly active users)
-- Monthly Active Users by Region
SELECT 
  year, 
  month, 
  region, 
  COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
GROUP BY year, month, region;

-- Top Categories by Play Count
SELECT 
  category, 
  COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
GROUP BY category
ORDER BY play_count DESC
LIMIT 10;

-- Average Session Length Weekly
SELECT 
  YEARWEEK(timestamp) AS week, 
  AVG(length) AS avg_session_length
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
GROUP BY YEARWEEK(timestamp);
