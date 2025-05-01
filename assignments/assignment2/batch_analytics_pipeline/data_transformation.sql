--  Hive SQL for data transformations (inserting into star schema)

-- Insert data from raw logs into the fact table
INSERT OVERWRITE TABLE fact_user_actions
SELECT
  r.user_id,
  r.content_id,
  r.action,
  CAST(r.timestamp AS TIMESTAMP) AS timestamp,
  r.device,
  r.region,
  r.session_id,
  c.category,
  c.title,
  c.length,
  c.artist
FROM raw_user_logs r
JOIN raw_content_metadata c ON r.content_id = c.content_id;

-- Insert data from raw content metadata into the dimension table
INSERT OVERWRITE TABLE dim_content
SELECT DISTINCT content_id, title, category, length, artist FROM raw_content_metadata;
