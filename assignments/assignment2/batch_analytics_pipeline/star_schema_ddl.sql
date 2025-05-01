--  Hive DDL for creating star schema (fact + dimension tables)
-- Fact Table for User Actions (Using Parquet format)
CREATE TABLE fact_user_actions (
  user_id STRING,
  content_id STRING,
  action STRING,
  timestamp TIMESTAMP,   -- Converted to TIMESTAMP type
  device STRING,
  region STRING,
  session_id STRING,
  category STRING,
  title STRING,
  length INT,
  artist STRING
)
STORED AS PARQUET;

-- Dimension Table for Content Metadata (Using Parquet format)
CREATE TABLE dim_content (
  content_id STRING,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
STORED AS PARQUET;
