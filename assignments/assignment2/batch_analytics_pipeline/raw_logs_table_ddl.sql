--  Hive DDL for raw tables (external tables)
-- Raw Logs Table (Partitioned by Year, Month, Day)
CREATE EXTERNAL TABLE raw_user_logs (
  user_id STRING,
  content_id STRING,
  action STRING,
  timestamp STRING,   -- Timestamp stored as STRING, will be converted during transformation
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs';

-- Raw Content Metadata Table (Partitioned by Year, Month, Day)
CREATE EXTERNAL TABLE raw_content_metadata (
  content_id STRING,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata';
