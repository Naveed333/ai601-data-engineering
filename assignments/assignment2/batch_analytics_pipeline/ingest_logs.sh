#!/bin/bash

# Accept the date parameter in YYYY-MM-DD format
DATE=$1

# Check if the date parameter is provided
if [ -z "$DATE" ]; then
  echo "Please provide a date in YYYY-MM-DD format"
  exit 1
fi

# Parse year, month, and day from the input date
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define the local directory where logs and metadata are stored
LOCAL_LOGS_DIR="raw_data/$DATE"

# Check if the directory exists
if [ ! -d "$LOCAL_LOGS_DIR" ]; then
  echo "Error: Local directory $LOCAL_LOGS_DIR does not exist."
  exit 1
fi

# Define the HDFS base directories
HDFS_LOGS_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create the necessary HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_LOGS_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Ingest the log files (CSV/JSON) into HDFS
echo "Ingesting logs into HDFS..."
hdfs dfs -put $LOCAL_LOGS_DIR/* $HDFS_LOGS_DIR/
hdfs dfs -put $LOCAL_LOGS_DIR/*metadata $HDFS_METADATA_DIR/

if [ $? -eq 0 ]; then
  echo "Logs successfully ingested into HDFS under /raw/logs/$YEAR/$MONTH/$DAY and /raw/metadata/$YEAR/$MONTH/$DAY"
else
  echo "Error occurred during ingestion!"
  exit 1
fi
