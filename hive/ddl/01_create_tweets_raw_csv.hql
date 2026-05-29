CREATE EXTERNAL TABLE IF NOT EXISTS tweets_raw_csv (
    tweet_id STRING,
    airline_sentiment STRING,
    airline_sentiment_confidence DOUBLE,
    negativereason STRING,
    negativereason_confidence DOUBLE,
    airline STRING,
    airline_sentiment_gold STRING,
    name STRING,
    negativereason_gold STRING,
    retweet_count INT,
    text STRING,
    tweet_coord STRING,
    tweet_created STRING,
    tweet_location STRING,
    user_timezone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ',',
   "quoteChar"     = '"'
)
STORED AS TEXTFILE
LOCATION '/project/raw'
TBLPROPERTIES (
  "skip.header.line.count"="1"
)
