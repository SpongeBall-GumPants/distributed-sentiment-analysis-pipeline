CREATE EXTERNAL TABLE IF NOT EXISTS tweets_stream_avro (
    tweet_id STRING,
    airline_sentiment STRING,
    airline STRING,
    retweet_count INT,
    text STRING,
    tweet_created STRING
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION '/project/streamed_tweets_avro/';
