CREATE EXTERNAL TABLE IF NOT EXISTS batch_airline_sentiment (
    airline STRING,
    total_tweets BIGINT,
    positive_count BIGINT,
    negative_count BIGINT,
    neutral_count BIGINT,
    negative_ratio DOUBLE
)
STORED AS PARQUET
LOCATION '/project/batch_results_parquet/';
