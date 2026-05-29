# -*- coding: utf-8 -*-
from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = "hpdp-kafka:9092"
TOPIC_IN = "tweets_topic"
TOPIC_OUT = "alerts_negative"
HDFS_PATH = "file:///opt/hpdp/project/streamed_tweets_avro"


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    conf = t_env.get_config().get_configuration()
    conf.set_string("execution.checkpointing.interval", "30s")
    conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    conf.set_string("execution.checkpointing.timeout", "10min")
    conf.set_string("state.backend", "filesystem")
    conf.set_string("state.checkpoints.dir", "file:///opt/hpdp/project/state/checkpoints")
    conf.set_string("parallelism.default", "1")

    print("=" * 60)
    print("Starting COMBINED Flink Job - Track A")
    print("=" * 60)
    print("Source: Kafka topic '{}'".format(TOPIC_IN))
    print("Sink 1: Negative alerts -> Kafka topic '{}'".format(TOPIC_OUT))
    print("Sink 2: All tweets -> AVRO files at '{}'".format(HDFS_PATH))
    print("=" * 60)

    
    t_env.execute_sql(
        """
    CREATE TEMPORARY TABLE tweets_kafka (
        tweet_id STRING,
        airline_sentiment STRING,
        airline STRING,
        retweet_count INT,
        text STRING,
        tweet_created STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{}',
        'properties.bootstrap.servers' = '{}',
        'properties.group.id' = 'hpdp-flink-trackA',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'json.fail-on-missing-field' = 'false'
    )
    """.format(TOPIC_IN, BOOTSTRAP)
    )

    
    t_env.execute_sql(
        """
    CREATE TEMPORARY TABLE alerts_kafka (
        alert_message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{}',
        'properties.bootstrap.servers' = '{}',
        'format' = 'raw',
        'scan.startup.mode' = 'earliest-offset'
    )
    """.format(TOPIC_OUT, BOOTSTRAP)
    )

    
    t_env.execute_sql(
        """
    CREATE TEMPORARY TABLE tweets_stream_avro (
        tweet_id STRING,
        airline_sentiment STRING,
        airline STRING,
        retweet_count INT,
        text STRING,
        tweet_created STRING,
        dt STRING
    )
    PARTITIONED BY (dt)
    WITH (
        'connector' = 'filesystem',
        'path' = '{}',
        'format' = 'avro',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '15min',
        'auto-compaction' = 'true',
        'compaction.file-size' = '128MB'
    )
    """.format(HDFS_PATH)
    )

    
    statement_set = t_env.create_statement_set()
    
    statement_set.add_insert_sql(
        """
    INSERT INTO alerts_kafka
    SELECT
        CONCAT('ALERT [', airline, ']: ', text) as alert_message
    FROM tweets_kafka
    WHERE airline_sentiment = 'negative'
        AND airline IS NOT NULL
        AND text IS NOT NULL
    """
    )
    
    statement_set.add_insert_sql(
        """
    INSERT INTO tweets_stream_avro
    SELECT
        tweet_id,
        airline_sentiment,
        airline,
        retweet_count,
        text,
        tweet_created,
        DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH') AS dt
    FROM tweets_kafka
    WHERE tweet_id IS NOT NULL AND airline IS NOT NULL
    """
    )
    
    print("\nStarting dual-sink execution...")
    statement_set.execute().wait()


if __name__ == "__main__":
    main()