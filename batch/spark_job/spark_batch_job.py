# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def main():
    print("=" * 60)
    print("Starting Spark Batch Job - Airline Sentiment Analysis")
    print("=" * 60)
    
    spark = (
        SparkSession.builder
        .appName("HPDP_Batch_Airline_Sentiment")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    schema = StructType([
        StructField("tweet_id", StringType(), True),
        StructField("airline_sentiment", StringType(), True),
        StructField("airline_sentiment_confidence", DoubleType(), True),
        StructField("negativereason", StringType(), True),
        StructField("negativereason_confidence", DoubleType(), True),
        StructField("airline", StringType(), True),
        StructField("airline_sentiment_gold", StringType(), True),
        StructField("name", StringType(), True),
        StructField("negativereason_gold", StringType(), True),
        StructField("retweet_count", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("tweet_coord", StringType(), True),
        StructField("tweet_created", StringType(), True),
        StructField("tweet_location", StringType(), True),
        StructField("user_timezone", StringType(), True)
    ])

    print("\n[1/5] Reading data from HDFS: /project/raw/Tweets.csv")
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .csv("hdfs:///project/raw/Tweets.csv")
    )
    
    total_records = df_raw.count()
    print("Total records read: " + str(total_records))
    print("\nSchema:")
    df_raw.printSchema()
    
    print("\nSample data:")
    df_raw.show(5, truncate=False)

    valid_airlines = ["Virgin America", "United", "Southwest", "Delta", "US Airways", "American"]
    
    print("\n[2/5] Filtering valid airlines")
    df_clean = df_raw.filter(F.col("airline").isin(valid_airlines))
    clean_records = df_clean.count()
    print("Records after filtering: " + str(clean_records))

    print("\n[3/5] Aggregating sentiment counts by airline...")
    agg_df = (
        df_clean
        .groupBy("airline")
        .agg(
            F.count("*").alias("total_tweets"),
            
            F.sum(
                F.when(F.col("airline_sentiment") == "positive", 1).otherwise(0)
            ).alias("positive_count"),
            
            F.sum(
                F.when(F.col("airline_sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_count"),
            
            F.sum(
                F.when(F.col("airline_sentiment") == "neutral", 1).otherwise(0)
            ).alias("neutral_count"),
        )
    )

    print("\n[4/5] Calculating negative ratio...")
    result_df = (
        agg_df
        .withColumn(
            "negative_ratio",
            F.round(F.col("negative_count") / F.col("total_tweets"), 4)
        )
        .orderBy(F.desc("negative_ratio"))
    )

    print("\n*** BATCH RESULTS ***")
    result_df.show(truncate=False)

    output_path = "hdfs:///project/batch_results_parquet"
    
    print("\n[5/5] Writing results to: " + output_path)
    result_df.write.mode("overwrite").parquet(output_path)
    
    print("\nBatch processing completed successfully!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()