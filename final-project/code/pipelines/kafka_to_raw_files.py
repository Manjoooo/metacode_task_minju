import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, hour
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ad-events")
    parser.add_argument("--raw-path", default="warehouse/raw/ad-events")
    parser.add_argument("--checkpoint-path", default="warehouse/checkpoints/raw-ad-events")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("kafka-to-raw-files")
        .getOrCreate()
    )

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select(
            col("data.event_id").alias("event_id"),
            col("data.event_time").cast(TimestampType()).alias("event_time"),
            col("data.campaign_id").alias("campaign_id"),
            col("data.user_id").alias("user_id"),
            col("data.event_type").alias("event_type"),
            col("data.amount").alias("amount"),
            col("data.ingest_time").cast(TimestampType()).alias("ingest_time"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
        )
        .withColumn("raw_date", to_date(col("event_time")))
        .withColumn("raw_hour", hour(col("event_time")))
    )

    query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .partitionBy("raw_date", "raw_hour")
        .start(args.raw_path)
    )

    print(f"[streaming] topic={args.topic}")
    print(f"[streaming] raw_path={args.raw_path}")
    print(f"[streaming] checkpoint_path={args.checkpoint_path}")

    query.awaitTermination()


if __name__ == "__main__":
    main()