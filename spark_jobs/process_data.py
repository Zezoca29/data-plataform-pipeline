import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices_raw")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "warehouse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse")


schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("quote_currency", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("change_24h", DoubleType(), True),
        StructField("source_last_updated_at", LongType(), True),
        StructField("collected_at", LongType(), False),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("crypto-stream-processing")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) as payload")
        .select(from_json(col("payload"), schema).alias("event"))
        .select("event.*")
        .withColumn("collected_ts", to_timestamp(col("collected_at")))
        .withColumn("source_updated_ts", to_timestamp(col("source_last_updated_at")))
    )

    lake_path = f"s3a://{MINIO_BUCKET}/bronze/crypto_prices"

    parquet_query = (
        parsed.writeStream.outputMode("append")
        .format("parquet")
        .option("path", lake_path)
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/crypto_prices")
        .partitionBy("symbol")
        .start()
    )

    def write_batch_to_postgres(batch_df, batch_id):
        (batch_df.write.mode("append").format("jdbc").option("url", POSTGRES_URL)
         .option("dbtable", "public.fact_crypto_prices_raw")
         .option("user", POSTGRES_USER)
         .option("password", POSTGRES_PASSWORD)
         .option("driver", "org.postgresql.Driver")
         .save())
        print(f"Batch {batch_id} persisted to PostgreSQL")

    postgres_query = (
        parsed.writeStream.foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/crypto_prices_postgres")
        .start()
    )

    parquet_query.awaitTermination()
    postgres_query.awaitTermination()


if __name__ == "__main__":
    main()
