import os
import json

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, round, from_json, current_timestamp, array_max
from pyspark.sql.types import DoubleType, MapType, StringType
from pyspark.ml.feature import IndexToString
from pyspark.ml.functions import vector_to_array


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-flows")

MODEL_LOAD_PATH = os.getenv("MODEL_LOAD_PATH", "/app/model")
FEATURE_COLUMNS_PATH = os.getenv("FEATURE_COLUMNS_PATH", "/app/shared/feature_columns.json")
LABEL_NAMES_PATH = os.getenv("LABEL_NAMES_PATH", "/app/shared/label_names.json")

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/app/output/predictions")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/app/output/checkpoints/live_ids")


spark = (
    SparkSession.builder
    .appName("ml-ids-bigdata-live")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


loaded_model = PipelineModel.load(MODEL_LOAD_PATH)

with open(FEATURE_COLUMNS_PATH, "r") as f:
    feature_columns = json.load(f)

with open(LABEL_NAMES_PATH, "r") as f:
    label_names = json.load(f)

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "5000")
    .load()
)

json_df = raw_kafka_df.select(
    from_json(
        col("value").cast("string"),
        MapType(StringType(), StringType())
    ).alias("data")
)

metadata_columns = [
    "Src IP",
    "Dst IP",
    "Src Port",
    "Dst Port",
    "Protocol",
    "source_file"
]

feature_columns_source = [
    c for c in feature_columns
    if c != "Total TCP Flow Time"
]

needed_columns = list(dict.fromkeys(
    metadata_columns +
    feature_columns_source +
    ["Total Connection Flow Time"]
))

data_df = json_df.select(
    *[
        col("data").getItem(c).alias(c)
        for c in needed_columns
    ]
)

data_df = data_df.withColumnRenamed(
    "Total Connection Flow Time",
    "Total TCP Flow Time"
)

missing_columns = [c for c in feature_columns if c not in data_df.columns]

if missing_columns:
    raise ValueError(f"[Spark-ML-IDS] [ERROR] Missing required model columns: {missing_columns}")

df_predict_input = data_df

for c in feature_columns:
    df_predict_input = df_predict_input.withColumn(
        c,
        col(c).cast(DoubleType())
    )

predictions = loaded_model.transform(df_predict_input)

converter = IndexToString(
    inputCol="prediction",
    outputCol="predicted_class",
    labels=label_names
)

predictions_with_labels = converter.transform(predictions)

predictions_with_labels = predictions_with_labels.withColumn(
    "confidence",
    array_max(vector_to_array(col("probability")))
)

result = predictions_with_labels.select(
    "source_file",
    "Src IP",
    "Dst IP",
    "Src Port",
    "Dst Port",
    "Protocol",
    "predicted_class",
    round("confidence", 2).alias("confidence")
)

log_stream = result.withColumn(
    "processed_at",
    current_timestamp()
)

attack_stream = log_stream.filter(
    col("predicted_class") != "BENIGN"
)

log_parquet_query = (
    log_stream
    .writeStream
    .format("parquet")
    .option("path", "/app/output/logs")
    .option("checkpointLocation", "/app/output/checkpoints/logs")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

attack_parquet_query = (
    attack_stream
    .writeStream
    .format("parquet")
    .option("path", "/app/output/attacks")
    .option("checkpointLocation", "/app/output/checkpoints/attacks")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

log_console_query = (
    log_stream.groupBy("source_file", "predicted_class").count()
    .writeStream
    .queryName("PREDICTION_SUMMARY_STREAM")
    .format("console")
    .option("truncate", "false")
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

attack_summary_console_query = (
    attack_stream
    .groupBy(
        "source_file",
        "Src IP",
        "Dst IP",
        "predicted_class"
    )
    .count()
    .writeStream
    .queryName("ATTACK_SUMMARY_STREAM")
    .format("console")
    .option("truncate", "false")
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()