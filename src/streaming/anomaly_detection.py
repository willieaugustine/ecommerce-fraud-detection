from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import json
import numpy as np

# Define schema for transaction data
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("location", StringType()),
    StructField("ip_address", StringType()),
    StructField("timestamp", StringType()),
    StructField("is_fraud", BooleanType())
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ECommerceAnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read streaming data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*")

# Feature Engineering
feature_df = parsed_df.withColumn("transaction_time", to_timestamp(col("timestamp"))) \
    .withColumn("hour_of_day", hour(col("transaction_time"))) \
    .withColumn("is_foreign", col("location") != "US") \
    .withColumn("user_product_combo", concat(col("user_id"), lit("_"), col("product_id")))

# Calculate historical stats (would normally come from a database or checkpoint)
historical_stats = {
    "avg_amount": 150.0,
    "std_amount": 120.0,
    "user_avg_amounts": {},  # Would be populated from history
    "user_freq": {}          # Would be populated from history
}

# Define anomaly detection functions
def detect_amount_anomalies(amount, avg, std):
    z_score = (amount - avg) / std
    return abs(z_score) > 3  # 3 standard deviations

def detect_velocity_anomalies(user_id, current_time, last_transaction_time):
    # Implement logic to check transaction frequency
    return False  # Simplified for example

# Register UDFs
detect_amount_anomalies_udf = udf(
    lambda amt: detect_amount_anomalies(amt, historical_stats["avg_amount"], historical_stats["std_amount"]),
    BooleanType()
)

# Apply anomaly detection
anomaly_df = feature_df.withColumn(
    "amount_anomaly", detect_amount_anomalies_udf(col("amount"))
    
# Add other anomaly checks as needed...

# Prepare features for clustering
assembler = VectorAssembler(
    inputCols=["amount", "hour_of_day"],
    outputCol="features"
)

feature_vector_df = assembler.transform(feature_df)

# Scale features
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(feature_vector_df)
scaled_df = scaler_model.transform(feature_vector_df)

# Train KMeans model (in production, this would be pre-trained and updated periodically)
kmeans = KMeans(k=3, seed=1, featuresCol="scaled_features")
model = kmeans.fit(scaled_df)

# Add cluster predictions
clustered_df = model.transform(scaled_df)

# Calculate distance to centroid (anomaly score)
def calculate_distance(vector, centers, cluster):
    center = centers[cluster]
    return float(np.sqrt(np.sum((vector - center) ** 2)))

calculate_distance_udf = udf(calculate_distance, DoubleType())

centers = model.clusterCenters()
clustered_df = clustered_df.withColumn(
    "anomaly_score",
    calculate_distance_udf(
        col("scaled_features"),
        array([array([float(x) for x in center]) for center in centers]),
        col("prediction")
    )
)

# Define threshold for anomaly (could be dynamic)
ANOMALY_THRESHOLD = 2.5  # Example threshold

# Flag anomalies
results_df = clustered_df.withColumn(
    "is_anomaly",
    (col("anomaly_score") > ANOMALY_THRESHOLD) | col("amount_anomaly")
)

# Select relevant columns for output
output_df = results_df.select(
    "transaction_id",
    "user_id",
    "amount",
    "location",
    "timestamp",
    "anomaly_score",
    "is_anomaly",
    "prediction"
)

# Write anomalies to console for debugging
query_console = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write anomalies to Kafka
query_kafka = output_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "transaction-alerts") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
