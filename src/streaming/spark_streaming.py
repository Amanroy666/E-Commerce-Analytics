"""
Spark Structured Streaming for Real-time Analytics
Computes aggregations, KPIs, and customer behavior metrics
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeAnalytics:
    def __init__(self, app_name="EcommerceAnalytics"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
    
    def create_kafka_stream(self, kafka_servers, topic):
        """Create Kafka streaming DataFrame"""
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON value
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        return parsed_df
    
    def compute_realtime_kpis(self, stream_df):
        """Compute real-time KPIs with windowing"""
        kpis = stream_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count"),
                sum("total_amount").alias("total_revenue"),
                countDistinct("user_id").alias("unique_users"),
                avg("total_amount").alias("avg_transaction_value")
            )
        
        return kpis
    
    def write_to_sink(self, df, output_path, checkpoint_path):
        """Write streaming results to sink"""
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="1 minute") \
            .start()
        
        return query

if __name__ == "__main__":
    analytics = RealTimeAnalytics()
    
    # Create Kafka stream
    stream = analytics.create_kafka_stream(
        kafka_servers="localhost:9092",
        topic="ecommerce-events"
    )
    
    # Compute KPIs
    kpis = analytics.compute_realtime_kpis(stream)
    
    # Write to output
    query = analytics.write_to_sink(
        df=kpis,
        output_path="/tmp/ecommerce-kpis",
        checkpoint_path="/tmp/checkpoints/kpis"
    )
    
    query.awaitTermination()
