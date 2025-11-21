"""Daily batch aggregation using PySpark"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, window

def daily_aggregation(input_path, output_path):
    spark = SparkSession.builder.appName("DailyAggregation").getOrCreate()
    
    # Read data
    df = spark.read.parquet(input_path)
    
    # Aggregate metrics
    daily_metrics = df.groupBy("date").agg(
        count("transaction_id").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        count(col("user_id").distinct()).alias("unique_customers")
    )
    
    # Write results
    daily_metrics.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    daily_aggregation("/data/transactions", "/data/aggregated")
