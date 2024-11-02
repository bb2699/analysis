# src/transaction_analytics/analyzer.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionAnalyzer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def validate_data_quality(self, df):
        """
        Validate data quality and log issues
        Returns: DataFrame with quality metrics
        """
        quality_metrics = df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("null_customers"),
            F.sum(F.when(F.col("amount") < 0, 1).otherwise(0)).alias("negative_amounts"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        
        # Log metrics for monitoring
        quality_metrics.show()
        return quality_metrics
    
    def detect_suspicious_patterns(self, df, threshold_multiplier=3):
        """
        Identify suspicious patterns based on:
        1. Unusually high transaction frequency
        2. Unusual amount patterns
        3. Multiple regions in short time
        """
        # Create window specs for different patterns
        customer_window = Window.partitionBy("customer_id")\
                               .orderBy("transaction_timestamp")\
                               .rangeBetween(-3600, 0)  # 1-hour window
        
        # Add pattern detection columns
        enriched_df = df.withColumn(
            "transactions_last_hour", 
            F.count("transaction_id").over(customer_window)
        ).withColumn(
            "amount_avg", 
            F.avg("amount").over(Window.partitionBy("customer_id"))
        ).withColumn(
            "amount_stddev", 
            F.stddev("amount").over(Window.partitionBy("customer_id"))
        ).withColumn(
            "is_suspicious", 
            (F.col("amount") > (F.col("amount_avg") + threshold_multiplier * F.col("amount_stddev"))) |
            (F.col("transactions_last_hour") > 10)
        )
        
        return enriched_df
    
    def create_regional_aggregates(self, df):
        """
        Create daily regional aggregates with fraud metrics
        """
        return df.withColumn(
            "date", F.date_trunc("day", "transaction_timestamp")
        ).groupBy("date", "region").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_transactions"),
            F.countDistinct("customer_id").alias("unique_customers")
        ).orderBy("date", "region")
    
    def optimize_for_analytics(self, df):
        """
        Optimize the data for analytics queries:
        1. Partition by date for efficient time-based queries
        2. Z-order by region for efficient regional analysis
        """
        # Write to Delta with optimized layout
        df.write.format("delta")\
            .partitionBy("date")\
            .option("zOrderBy", "region")\
            .option("checkpointLocation", "/tmp/checkpoints/transactions")\
            .mode("overwrite")\
            .saveAsTable("transaction_analytics")
    
    def process_transactions(self, input_table):
        """
        Main pipeline to process transactions
        """
        # Read input data
        df = self.spark.table(input_table)
        
        # Validate data quality
        quality_metrics = self.validate_data_quality(df)
        if quality_metrics.first()["null_customers"] > 0:
            print("WARNING: Found null customer IDs")
        
        # Detect suspicious patterns
        enriched_df = self.detect_suspicious_patterns(df)
        
        # Create regional aggregates
        agg_df = self.create_regional_aggregates(enriched_df)
        
        # Optimize and save results
        self.optimize_for_analytics(agg_df)
        
        return agg_df

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Transaction Analysis")\
        .config("spark.sql.adaptive.enabled", "true")\
        .config("spark.sql.shuffle.partitions", "200")\
        .getOrCreate()
    
    analyzer = TransactionAnalyzer(spark)
    results = analyzer.process_transactions("transactions")