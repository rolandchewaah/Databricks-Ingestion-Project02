import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

# --- Transformation Logic (Pure Functions for Testing) ---

def apply_silver_transformations(df):
    """Cleanses data by removing null users and duplicates."""
    return (df.filter(col("user_id").isNotNull())
              .dropDuplicates(["user_id"]))

def apply_gold_aggregations(df):
    """Aggregates data for business metrics."""
    return df.groupBy("event_date", "region").count()

# --- Main Ingestion Pipeline ---

def run_pipeline(catalog, schema, source_path):
    spark = SparkSession.builder.getOrCreate()
    
    # 1. Bronze
    bronze_table = f"{catalog}.{schema}.bronze_events"
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(source_path)
        .withColumn("ingested_at", current_timestamp())
        .writeStream.outputMode("append").table(bronze_table))

    # 2. Silver (Using the function we can test!)
    silver_table = f"{catalog}.{schema}.silver_events"
    bronze_df = spark.readStream.table(bronze_table)
    silver_df = apply_silver_transformations(bronze_df)
    
    silver_df.writeStream.outputMode("append").table(silver_table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--source_path", required=True)
    args = parser.parse_args()
    
    run_pipeline(args.catalog, args.schema, args.source_path)