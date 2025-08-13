from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="AirbnbNeighborhoodAggregates"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    # Paths
    revenue_path = "output/revenue_by_listing.csv"
    listings_path = "output/cleaned_listings.parquet"
    output_path = "output/revenue_by_neighbourhood.csv"

    logger.info("Loading revenue by listing CSV...")
    revenue_df = spark.read.option("header", True).csv(revenue_path)

    logger.info("Casting revenue columns to appropriate types...")
    revenue_df = revenue_df.withColumn("total_revenue", col("total_revenue").cast("double")) \
                           .withColumn("occupancy_rate", col("occupancy_rate").cast("double")) \
                           .withColumn("listing_id", col("listing_id").cast("string"))

    logger.info("Loading cleaned listings parquet...")
    listings_df = spark.read.parquet(listings_path).select("id", "neighbourhood_cleansed")

    logger.info("Joining revenue with listings...")
    joined_df = revenue_df.join(
        listings_df.select("id",col("neighbourhood_cleansed").alias("listing_neighbourhood")),
        revenue_df.listing_id == listings_df.id, 
        "left"
    ).drop("id")
    
    logger.info("Aggregating by neighbourhood...")
    neighbourhood_agg_df = joined_df.groupBy("listing_neighbourhood").agg(
        _sum("total_revenue").alias("total_revenue"),
        avg("occupancy_rate").alias("avg_occupancy_rate"),
        count("listing_id").alias("num_listings")
    ).orderBy(col("total_revenue").desc())

    logger.info("Writing neighbourhood aggregates CSV...")
    neighbourhood_agg_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

    logger.info(f"✅ Neighbourhood aggregation complete. Output saved to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
