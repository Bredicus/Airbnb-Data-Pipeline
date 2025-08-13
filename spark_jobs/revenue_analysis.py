from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, when, lit
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="AirbnbRevenueAnalysis"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Quiet Spark logs
    return spark

def main():
    spark = create_spark_session()

    # File paths
    listings_path = "output/cleaned_listings.parquet"
    calendar_path = "output/cleaned_calendar.parquet"
    output_path = "output/revenue_by_listing.csv"

    logger.info("Loading cleaned data...")
    listings_df = spark.read.parquet(listings_path)
    calendar_df = spark.read.parquet(calendar_path)

    logger.info("Preparing calendar data with revenue info...")

    # Add column to flag booked nights: available = False → booked = 1
    calendar_df = calendar_df.withColumn(
        "booked",
        when(col("available") == False, lit(1)).otherwise(lit(0))
    )

    # Revenue is price × booked (0 if not booked)
    calendar_df = calendar_df.withColumn(
        "daily_revenue",
        col("price") * col("booked")
    )

    logger.info("Aggregating calendar data by listing_id...")

    revenue_df = calendar_df.groupBy("listing_id").agg(
        _sum("booked").alias("total_booked_nights"),
        count("date").alias("total_nights"),
        _sum("daily_revenue").alias("total_revenue"),
        avg("price").alias("avg_nightly_price")
    )

    # Calculate occupancy rate
    revenue_df = revenue_df.withColumn(
        "occupancy_rate",
        (col("total_booked_nights") / col("total_nights")) * 100
    )

    logger.info("Joining with listings metadata...")
    joined_df = revenue_df.join(listings_df.select("id", "neighbourhood_cleansed", "room_type"), revenue_df["listing_id"] == col("id"), how="left")

    # Optional: drop the duplicate id column from listings
    joined_df = joined_df.drop("id")

    logger.info("Writing aggregated revenue data to CSV...")

    joined_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

    logger.info("✅ Revenue analysis complete. Output written to: %s", output_path)

    spark.stop()

if __name__ == "__main__":
    main()
