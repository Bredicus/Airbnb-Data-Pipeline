from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, to_date, lit
from pyspark.sql.functions import col, regexp_replace, when, to_date, lit
from pyspark.sql.types import DoubleType, IntegerType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="AirbnbETL"):
    """Initialize a Spark session."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clean_listings(df):
    """Clean listings DataFrame."""
    logger.info(f"Original listings row count: {df.count()}")

    # Remove $ and , from price-related columns and cast to float
    price_cols = ["price", "weekly_price", "monthly_price", "security_deposit", "cleaning_fee"]
    for col_name in price_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[$,]", ""))
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Cast useful columns to correct types
    df = df.withColumn("minimum_nights", col("minimum_nights").cast(IntegerType()))
    df = df.withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType()))

    # Keep only necessary columns
    keep_cols = ["id", "price", "neighbourhood_cleansed", "room_type"]
    df = df.select(*keep_cols).dropna(subset=keep_cols)

    logger.info(f"Cleaned listings row count: {df.count()}")
    return df

def clean_calendar(calendar_df, listings_df):
    """Clean calendar DataFrame."""
    logger.info(f"Original calendar row count: {calendar_df.count()}")

    # Join with listings to get price as fallback
    calendar_df = calendar_df.join(
        listings_df.select(col("id").alias("listing_id"), col("price").alias("price_listing")),
        on="listing_id",
        how="inner"
    )

    # Clean price: remove symbols, fallback to listing price
    calendar_df = calendar_df.withColumn("price", regexp_replace("price", "[$,]", ""))
    calendar_df = calendar_df.withColumn("price", when((col("price") == "") | (col("price").isNull()), col("price_listing")).otherwise(col("price")))
    calendar_df = calendar_df.withColumn("price", col("price").cast(DoubleType()))

    # Clean availability and convert date
    calendar_df = calendar_df.withColumn("available", when(col("available") == "t", lit(True)).otherwise(lit(False)))
    calendar_df = calendar_df.withColumn("date", to_date("date"))

    # Keep relevant columns
    keep_cols = ["listing_id", "date", "price", "available"]
    calendar_df = calendar_df.select(*keep_cols).dropna(subset=keep_cols)

    logger.info(f"Cleaned calendar row count: {calendar_df.count()}")
    return calendar_df

def main():
    spark = create_spark_session()

    listings_path = "data/listings.csv.gz"
    calendar_path = "data/calendar.csv.gz"

    logger.info("Reading listings.csv...")
    listings_df = spark.read.csv(listings_path, 
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE"
    )

    logger.info("Reading calendar.csv...")
    calendar_df = spark.read.csv(calendar_path, 
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE"
    )

    logger.info("Cleaning listings data...")
    listings_clean = clean_listings(listings_df)

    logger.info("Cleaning calendar data...")
    calendar_clean = clean_calendar(calendar_df, listings_clean)

    listings_out = "output/cleaned_listings.parquet"
    calendar_out = "output/cleaned_calendar.parquet"

    logger.info(f"Writing cleaned listings to {listings_out}...")
    listings_clean.write.mode("overwrite").parquet(listings_out)

    logger.info(f"Writing cleaned calendar to {calendar_out}...")
    calendar_clean.write.mode("overwrite").parquet(calendar_out)

    logger.info("ETL pipeline completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
