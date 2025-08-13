from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, count
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="AirbnbDataValidation"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def write_report(report_lines, path="output/data_validation_report.txt"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for line in report_lines:
            f.write(line + "\n")
    logger.info("📝 Validation report written to: %s", path)

def validate_dataframe(df, name, required_cols):
    issues = []
    row_count = df.count()
    issues.append(f"🗂 Dataset: {name}")
    issues.append(f"→ Total rows: {row_count}")

    dtype_map = dict(df.dtypes)

    for col_name in required_cols:
        dtype = dtype_map.get(col_name)
        if dtype in ("double", "float"):
            null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        else:
            null_count = df.filter(col(col_name).isNull()).count()

        if null_count > 0:
            issues.append(f"⚠️  {null_count} rows have NULL in '{col_name}' (type: {dtype})")
        else:
            issues.append(f"✅ Column '{col_name}' ({dtype}) has no missing values.")

    issues.append("-" * 40)
    return issues

def main():
    spark = create_spark_session()

    # File paths
    listings_path = "output/cleaned_listings.parquet"
    calendar_path = "output/cleaned_calendar.parquet"
    revenue_path = "output/revenue_by_listing.csv"
    report_path = "output/data_validation_report.txt"

    logger.info("Loading cleaned listings...")
    listings_df = spark.read.parquet(listings_path)

    logger.info("Loading cleaned calendar...")
    calendar_df = spark.read.parquet(calendar_path)

    logger.info("Loading revenue by listing...")
    revenue_df = spark.read.option("header", True).csv(revenue_path)

    # Cast revenue columns (if needed)
    revenue_df = revenue_df.withColumn("total_revenue", col("total_revenue").cast("double")) \
                           .withColumn("occupancy_rate", col("occupancy_rate").cast("double"))

    logger.info("Running validations...")

    report = []

    report += validate_dataframe(listings_df, "cleaned_listings.parquet", ["id", "price", "neighbourhood_cleansed"])
    report += validate_dataframe(calendar_df, "cleaned_calendar.parquet", ["listing_id", "date", "price", "available"])
    report += validate_dataframe(revenue_df, "revenue_by_listing.csv", ["listing_id", "total_revenue", "occupancy_rate"])

    write_report(report, path=report_path)

    logger.info("✅ Data validation completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
