# Airbnb Data Pipeline (Apache Spark)

## Overview

This project uses [Inside Airbnb](http://insideairbnb.com/get-the-data.html) data to build a scalable data pipeline using Apache Spark. It simulates the kind of data transformation, validation, and insight generation that a data engineer would perform at scale in a real-world environment.

You must download and add the data from Inside Airbnb yourself.

We focus on:
- Cleaning and transforming raw listing + calendar data
- Estimating revenue and occupancy for Airbnb hosts
- Aggregating key metrics by neighborhood
- Validating data quality
- Storing processed outputs in optimized formats (Parquet)

This project is designed to showcase Spark fundamentals for ETL, data analysis, and basic pipeline engineering.

---

## Goals

- Load, clean, and normalize Airbnb listing and calendar data
- Join datasets to compute per-listing revenue and occupancy rates
- Group and analyze trends by neighborhood and room type
- Implement data validation rules and generate quality reports
- Prepare the data for potential downstream ML tasks (optional)

---

## Tech Stack

| Tool           | Purpose                             |
|----------------|-------------------------------------|
| Apache Spark   | Distributed processing (ETL, joins) |
| Python 3.x     | Core language                       |
| PySpark        | Spark's Python API                  |
| Pandas         | Lightweight exploration             |
| Jupyter        | Optional: EDA in notebooks          |
| Parquet        | Optimized output format             |

---

## Project Structure

```bash
spark-airbnb/
├── data/                           # Raw input CSVs (listings.csv, calendar.csv)
├── spark_jobs/                     # Spark processing scripts
│   ├── etl_pipeline.py             # Clean & normalize data
│   ├── revenue_analysis.py         # Join + calculate occupancy & revenue
│   ├── neighborhood_aggregates.py  # Grouped metrics by location
│   └── validate_data.py            # Data quality rules
├── output/                         # Parquet + CSV output files
├── notebooks/                      # Optional Jupyter EDA
|   ├── debug_data_validation.py  
│   └── exploratory_analysis.ipynb
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```
---

## Spark Job Execution Order

To process the Airbnb data end-to-end, run the Spark jobs in the order listed below from the **project root**. Each step depends on the outputs of the previous one.

| Step | Script                                  | Description                                                                 | Output Files                                                         |
|------|-----------------------------------------|-----------------------------------------------------------------------------|----------------------------------------------------------------------|
| 1️⃣   | `spark_jobs/etl_pipeline.py`            | Cleans and normalizes raw listings & calendar data                          | `output/cleaned_listings.parquet`, `output/cleaned_calendar.parquet` |
| 2️⃣   | `spark_jobs/revenue_analysis.py`        | Joins cleaned data and computes revenue & occupancy rate per listing        | `output/revenue_by_listing.csv`                                      |
| 3️⃣   | `spark_jobs/neighborhood_aggregates.py` | Aggregates revenue and occupancy by neighborhood                            | `output/revenue_by_neighborhood.csv`                                 |
| 4️⃣   | `spark_jobs/validate_data.py`           | Validates all cleaned and generated data for nulls, schema issues, etc.     | `output/validation_report.txt`                                       |

---

✅ After running the pipeline, you can open any notebook from the `notebooks/` directory to explore results or debug issues interactively.
