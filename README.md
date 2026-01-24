# Data-Pipeline-Automation-BMW-Sales-
- Built an automated ETL pipeline orchestrated with Apache Airflow to process BMW sales data with a modular workflow (extract → transform → load) and scheduled execution.
- Cleaned and standardized fields (e.g., model/year/region/specs, mileage, price, sales volume) and created derived attributes such as Sales_Classification for downstream analysis.
- Loaded the curated dataset into MongoDB (Atlas/Compass) and validated successful ingestion (≈ 150K documents) to support scalable querying and analytics.
- Implemented basic data quality handling (type casting, missing value handling, and consistent schema across pipeline outputs).
