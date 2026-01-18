"""
=================================================
Milestone 3 - Transform (PySpark)

Nama  : M. Raihan Alfain Y.
Batch : 012

Deskripsi:
Membersihkan dan memproses data menggunakan PySpark sesuai hasil EDA,
lalu menyimpan hasilnya ke parquet clean.
=================================================
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_spark(app_name: str = "P2M3_Transform") -> SparkSession:
    """Membuat / mengambil SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def transform_bmw_sales(df: DataFrame) -> DataFrame:
    """
    Transformasi utama (cleaning + processing) sesuai EDA.

    Yang dilakukan:
    - trim kolom string
    - hapus koma trailing pada string (contoh: "Asia," -> "Asia")
    - cast tipe data numerik
    - filter range yang masuk akal
    - buat Record_ID (sha2) untuk unik
    - drop duplicates berdasarkan Record_ID

    Returns
    -------
    DataFrame
    """
    # kolom string 
    str_cols = [
        "Model", "Region", "Color", "Fuel_Type",
        "Transmission", "Sales_Classification"
    ]

    for c in str_cols:
        if c in df.columns:
            df = (
                df.withColumn(c, F.trim(F.col(c)))
                  # hapus koma trailing: "Asia," -> "Asia"
                  .withColumn(c, F.regexp_replace(F.col(c), r",\s*$", ""))
            )

    # cast numerik 
    cast_map = {
        "Year": "int",
        "Engine_Size_L": "double",
        "Mileage_KM": "int",
        "Price_USD": "int",
        "Sales_Volume": "int",
    }
    for col_name, t in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(t))

    # filtering basic 
    if "Year" in df.columns:
        df = df.filter((F.col("Year") >= 2010) & (F.col("Year") <= 2024))
    if "Engine_Size_L" in df.columns:
        df = df.filter((F.col("Engine_Size_L") > 0) & (F.col("Engine_Size_L") < 10))
    if "Mileage_KM" in df.columns:
        df = df.filter(F.col("Mileage_KM") >= 0)
    if "Price_USD" in df.columns:
        df = df.filter(F.col("Price_USD") > 0)
    if "Sales_Volume" in df.columns:
        df = df.filter(F.col("Sales_Volume") >= 0)

    # Record_ID unik (hash dari kolom-kolom utama)
    key_cols = [c for c in [
        "Model", "Year", "Region", "Color", "Fuel_Type", "Transmission",
        "Engine_Size_L", "Mileage_KM", "Price_USD", "Sales_Volume", "Sales_Classification"
    ] if c in df.columns]

    df = df.withColumn(
        "Record_ID",
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in key_cols]), 256)
    )

    df = df.dropDuplicates(["Record_ID"])
    return df


def transform_parquet_to_parquet(input_parquet: str, output_parquet: str) -> str:
    """
    Load parquet raw -> transform -> write parquet clean.
    """
    spark = create_spark()
    df = spark.read.parquet(input_parquet)

    out = transform_bmw_sales(df)
    out.write.mode("overwrite").parquet(output_parquet)

    spark.stop()
    return output_parquet


def main() -> None:
    in_pq = os.getenv("P2M3_RAW_PARQUET", "/opt/airflow/data/staging/bmw_sales_raw_parquet")
    out_pq = os.getenv("P2M3_CLEAN_PARQUET", "/opt/airflow/data/staging/bmw_sales_clean_parquet")

    out = transform_parquet_to_parquet(in_pq, out_pq)
    print(f"[OK] Transform selesai -> {out}")


if __name__ == "__main__":
    main()
