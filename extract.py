"""
=================================================
Milestone 3 - Extract (PySpark)

Nama  : M. Raihan Alfain Y.
Batch : 012

Deskripsi:
Membaca dataset CSV menggunakan PySpark dan menyimpan hasil extract
ke file parquet (staging/raw).
=================================================
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession


def create_spark(app_name: str = "P2M3_Extract") -> SparkSession:
    """
    Membuat / mengambil SparkSession.

    Parameters
    ----------
    app_name : str
        Nama aplikasi Spark.

    Returns
    -------
    SparkSession
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def extract_csv_to_parquet(input_csv: str, output_parquet: str) -> str:
    """
    Extract: baca CSV -> simpan parquet.

    Parameters
    ----------
    input_csv : str
        Path file CSV input.
    output_parquet : str
        Folder/path output parquet.

    Returns
    -------
    str
        Path output parquet.
    """
    spark = create_spark()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_csv)
    )

    df.write.mode("overwrite").parquet(output_parquet)
    spark.stop()
    return output_parquet


def main() -> None:
    input_csv = os.getenv(
        "P2M3_INPUT_CSV",
        "/opt/airflow/data/BMW sales data (2010-2024).csv"
    )
    output_parquet = os.getenv(
        "P2M3_RAW_PARQUET",
        "/opt/airflow/data/staging/bmw_sales_raw_parquet"
    )

    out = extract_csv_to_parquet(input_csv, output_parquet)
    print(f"[OK] Extract selesai -> {out}")


if __name__ == "__main__":
    main()
