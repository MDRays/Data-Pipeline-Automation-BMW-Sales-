import os
import sys
from pymongo import MongoClient
from pyspark.sql import SparkSession


def read_transformed(spark: SparkSession, path: str):
    """
    Baca hasil transform dari folder path.
    Coba baca parquet dulu, kalau gagal baru fallback ke csv.
    """
    try:
        df = spark.read.parquet(path)
        return df
    except Exception:
        # fallback csv 
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        return df


def main():
    # Ambil dari ENV (WAJIB)
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB", "bmw_store")
    mongo_collection = os.getenv("MONGO_COLLECTION", "bmw_sales")

    input_dir = os.getenv("BMW_TRANSFORM_DIR", "/opt/airflow/data/transform")

    if not mongo_uri:
        raise ValueError("MONGO_URI belum diset. Isi di .env atau environment docker-compose.")

    spark = (
        SparkSession.builder
        .appName("BMWSales_Load")
        .getOrCreate()
    )

    df = read_transformed(spark, input_dir)

    row_count = df.count()
    print(f"[INFO] Rows to load = {row_count}")

    # Biar kalau kosong, task FAIL 
    if row_count == 0:
        spark.stop()
        print("[ERROR] Data transform kosong. Tidak ada yang di-load.")
        sys.exit(1)

    # Konversi ke list dict untuk insert Mongo
    docs = [r.asDict(recursive=True) for r in df.collect()]

    client = MongoClient(mongo_uri)
    client[mongo_db][mongo_collection].insert_many(docs)

    print(f"[OK] Inserted {len(docs)} docs -> {mongo_db}.{mongo_collection}")

    spark.stop()


if __name__ == "__main__":
    main()