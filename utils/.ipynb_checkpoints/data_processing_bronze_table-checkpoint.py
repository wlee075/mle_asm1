import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import logging

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(
    LOG_DIR, f"etl_pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_csv_files_in_directory(directory_path):
    try:
        csv_files = [f for f in os.listdir(directory_path) if f.endswith(".csv")]
        return csv_files
    except FileNotFoundError:
        print(f"Directory not found: {directory_path}")
        return []
    except PermissionError:
        print(f"Permission denied for directory: {directory_path}")
        return []


def process_bronze_datalake(
    directory_path, dates_list, processing_function, spark, csv_file_path
):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory: {directory_path}")
    else:
        print(f"Directory already exists: {directory_path}")

    for date_str in dates_list:
        print(f"Processing date: {date_str}")
        processing_function(date_str, directory_path, spark, csv_file_path)


def process_bronze_table(snapshot_date_str, bronze_lms_directory, spark, csv_file_path):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    if not os.path.isfile(csv_file_path):
        print(f"Error: File not found at path: {csv_file_path}")
        return
    try:
        if "financial" in bronze_lms_directory:
            df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
            df = df.withColumn(
                "snapshot_date", to_date(col("snapshot_date"), "M/d/yy")
            ).filter(F.col("snapshot_date") == snapshot_date.strftime("%Y-%m-%d"))
        else:
            df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(
                F.col("snapshot_date") == snapshot_date.strftime("%Y-%m-%d")
            )
    except Exception as e:
        print(f"Error reading data for date {snapshot_date_str}: {e}")
        return
    df_with_metadata = df.withColumn("_metadata_file_name", F.input_file_name())
    delta_table_path = os.path.join(bronze_lms_directory, f"date={snapshot_date_str}")
    try:
        df_with_metadata.write.format("parquet").mode("overwrite").partitionBy(
            "snapshot_date"
        ).save(delta_table_path)
        print(f"Saved data to Parquet table at: {delta_table_path}")
    except Exception as e:
        print(f"Error writing data to Parquet table for date {snapshot_date_str}: {e}")
