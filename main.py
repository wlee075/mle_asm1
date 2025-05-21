import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import logging
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, TimestampType
import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table

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

# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01"
start_date_str = "2023-01-01"
end_date_str = "2024-12-01"


# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates



dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
data_directory = 'data'
list_of_raw_files = utils.data_processing_bronze_table.get_csv_files_in_directory(data_directory)

# build bronze
for file_name in list_of_raw_files:
    try:
        base_name = os.path.splitext(file_name)[0]
        bronze_directory_path = f"datamart/bronze/{base_name}/"
        csv_file_path = os.path.join(data_directory, file_name)
        utils.data_processing_bronze_table.process_bronze_datalake(
            directory_path=bronze_directory_path,
            dates_list=dates_str_lst,
            processing_function=utils.data_processing_bronze_table.process_bronze_table,
            spark=spark,
            csv_file_path=csv_file_path
        )
    except Exception as e:
        logger.error("An error occurred while processing the table.")
        logger.error(f"Exception: {e}")

processor = utils.data_processing_silver_table.DataProcessor(spark)

# build cleaned silver tables
for file_name in list_of_raw_files:
    try:
        base_name = os.path.splitext(file_name)[0]
        bronze_directory_path = f"datamart/bronze/{base_name}/"
        silver_directory = f'datamart/silver/{base_name}_cleaned'
        processor.process_bronze_to_silver(
            bronze_path=bronze_directory_path,
            silver_path=silver_directory,
            dates=dates_str_lst,
            source_type=base_name
        )
    except Exception as e:
        logger.error("An error occurred while processing the table.")
        logger.error(f"Exception: {e}")
        
# build normalised silver tables
silver_builder = utils.data_processing_silver_table.SilverDataMart(spark)
silver_builder.run(write_mode="overwrite", preview=True)
# build gold table and labels
proc = utils.data_processing_gold_table.GoldDataProcessor(spark)
proc.build_loan_analytics_star()
proc.build_label_store(dpd_threshold=30, mob_threshold=6)


