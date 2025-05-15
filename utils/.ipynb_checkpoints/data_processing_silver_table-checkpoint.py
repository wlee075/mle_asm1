from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import os

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, f"etl_pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

LMS_LOAN_SCHEMA = StructType([
    StructField("loan_id", StringType()),
    StructField("Customer_ID", StringType()),
    StructField("loan_start_date", DateType()),
    StructField("tenure", IntegerType()),
    StructField("installment_num", IntegerType()),
    StructField("loan_amt", IntegerType()),
    StructField("due_amt", DoubleType()),
    StructField("paid_amt", DoubleType()),
    StructField("overdue_amt", DoubleType()),
    StructField("balance", DoubleType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

ATTRIBUTES_SCHEMA = StructType([
    StructField("Customer_ID", StringType()),
    StructField("Name", StringType()),
    StructField("Age", StringType()),
    StructField("SSN", StringType()),
    StructField("Occupation", StringType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

CLICKSTREAM_SCHEMA = StructType([
    *[StructField(f"fe_{i}", IntegerType()) for i in range(1, 21)],
    StructField("Customer_ID", StringType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

FINANCIALS_SCHEMA = StructType([
    StructField("Customer_ID", StringType(), True),
    StructField("Annual_Income", StringType(), True),
    StructField("Monthly_Inhand_Salary", DoubleType(), True),
    StructField("Num_Bank_Accounts", IntegerType(), True),
    StructField("Num_Credit_Card", IntegerType(), True),
    StructField("Interest_Rate", IntegerType(), True),
    StructField("Num_of_Loan", StringType(), True),
    StructField("Type_of_Loan", StringType(), True),
    StructField("Delay_from_due_date", IntegerType(), True),
    StructField("Num_of_Delayed_Payment", StringType(), True),
    StructField("Changed_Credit_Limit", StringType(), True),
    StructField("Num_Credit_Inquiries", IntegerType(), True),
    StructField("Credit_Mix", StringType(), True),
    StructField("Outstanding_Debt", StringType(), True),
    StructField("Credit_Utilization_Ratio", DoubleType(), True),
    StructField("Credit_History_Age", StringType(), True),
    StructField("Payment_of_Min_Amount", StringType(), True),
    StructField("Total_EMI_per_month", DoubleType(), True),
    StructField("Amount_invested_monthly", StringType(), True),
    StructField("Payment_Behaviour", StringType(), True),
    StructField("Monthly_Balance", StringType(), True),
    StructField("_metadata_file_name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("snapshot_date", DateType(), True)
])

CLEANED_LMS_LOAN_SCHEMA = StructType([
    StructField("loan_id", StringType()),
    StructField("Customer_ID", StringType()),
    StructField("loan_start_date", DateType()),
    StructField("tenure", IntegerType()),
    StructField("installment_num", IntegerType()),
    StructField("loan_amt", IntegerType()),
    StructField("due_amt", FloatType()),
    StructField("paid_amt", FloatType()),
    StructField("overdue_amt", FloatType()),
    StructField("balance", FloatType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

CLEANED_FINANCIALS_SCHEMA = StructType([
    StructField("Customer_ID", StringType(), nullable=True),
    StructField("Annual_Income", FloatType(), nullable=True),
    StructField("Monthly_Inhand_Salary", FloatType(), nullable=True),
    StructField("Num_Bank_Accounts", StringType(), nullable=True),
    StructField("Num_Credit_Card", IntegerType(), nullable=True),
    StructField("Interest_Rate", FloatType(), nullable=True),
    StructField("Num_of_Loan", IntegerType(), nullable=True),
    StructField("Delay_from_due_date", StringType(), nullable=True),
    StructField("Num_of_Delayed_Payment", FloatType(), nullable=True),
    StructField("Changed_Credit_Limit", FloatType(), nullable=True),
    StructField("Num_Credit_Inquiries", IntegerType(), nullable=True),
    StructField("Credit_Mix", IntegerType(), nullable=True),
    StructField("Outstanding_Debt", FloatType(), nullable=True),
    StructField("Credit_Utilization_Ratio", FloatType(), nullable=True),
    StructField("Payment_of_Min_Amount", IntegerType(), nullable=True),
    StructField("Total_EMI_per_month", FloatType(), nullable=True),
    StructField("Amount_invested_monthly", FloatType(), nullable=True),
    StructField("Payment_Behaviour", IntegerType(), nullable=True),
    StructField("Monthly_Balance", FloatType(), nullable=True),
    StructField("_metadata_file_name", StringType(), nullable=True),
    StructField("snapshot_date", DateType(), nullable=True),
    StructField("Credit_History_Years", IntegerType(), nullable=True),
    StructField("Credit_History_Months", IntegerType(), nullable=True),
    StructField("Has_Credit-Builder_Loan", IntegerType(), nullable=True),
    StructField("Has_Student_Loan", IntegerType(), nullable=True),
    StructField("Has_Mortgage_Loan", IntegerType(), nullable=True),
    StructField("Has_Payday_Loan", IntegerType(), nullable=True),
    StructField("Has_Personal_Loan", IntegerType(), nullable=True),
    StructField("Has_Debt_Consolidation_Loan", IntegerType(), nullable=True),
    StructField("Has_Auto_Loan", IntegerType(), nullable=True),
    StructField("Has_Home_Equity_Loan", IntegerType(), nullable=True)
])

CLEANED_ATTRIBUTES_SCHEMA = StructType([
    StructField("Customer_ID", StringType()),
    StructField("Name", StringType()),
    StructField("Age", IntegerType()),
    StructField("SSN", StringType()),
    StructField("Occupation", StringType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

CLEANED_CLICKSTREAM_SCHEMA = StructType([
    *[StructField(f"fe_{i}", IntegerType()) for i in range(1, 21)],
    StructField("Customer_ID", StringType()),
    StructField("_metadata_file_name", StringType()),
    StructField("date", DateType()), 
    StructField("snapshot_date", DateType())
])

SCHEMA_MAP = {
    "lms_loan_daily": LMS_LOAN_SCHEMA,
    "features_attributes": ATTRIBUTES_SCHEMA,
    "feature_clickstream": CLICKSTREAM_SCHEMA,
    "features_financials": FINANCIALS_SCHEMA
}

CLEANED_SCHEMA_MAP = {
    "features_financials": CLEANED_FINANCIALS_SCHEMA,
    "lms_loan_daily": CLEANED_LMS_LOAN_SCHEMA,
    "features_attributes": CLEANED_ATTRIBUTES_SCHEMA,
    "feature_clickstream": CLEANED_CLICKSTREAM_SCHEMA
}


class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def validate_parquet_schema(self, directory_path, expected_schema):
        parquet_schema = self.spark.read.parquet(directory_path).schema
        if parquet_schema != expected_schema:
            logger.error(f"Schema mismatch detected:\nExpected: {expected_schema}\nFound: {parquet_schema}")
            raise ValueError("Schema mismatch between expected and actual Parquet files.")
        logger.info("Schema validation passed.")

    def read_parquet(self, path, schema):
        try:
            return self.spark.read.schema(schema).parquet(path)
        except Exception as e:
            logger.error(f"Read failed for {path}: {e}")
            raise

    def write_parquet(self, df, path, source_type):
        try:
            expected_schema = CLEANED_SCHEMA_MAP[source_type]
            cleaned_df = df.select(expected_schema.fieldNames())
            cleaned_df.repartition("snapshot_date").write \
                .partitionBy("snapshot_date") \
                .mode("append") \
                .parquet(path)
        except Exception as e:
            logger.error(f"Write failed to {path}: {e}")
            raise

    def clean_loan_types(self, df):
        return df.withColumn(
            "Loan_Types",
            array_distinct(split(regexp_replace(col("Type_of_Loan"), r",\s*and\s*", ","), ",\\s*"))
        ).withColumn(
            "Loan_Types",
            expr("filter(Loan_Types, x -> x != '')")
        )

    def create_loan_flags(self, df, loan_types):
        for loan in loan_types:
            df = df.withColumn(
                f"Has_{loan.replace(' ', '_')}",
                array_contains(col("Loan_Types"), loan).cast("int")
            )
        return df.drop("Loan_Types", "Type_of_Loan")

    def process_lms_loan(self, df):
        return df \
            .withColumn("loan_start_date", to_date(col("loan_start_date"), "yyyy-MM-dd")) \
            .withColumn("due_amt", regexp_replace(col("due_amt"), "_", "").cast(FloatType())) \
            .withColumn("tenure", regexp_replace(col("tenure"), "_", "").cast(IntegerType())) \
            .withColumn("installment_num", regexp_replace(col("installment_num"), "_", "").cast(IntegerType())) \
            .withColumn("loan_amt", regexp_replace(col("loan_amt"), "_", "").cast(FloatType())) \
            .withColumn("paid_amt", regexp_replace(col("paid_amt"), "_", "").cast(FloatType())) \
            .withColumn("overdue_amt", regexp_replace(col("overdue_amt"), "_", "").cast(FloatType())) \
            .withColumn("balance", regexp_replace(col("balance"), "_", "").cast(FloatType())) \
            .withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))

    def process_financials(self, df):
        return df \
            .withColumn("Annual_Income", regexp_replace("Annual_Income", "_", "").cast(FloatType())) \
            .withColumn("Num_of_Loan", regexp_replace("Num_of_Loan", "_", "").cast(IntegerType())) \
            .withColumn("Num_of_Delayed_Payment", 
                       regexp_replace("Num_of_Delayed_Payment", "_", "").cast(FloatType())) \
            .withColumn("Payment_Behaviour",
                when(col("Payment_Behaviour") == "!@9#%8", None)
                 .otherwise(col("Payment_Behaviour"))
            ) \
            .withColumn("Credit_History_Years",
                regexp_extract("Credit_History_Age", "(\\d+) Years", 1).cast(IntegerType())
            ) \
            .withColumn("Credit_History_Months",
                regexp_extract("Credit_History_Age", "(\\d+) Months", 1).cast(IntegerType())
            ) \
            .transform(self.clean_loan_types) \
            .transform(lambda df: self.create_loan_flags(df, [
                "Credit-Builder Loan", "Student Loan", "Mortgage Loan",
                "Payday Loan", "Personal Loan", "Debt Consolidation Loan",
                "Auto Loan", "Home Equity Loan"
            ])) \
            .withColumn("Payment_Behaviour",
                when(col("Payment_Behaviour") == "Good", 1)
                 .when(col("Payment_Behaviour") == "Bad", 0)
                 .otherwise(None)
            ) \
            .withColumn("Credit_Mix",
                when(col("Credit_Mix") == "Standard", 2)
                 .when(col("Credit_Mix") == "Good", 1)
                 .when(col("Credit_Mix") == "Bad", 0)
                 .otherwise(None)
            ) \
            .withColumn("Payment_of_Min_Amount",
                when(col("Payment_of_Min_Amount") == "Yes", 1)
                 .when(col("Payment_of_Min_Amount") == "No", 0)
                 .otherwise(None)
            ) \
            .drop("Credit_History_Age")

    def process_attributes(self, df):
        return df \
            .withColumn("Age", 
                regexp_replace(col("Age"), "_", "").cast(IntegerType())
            ) \
            .transform(lambda df: self.remove_outliers(df, ["Age"]))

    def remove_outliers(self, df, columns, threshold=1.5):
        for col_name in columns:
            bounds = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
            if bounds:
                Q1, Q3 = bounds
                IQR = Q3 - Q1
                df = df.filter(
                    (col(col_name) >= Q1 - threshold*IQR) &
                    (col(col_name) <= Q3 + threshold*IQR)
                )
        return df

    def validate_processed_schema(self, df, expected_schema):
        if df.schema != expected_schema:
            logger.error(f"Schema mismatch detected:Expected-{expected_schema} Found: {df.schema}")
            raise ValueError("Schema mismatch in processed data.")
        logger.info("Processed schema validation passed.")

    def process_bronze_to_silver(self, bronze_path, silver_path, dates, source_type):
        
        for date_str in dates:
            try:
                schema = SCHEMA_MAP[source_type]
                self.validate_parquet_schema(bronze_path, schema)
                df = self.read_parquet(bronze_path, schema) \
                    .filter(col("snapshot_date") == date_str) \
                    .drop("date") \
                    .dropDuplicates()

                logger.info(f"Initial DataFrame count after filtering for {date_str}: {df.count()}")

                if source_type == 'lms_loan_daily':
                    processed_df = self.process_lms_loan(df)
                elif source_type == 'features_financials':
                    processed_df = self.process_financials(df)
                elif source_type == 'features_attributes':
                    processed_df = self.process_attributes(df)
                elif source_type == 'feature_clickstream':
                    processed_df = df

                logger.info(f"Processed DataFrame count for {date_str}: {processed_df.count()}")
                logger.info(f"Processed DataFrame schema for {date_str}: {processed_df.schema}")

                expected_schema = CLEANED_SCHEMA_MAP[source_type]
                self.validate_processed_schema(processed_df, expected_schema)
                logger.debug(f"Writing processed data to {silver_path} for {date_str}")
                self.write_parquet(processed_df, silver_path, source_type)
                logger.debug(f"schema is {processed_df.printSchema()}")
                logger.info(f"Successfully processed {source_type} for {date_str}")
                
            except Exception as e:
                logger.error(f"Processing failed for {date_str}: {e}")