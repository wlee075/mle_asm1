from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
from pyspark.sql import functions as F
from datetime import datetime, timedelta

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

LMS_LOAN_SCHEMA = StructType(
    [
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
        StructField("snapshot_date", DateType()),
    ]
)

ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("Customer_ID", StringType()),
        StructField("Name", StringType()),
        StructField("Age", StringType()),
        StructField("SSN", StringType()),
        StructField("Occupation", StringType()),
        StructField("_metadata_file_name", StringType()),
        StructField("date", DateType()),
        StructField("snapshot_date", DateType()),
    ]
)

CLICKSTREAM_SCHEMA = StructType(
    [
        *[StructField(f"fe_{i}", IntegerType()) for i in range(1, 21)],
        StructField("Customer_ID", StringType()),
        StructField("_metadata_file_name", StringType()),
        StructField("date", DateType()),
        StructField("snapshot_date", DateType()),
    ]
)

FINANCIALS_SCHEMA = StructType(
    [
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
        StructField("snapshot_date", DateType(), True),
    ]
)

CLEANED_LMS_LOAN_SCHEMA = StructType(
    [
        StructField("loan_id", StringType()),
        StructField("Customer_ID", StringType()),
        StructField("loan_start_date", DateType()),
        StructField("tenure", IntegerType()),
        StructField("installment_num", IntegerType()),
        StructField("loan_amt", FloatType()),
        StructField("due_amt", FloatType()),
        StructField("paid_amt", FloatType()),
        StructField("overdue_amt", FloatType()),
        StructField("balance", FloatType()),
        StructField("_metadata_file_name", StringType()),
        StructField("snapshot_date", DateType()),
    ]
)

CLEANED_FINANCIALS_SCHEMA = StructType(
    [
        StructField("Customer_ID", StringType(), nullable=True),
        StructField("Annual_Income", FloatType(), nullable=True),
        StructField("Monthly_Inhand_Salary", FloatType(), nullable=True),
        StructField("Num_Bank_Accounts", IntegerType(), nullable=True),
        StructField("Num_Credit_Card", IntegerType(), nullable=True),
        StructField("Interest_Rate", IntegerType(), nullable=True),
        StructField("Num_of_Loan", IntegerType(), nullable=True),
        StructField("Delay_from_due_date", IntegerType(), nullable=True),
        StructField("Num_of_Delayed_Payment", FloatType(), nullable=True),
        StructField("Changed_Credit_Limit", FloatType(), nullable=True),
        StructField("Num_Credit_Inquiries", IntegerType(), nullable=True),
        StructField("Credit_Mix", IntegerType(), nullable=True),
        StructField("Outstanding_Debt", FloatType(), nullable=True),
        StructField("Credit_Utilization_Ratio", FloatType(), nullable=True),
        StructField("Payment_of_Min_Amount", IntegerType(), nullable=True),
        StructField("Total_EMI_per_month", FloatType(), nullable=True),
        StructField("Amount_invested_monthly", FloatType(), nullable=True),
        StructField("Payment_Behaviour", StringType(), nullable=True),
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
        StructField("Has_Home_Equity_Loan", IntegerType(), nullable=True),
    ]
)

CLEANED_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("Customer_ID", StringType()),
        StructField("Name", StringType()),
        StructField("Age", IntegerType()),
        StructField("SSN", StringType()),
        StructField("Occupation", StringType()),
        StructField("_metadata_file_name", StringType()),
        StructField("snapshot_date", DateType()),
    ]
)

CLEANED_CLICKSTREAM_SCHEMA = StructType(
    [
        *[StructField(f"fe_{i}", IntegerType()) for i in range(1, 21)],
        StructField("Customer_ID", StringType()),
        StructField("_metadata_file_name", StringType()),
        StructField("snapshot_date", DateType()),
    ]
)

SCHEMA_MAP = {
    "lms_loan_daily": LMS_LOAN_SCHEMA,
    "features_attributes": ATTRIBUTES_SCHEMA,
    "feature_clickstream": CLICKSTREAM_SCHEMA,
    "features_financials": FINANCIALS_SCHEMA,
}

CLEANED_SCHEMA_MAP = {
    "features_financials": CLEANED_FINANCIALS_SCHEMA,
    "lms_loan_daily": CLEANED_LMS_LOAN_SCHEMA,
    "features_attributes": CLEANED_ATTRIBUTES_SCHEMA,
    "feature_clickstream": CLEANED_CLICKSTREAM_SCHEMA,
}


class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def validate_parquet_schema(self, directory_path, expected_schema):
        parquet_schema = self.spark.read.parquet(directory_path).schema
        if parquet_schema != expected_schema:
            logger.error(
                f"Schema mismatch detected:\nExpected: {expected_schema}\nFound: {parquet_schema}"
            )
            raise ValueError(
                "Schema mismatch between expected and actual Parquet files."
            )
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
            if self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            ).exists(self.spark._jvm.org.apache.hadoop.fs.Path(path)):
                existing_df = self.spark.read.parquet(path)
                relevant_columns = [
                    col
                    for col in cleaned_df.columns
                    if col not in ["snapshot_date", "_metadata_file_name"]
                ]
                if set(relevant_columns) != set(
                    [
                        col
                        for col in existing_df.columns
                        if col not in ["snapshot_date", "_metadata_file_name"]
                    ]
                ):
                    raise ValueError(
                        "Schema mismatch between incoming DataFrame and existing Parquet file."
                    )
                cleaned_df = cleaned_df.repartition("snapshot_date")
                existing_df = existing_df.repartition("snapshot_date")
                cleaned_df = cleaned_df.withColumn(
                    "unique_id", F.concat_ws("_", *cleaned_df.columns)
                )
                existing_df = existing_df.withColumn(
                    "unique_id", F.concat_ws("_", *existing_df.columns)
                )
                common_rows = cleaned_df.select("unique_id").intersect(
                    existing_df.select("unique_id")
                )
                if common_rows.count() == cleaned_df.count():
                    logger.info(
                        f"No new data to append to {path}. Skipping write operation."
                    )
                    return
                cleaned_df = cleaned_df.drop("unique_id")
            cleaned_df.repartition("snapshot_date").write.partitionBy(
                "snapshot_date"
            ).mode("append").parquet(path)
            logger.info(f"Data successfully appended to {path}.")

        except Exception as e:
            logger.error(f"Write failed to {path}: {e}")
            raise

    def clean_loan_types(self, df):
        return df.withColumn(
            "Loan_Types",
            array_distinct(
                split(regexp_replace(col("Type_of_Loan"), r",\s*and\s*", ","), ",\\s*")
            ),
        ).withColumn("Loan_Types", expr("filter(Loan_Types, x -> x != '')"))

    def create_loan_flags(self, df, loan_types):
        for loan in loan_types:
            df = df.withColumn(
                f"Has_{loan.replace(' ', '_')}",
                array_contains(col("Loan_Types"), loan).cast("int"),
            )
        return df.drop("Loan_Types", "Type_of_Loan")

    def process_lms_loan(self, df):
        result = (
            df.withColumn(
                "loan_start_date", to_date(col("loan_start_date"), "yyyy-MM-dd")
            )
            .withColumn(
                "due_amt", regexp_replace(col("due_amt"), "_", "").cast(FloatType())
            )
            .withColumn(
                "tenure", regexp_replace(col("tenure"), "_", "").cast(IntegerType())
            )
            .withColumn(
                "installment_num",
                regexp_replace(col("installment_num"), "_", "").cast(IntegerType()),
            )
            .withColumn(
                "loan_amt", regexp_replace(col("loan_amt"), "_", "").cast(FloatType())
            )
            .withColumn(
                "paid_amt", regexp_replace(col("paid_amt"), "_", "").cast(FloatType())
            )
            .withColumn(
                "overdue_amt",
                regexp_replace(col("overdue_amt"), "_", "").cast(FloatType()),
            )
            .withColumn(
                "balance", regexp_replace(col("balance"), "_", "").cast(FloatType())
            )
            .withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        )
        return result

    def process_financials(self, df):
        result = (
            df.withColumn(
                "Annual_Income",
                regexp_replace("Annual_Income", "_", "").cast(FloatType()),
            )
            .withColumn(
                "Changed_Credit_Limit", col("Changed_Credit_Limit").cast("float")
            )
            .withColumn(
                "Credit_Utilization_Ratio",
                col("Credit_Utilization_Ratio").cast("float"),
            )
            .withColumn(
                "Amount_invested_monthly", col("Amount_invested_monthly").cast("float")
            )
            .withColumn("Monthly_Balance", col("Monthly_Balance").cast("float"))
            .withColumn("Total_EMI_per_month", col("Total_EMI_per_month").cast("float"))
            .withColumn("Outstanding_Debt", col("Outstanding_Debt").cast("float"))
            .withColumn(
                "Monthly_Inhand_Salary",
                regexp_replace("Monthly_Inhand_Salary", "_", "").cast(FloatType()),
            )
            .withColumn(
                "Num_of_Loan",
                when(col("Num_of_Loan").cast("int") < 0, None).otherwise(
                    regexp_replace("Num_of_Loan", "_", "").cast(IntegerType())
                ),
            )
            .withColumn(
                "Num_of_Delayed_Payment",
                regexp_replace("Num_of_Delayed_Payment", "_", "").cast(FloatType()),
            )
            .withColumn(
                "Payment_Behaviour",
                when(col("Payment_Behaviour") == "!@9#%8", None).otherwise(
                    col("Payment_Behaviour")
                ),
            )
            .withColumn(
                "Credit_History_Years",
                regexp_extract("Credit_History_Age", "(\\d+) Years", 1).cast(
                    IntegerType()
                ),
            )
            .withColumn(
                "Credit_History_Months",
                regexp_extract("Credit_History_Age", "(\\d+) Months", 1).cast(
                    IntegerType()
                ),
            )
            .transform(self.clean_loan_types)
            .transform(
                lambda df: self.create_loan_flags(
                    df,
                    [
                        "Credit-Builder Loan",
                        "Student Loan",
                        "Mortgage Loan",
                        "Payday Loan",
                        "Personal Loan",
                        "Debt Consolidation Loan",
                        "Auto Loan",
                        "Home Equity Loan",
                    ],
                )
            )
            .withColumn(
                "Credit_Mix",
                when(col("Credit_Mix") == "Standard", 2)
                .when(col("Credit_Mix") == "Good", 1)
                .when(col("Credit_Mix") == "Bad", 0)
                .otherwise(None),
            )
            .withColumn(
                "Payment_of_Min_Amount",
                when(col("Payment_of_Min_Amount") == "Yes", 1)
                .when(col("Payment_of_Min_Amount") == "No", 0)
                .otherwise(None),
            )
            .drop("Credit_History_Age")
        )
        return result

    def process_attributes(self, df):
        return (
            df.withColumn(
                "Age", regexp_replace(col("Age"), "_", "").cast(IntegerType())
            )
            .transform(lambda df: self.remove_outliers(df, ["Age"]))
            .withColumn(
                "SSN",
                when(col("SSN").rlike("^\\d{3}-\\d{2}-\\d{4}$"), col("SSN")).otherwise(
                    None
                ),
            )
        )

    def remove_outliers(self, df, columns, threshold=1.5):
        for col_name in columns:
            bounds = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
            if bounds:
                Q1, Q3 = bounds
                IQR = Q3 - Q1
                df = df.filter(
                    (col(col_name) >= Q1 - threshold * IQR)
                    & (col(col_name) <= Q3 + threshold * IQR)
                )
        return df

    def validate_processed_schema(self, df, expected_schema):
        if df.schema != expected_schema:
            logger.error(
                f"Schema mismatch detected:Expected-{expected_schema} Found: {df.schema}"
            )
            raise ValueError("Schema mismatch in processed data.")
        logger.info("Processed schema validation passed.")

    def path_exists(self, local_path):
        return os.path.exists(local_path)

    def process_bronze_to_silver(
        self, bronze_path, silver_path, dates, source_type, retention_days=30
    ):
        for date_str in dates:
            try:
                schema = SCHEMA_MAP[source_type]
                self.validate_parquet_schema(bronze_path, schema)
                df = (
                    self.read_parquet(bronze_path, schema)
                    .filter(col("snapshot_date") == date_str)
                    .drop("date")
                )
                silver_snapshot_path = f"{silver_path}/snapshot_date={date_str}"
                if self.path_exists(silver_snapshot_path):
                    logger.info(
                        f"Data for {date_str} already exists in silver. Skipping."
                    )
                    continue
                if source_type == "lms_loan_daily":
                    processed_df = self.process_lms_loan(df)
                elif source_type == "features_financials":
                    processed_df = self.process_financials(df)
                elif source_type == "features_attributes":
                    processed_df = self.process_attributes(df)
                elif source_type == "feature_clickstream":
                    processed_df = df
                processed_df = processed_df.dropna()
                logger.info(
                    f"Processed DataFrame count for {date_str}: {processed_df.count()}"
                )
                logger.info(
                    f"Processed DataFrame schema for {date_str}: {processed_df.schema}"
                )
                expected_schema = CLEANED_SCHEMA_MAP[source_type]
                self.validate_processed_schema(processed_df, expected_schema)
                logger.debug(f"Writing processed data to {silver_path} for {date_str}")
                self.write_parquet(processed_df, silver_path, source_type=source_type)
                logger.info(f"Successfully processed {source_type} for {date_str}")

            except Exception as e:
                logger.error(f"Processing failed for {date_str}: {e}")


class SilverDataMart:
    def __init__(self, spark: SparkSession, silver_root: str = "datamart/silver"):
        self.spark = spark
        self.silver_root = silver_root
        logger.info("Loading cleaned silver inputs …")
        self.lms_df = spark.read.parquet(f"{silver_root}/lms_loan_daily_cleaned")
        self.cs_df = spark.read.parquet(f"{silver_root}/feature_clickstream_cleaned")
        self.fa_df = spark.read.parquet(f"{silver_root}/features_attributes_cleaned")
        self.ff_df = spark.read.parquet(f"{silver_root}/features_financials_cleaned")

        logger.info(
            "Input row counts – "
            f"LMS: {self.lms_df.count():,}, "
            f"Clickstream: {self.cs_df.count():,}, "
            f"Attr: {self.fa_df.count():,}, "
            f"Fin: {self.ff_df.count():,}"
        )

        self.tables = {}

    @property
    def snapshot_df(self):
        if "_snapshot" not in self.tables:
            sdf = (
                self.lms_df.select("snapshot_date", "_metadata_file_name")
                .union(self.cs_df.select("snapshot_date", "_metadata_file_name"))
                .union(self.fa_df.select("snapshot_date", "_metadata_file_name"))
                .union(self.ff_df.select("snapshot_date", "_metadata_file_name"))
                .distinct()
                .withColumn("snapshot_id", F.monotonically_increasing_id())
            )
            self.tables["_snapshot"] = sdf
        return self.tables["_snapshot"]

    def build_dim_customer(self):
        df = self.fa_df.select(
            F.col("Customer_ID").alias("customer_id"),
            "SSN",
            "Name",
            "Age",
            "Occupation",
        ).distinct()
        self.tables["dim_customer"] = df

    def build_dim_feature(self, n_features: int = 20):
        df = self.spark.createDataFrame(
            [(i, f"fe_{i}") for i in range(1, n_features + 1)],
            ["feature_id", "feature_name"],
        )
        self.tables["dim_feature"] = df

    def build_dim_credit_mix(self):
        df = self.ff_df.select(F.col("Credit_Mix").alias("credit_mix_id")).distinct()
        self.tables["dim_credit_mix"] = df

    def build_dim_payment_behaviour(self):
        df = (
            self.ff_df.select(F.col("Payment_Behaviour").alias("payment_behaviour_id"))
            .withColumn("spend", split(col("payment_behaviour_id"), "_").getItem(0))
            .withColumn("value", split(col("payment_behaviour_id"), "_").getItem(2))
            .distinct()
        )
        self.tables["dim_payment_behaviour"] = df

    def build_dim_min_payment(self):
        df = self.ff_df.select(
            F.col("Payment_of_Min_Amount").alias("payment_code")
        ).distinct()
        self.tables["dim_min_payment"] = df

    def build_dim_loan_type(self):
        loan_flag_cols = [c for c in self.ff_df.columns if c.startswith("Has_")]
        df = self.spark.createDataFrame(
            [(i + 1, col) for i, col in enumerate(loan_flag_cols)],
            ["loan_type_id", "loan_flag_col"],
        ).withColumn("loan_type_name", F.expr("replace(loan_flag_col,'Has_','')"))
        self.tables["dim_loan_type"] = df
        self.tables["_loan_flag_cols"] = loan_flag_cols

    def build_fact_clickstream(self):
        feature_cols = [f"fe_{i}" for i in range(1, 21)]
        expr_str = ",".join([f"'{c}', {c}" for c in feature_cols])

        cs_unpivot = self.cs_df.select(
            F.col("Customer_ID").alias("customer_id"),
            "snapshot_date",
            "_metadata_file_name",
            F.expr(
                f"stack({len(feature_cols)}, {expr_str}) " "as (feature_name, value)"
            ),
        )

        df = (
            cs_unpivot.join(
                self.snapshot_df,
                on=["snapshot_date", "_metadata_file_name"],
                how="left",
            )
            .join(self.tables["dim_feature"], on="feature_name", how="left")
            .select("customer_id", "feature_id", "value", "snapshot_id")
        )
        self.tables["fact_clickstream"] = df

    def build_fact_financials(self):
        df = (
            self.ff_df.alias("f")
            .join(self.snapshot_df, on=["snapshot_date", "_metadata_file_name"])
            .select(
                F.col("f.Customer_ID").alias("customer_id"),
                "snapshot_id",
                "Annual_Income",
                "Monthly_Inhand_Salary",
                "Num_Bank_Accounts",
                "Num_Credit_Card",
                "Interest_Rate",
                "Num_of_Loan",
                "Delay_from_due_date",
                "Num_of_Delayed_Payment",
                "Changed_Credit_Limit",
                "Num_Credit_Inquiries",
                "Outstanding_Debt",
                "Credit_Utilization_Ratio",
                "Total_EMI_per_month",
                "Amount_invested_monthly",
                F.col("f.Payment_of_Min_Amount").alias("payment_code"),
                F.col("f.Payment_Behaviour").alias("payment_behaviour_id"),
                F.col("f.Credit_Mix").alias("credit_mix_id"),
                "Credit_History_Years",
                "Credit_History_Months",
            )
        )
        self.tables["fact_financials"] = df

    def build_fact_loan(self):
        df = (
            self.lms_df.alias("l")
            .join(self.snapshot_df, on=["snapshot_date", "_metadata_file_name"])
            .select(
                F.col("l.loan_id"),
                F.col("l.Customer_ID").alias("customer_id"),
                "snapshot_id",
                "loan_start_date",
                "tenure",
                "installment_num",
                "loan_amt",
                "due_amt",
                "paid_amt",
                "overdue_amt",
                "balance",
            )
        )
        self.tables["fact_loan"] = df

    def _quote(self, col_name: str) -> str:
        specials = {"-", " ", "/", ".", "+"}
        return f"`{col_name}`" if any(ch in col_name for ch in specials) else col_name
    
    
    def build_fact_customer_loan_type(self):
        loan_flag_cols = self.tables["_loan_flag_cols"]
        flag_expr = ",".join(
            [f"'{c}', {self._quote(c)}" for c in loan_flag_cols]
        )
        loan_flags_unpivot = (
            self.ff_df
            .select(
                F.col("Customer_ID").alias("customer_id"),
                "snapshot_date", "_metadata_file_name",
                F.expr(
                    f"stack({len(loan_flag_cols)}, {flag_expr}) "
                    "as (loan_flag_col, flag_value)"
                )
            )
            .filter("flag_value = 1")
            .join(self.snapshot_df, on=["snapshot_date", "_metadata_file_name"])
            .join(self.tables["dim_loan_type"], on="loan_flag_col")
            .select("customer_id", "loan_type_id", "snapshot_id")
        )
        self.tables["fact_customer_loan_type"] = loan_flags_unpivot

    def run(self, write_mode: str = "overwrite", preview: bool = False):
        _ = self.snapshot_df
        self.build_dim_customer()
        self.build_dim_feature()
        self.build_dim_credit_mix()
        self.build_dim_payment_behaviour()
        self.build_dim_min_payment()
        self.build_dim_loan_type()

        self.build_fact_clickstream()
        self.build_fact_financials()
        self.build_fact_loan()
        self.build_fact_customer_loan_type()

        for name, df in self.tables.items():
            if name.startswith("_"):
                continue
            path = f"{self.silver_root}/{name}"
            logger.info(f"Writing {name}  to  {path}")
            df.write.mode(write_mode).parquet(path)

        if preview:
            for name, df in self.tables.items():
                if name.startswith("_"):
                    continue
                logger.info(f"\nSchema – {name}")
                df.printSchema()
                df.show(5, truncate=False)

