from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging, os
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import functions as F

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"gold_pipeline_{datetime.now():%Y-%m-%d}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

SILVER_PATHS = {
    "fact_loan": "datamart/silver/fact_loan",
    "fact_financials": "datamart/silver/fact_financials",
    "fact_customer_loan_type": "datamart/silver/fact_customer_loan_type",
    "fact_clickstream": "datamart/silver/fact_clickstream",
    "dim_customer": "datamart/silver/dim_customer",
    "dim_credit_mix": "datamart/silver/dim_credit_mix",
    "dim_payment_behaviour": "datamart/silver/dim_payment_behaviour",
    "dim_min_payment": "datamart/silver/dim_min_payment",
    "dim_loan_type": "datamart/silver/dim_loan_type",
    "dim_feature": "datamart/silver/dim_feature",
    "dim_snapshot": "datamart/silver/dim_snapshot",
}

GOLD_PATHS = {
    "loan_analytics": "datamart/gold/loan_analytics_star",
    "customer_summary": "datamart/gold/customer_summary",
}


class GoldDataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_silver(self, name: str):
        path = SILVER_PATHS[name]
        logger.info(f"Reading Silver table '{name}' from {path}")
        return self.spark.read.parquet(path)

    def write_gold(self, df, name: str, partition_cols=None):
        path = GOLD_PATHS[name]
        mode = "overwrite"
        if partition_cols:
            w = df.repartition(*partition_cols)
        else:
            w = df
        logger.info(f"Writing Gold table '{name}' to {path}")
        (w.write.mode(mode).partitionBy(*(partition_cols or [])).parquet(path))

    def check_null(self, table, column):
        table.printSchema()
        unique_values = table.select(f"{column}").distinct().collect()
        unique_values_list = [row[f"{column}"] for row in unique_values]
        logger.info(f"TEST Unique values in column '{column}': {unique_values_list}")
        null_val = table.filter(F.col(f"{column}").isNull()).count()
        logger.warning(f"Unmatched {column} values in {table}: {null_val}")

    def build_loan_analytics_star(self):
        dim_credit_mix = self.read_silver("dim_credit_mix")
        dim_pay_behav = self.read_silver("dim_payment_behaviour")
        dim_min_pay = self.read_silver("dim_min_payment")
        dim_loan_type = self.read_silver("dim_loan_type")
        dim_snapshot = self.read_silver("dim_snapshot")
        fact_loan = self.read_silver("fact_loan")
        dim_customer = self.read_silver("dim_customer")
        fact_financials = self.read_silver("fact_financials")
        fact_cust_loan = self.read_silver("fact_customer_loan_type")
        fact_financials = fact_financials.drop(fact_financials["snapshot_id"])
        fact_loan = fact_loan.drop(fact_loan["snapshot_id"])
        star = (
            fact_loan.join(dim_customer, "customer_id")
            .join(fact_financials, "customer_id", "inner")
            .join(fact_cust_loan, "customer_id", "left")
            .cache()
        )

        star = star.join(dim_credit_mix, "credit_mix_id", "left")
        logger.info(f"Rows after dim_credit_mix join: {star.count()}")
        null_credit_mix = star.filter(F.col("credit_mix_id").isNull()).count()
        logger.warning(f"Rows with NULL credit_mix_id: {null_credit_mix}")
        star = star.join(dim_pay_behav, "payment_behaviour_id", "left")
        logger.info(f"Rows after payment_behaviour join: {star.count()}")
        null_pay_behav = star.filter(F.col("payment_behaviour_id").isNull()).count()
        logger.warning(f"Rows with NULL payment_behaviour_id: {null_pay_behav}")
        star = star.join(dim_min_pay, ["payment_code"], "left")
        logger.info(f"Rows after min_payment join: {star.count()}")
        null_payment_code = star.filter(F.col("payment_code").isNull()).count()
        star = star.join(dim_loan_type, "loan_type_id", "left")
        logger.info(f"Rows after loan_type join: {star.count()}")
        null_loan_type = star.filter(F.col("loan_type_id").isNull()).count()
        star = (
            star.alias("s")
            .join(
                dim_snapshot.alias("ds"),
                F.col("s.snapshot_id") == F.col("ds.snapshot_id"),
                "left",
            )
            .select(
                F.col("s.*"),
                F.col("ds.snapshot_date"),
            )
        )
        null_snapshot = star.filter(F.col("snapshot_date").isNull()).count()
        star = star.select(
            "loan_id",
            "customer_id",
            "SSN",
            "Name",
            "Age",
            "Occupation",
            "credit_mix_id",
            "payment_behaviour_id",
            "payment_code",
            "loan_type_id",
            "loan_flag_col",
            "snapshot_date",
            "loan_start_date",
            "tenure",
            "installment_num",
            "loan_amt",
            "due_amt",
            "paid_amt",
            "overdue_amt",
            "balance",
        )
        total_nulls = (
            null_credit_mix
            + null_pay_behav
            + null_payment_code
            + null_loan_type
            + null_snapshot
        )
        logger.info(
            f"Total rows with NULL dimension keys: {total_nulls}/{star.count()}"
        )
        self.write_gold(star, "loan_analytics", partition_cols=["snapshot_date"])

    def build_customer_summary(self):
        dim_customer = self.read_silver("dim_customer")
        fact_fin = self.read_silver("fact_financials")
        fact_click = self.read_silver("fact_clickstream")
        dim_snapshot = self.read_silver("dim_snapshot")
        fact_fin_with_date = fact_fin.join(dim_snapshot, "snapshot_id")

        window = Window.partitionBy("customer_id").orderBy(F.desc("snapshot_date"))
        latest_fin = (
            fact_fin_with_date.withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn", "snapshot_date", "snapshot_id")
        )
        click_agg = fact_click.groupBy("customer_id").agg(
            sum("value").alias("total_events")
        )
        summary = (
            dim_customer.join(latest_fin, "customer_id")
            .join(click_agg, "customer_id", how="left")
            .select(
                "customer_id",
                "name",
                "age",
                "occupation",
                "Annual_Income",
                "Monthly_Inhand_Salary",
                "Outstanding_Debt",
                "total_events",
            )
        )
        self.write_gold(summary, "customer_summary")

    def run_all(self):
        self.build_loan_analytics_star()
        self.build_customer_summary()
