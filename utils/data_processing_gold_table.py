from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging, os
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from functools import reduce
import operator
from pyspark.sql.functions import col, when, lit

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
    "label_store":     "datamart/gold/label_store"
}


class GoldDataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_label_store(self, dpd_threshold: int, mob_threshold: int):
        logger.info("Reading silver fact_loan for all snapshot_dates")
        loan_df = self.read_silver("fact_loan")
        label_df = (
            loan_df
              .filter(col("mob") == mob_threshold)
              .withColumn(
                  "label",
                  when(col("dpd") >= dpd_threshold, 1).otherwise(0)
              )
              .withColumn(
                  "label_def",
                  lit(f"{dpd_threshold}dpd_{mob_threshold}mob")
              )
              .select(
                  "loan_id",
                  col("Customer_ID").alias("customer_id"),
                  "label",
                  "label_def",
                  "snapshot_date"
              )
        )
        logger.info(
            f"Writing label_store for all dates ({dpd_threshold}dpd/{mob_threshold}mob)"
        )
        self.write_gold(
            label_df,
            name="label_store",
            partition_cols=["snapshot_date"]
        )
        return label_df

    def read_silver(self, name: str):
        path = SILVER_PATHS[name]
        logger.info(f"Reading Silver table '{name}' from {path}")
        return self.spark.read.parquet(path)

    def write_gold(self, df, name: str, partition_cols=None):
        path = GOLD_PATHS[name]
        mode = "overwrite"
        writer = df
        if partition_cols:
            writer = df.repartition(*partition_cols)
            logger.info(
                f"Writing Gold table '{name}' to {path}, partitioned by {partition_cols}"
            )
            writer.write.mode(mode).partitionBy(*partition_cols).parquet(path)
        else:
            logger.info(f"Writing Gold table '{name}' to {path}")
            writer.write.mode(mode).parquet(path)

    def read_silver_features(self, snapshot_date_str: str):
        file_name = (
            f"silver_feature_store_{snapshot_date_str.replace('-', '_')}.parquet"
        )
        path = os.path.join(FEATURE_SILVER_DIR, file_name)
        logger.info(f"Reading Silver feature-store '{file_name}'")
        return self.spark.read.parquet(path)

    def write_gold_features(self, df, snapshot_date_str: str):
        file_name = f"gold_feature_store_{snapshot_date_str.replace('-', '_')}.parquet"
        path = os.path.join(FEATURE_GOLD_DIR, file_name)
        logger.info(f"Writing Gold feature-store to '{file_name}'")
        df.write.mode("overwrite").parquet(path)

    def remove_outliers(self, df, columns=None, threshold=1.5):
        if columns is None:
            columns = [
                "Age",
                "Interest_Rate",
                "Num_Bank_Accounts",
                "Num_Credit_Card",
                "Num_of_Loan",
                "Num_of_Delayed_Payment",
                "Num_Credit_Inquiries",
                "Total_EMI_per_month",
                "Amount_invested_monthly",
                "Monthly_Balance",
                "Annual_Income",
                "Outstanding_Debt",
                "Monthly_Inhand_Salary",
            ]

        conditions = []
        for c in columns:
            if c not in df.columns:
                continue

            q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            conditions.append((col(c) >= lower) & (col(c) <= upper))

        if not conditions:
            return df

        combined = reduce(operator.and_, conditions)
        return df.filter(combined)

    def apply_log_transformations_spark(self, df: DataFrame) -> DataFrame:
        min_balance_row = df.selectExpr("min(Monthly_Balance) as min_balance").first()
        min_balance = min_balance_row.min_balance if min_balance_row else None
        log_features = [
            "Annual_Income",
            "Monthly_Inhand_Salary",
            "Outstanding_Debt",
            "Total_EMI_per_month",
            "Amount_invested_monthly",
            "Num_Credit_Inquiries",
        ]
        new_cols = [
            log1p(col(f)).alias(f"{f}_Log") for f in log_features if f in df.columns
        ]
        if "Monthly_Balance" in df.columns and min_balance is not None:
            shifted = col("Monthly_Balance") - lit(min_balance) + lit(1)
            new_cols.append(log1p(shifted).alias("Monthly_Balance_Log"))
        return df.select("*", *new_cols)

    def add_financial_ratios(self, df):
        ratio_defs = [
            ("Debt_to_Income_Ratio", "Outstanding_Debt", "Annual_Income"),
            ("EMI_to_Income_Ratio", "Total_EMI_per_month", "Monthly_Inhand_Salary"),
            (
                "Invest_to_Income_Ratio",
                "Amount_invested_monthly",
                "Monthly_Inhand_Salary",
            ),
            ("Credit_Inquiry_to_Loan_Ratio", "Num_Credit_Inquiries", "Num_of_Loan"),
        ]

        new_cols = [
            when(col(den) > 0, col(num) / col(den)).otherwise(lit(0.0)).alias(name)
            for name, num, den in ratio_defs
            if num in df.columns and den in df.columns
        ]
        return df.select("*", *new_cols)

    def build_loan_analytics_star(self):
        multicollinear_raw_features = [
            "Annual_Income",
            "Monthly_Inhand_Salary",
            "Monthly_Balance",
            "Amount_invested_monthly",
            "Outstanding_Debt",
            "Total_EMI_per_month",
            "Monthly_Inhand_Salary_log",
        ]
        dim_credit_mix = self.read_silver("dim_credit_mix")
        dim_pay_behav = self.read_silver("dim_payment_behaviour")
        dim_min_pay = self.read_silver("dim_min_payment")
        dim_loan_type = self.read_silver("dim_loan_type")
        dim_customer = self.read_silver("dim_customer")
        fact_loan = self.read_silver("fact_loan")
        fact_fin_raw = self.read_silver("fact_financials")
        fact_fin_raw = fact_fin_raw.withColumn(
            "Monthly_Balance", col("Monthly_Inhand_Salary") - col("Total_EMI_per_month")
        ).join(
            dim_customer.select("customer_id", "Age", "Occupation"),
            on="customer_id",
            how="left",
        )
        fact_fin = fact_fin_raw.drop("Age", "Occupation")
        fact_cust_loan = self.read_silver("fact_customer_loan_type")
        star = (
            fact_loan.join(fact_fin, ["customer_id", "snapshot_date"], "inner")
            .join(fact_cust_loan, ["customer_id", "snapshot_date"], "left")
            .join(dim_customer, "customer_id")
            .join(dim_credit_mix, "credit_mix_id", "left")
            .join(dim_pay_behav, "payment_behaviour_id", "left")
            .join(dim_min_pay, ["payment_code"], "left")
            .join(dim_loan_type, "loan_type_id", "left")
            .transform(self.remove_outliers)
            .transform(lambda df: self.apply_log_transformations_spark(df))
            .transform(lambda df: self.add_financial_ratios(df))
            .withColumn(
                "Is_Employed", when(col("Occupation") == "employed", 1).otherwise(0)
            )
            .drop(*multicollinear_raw_features)
            .cache()
        )
        star = star.select(
            "customer_id",
            "Name",
            "Age",
            "Credit_Utilization_Ratio",
            "Credit_History_Years",
            "Credit_History_Months",
            "Annual_Income_Log",
            "Outstanding_Debt_Log",
            "Total_EMI_per_month_Log",
            "Amount_invested_monthly_Log",
            "Num_Credit_Inquiries_Log",
            "Monthly_Balance_Log",
            "Debt_to_Income_Ratio",
            "EMI_to_Income_Ratio",
            "Invest_to_Income_Ratio",
            "Credit_Inquiry_to_Loan_Ratio",
            "Is_Employed",
            "snapshot_date",
        )
        star.show()
        self.write_gold(star, "loan_analytics", partition_cols=["snapshot_date"])
