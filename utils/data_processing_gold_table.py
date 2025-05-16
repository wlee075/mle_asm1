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
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

SILVER_PATHS = {
    "fact_loan": "datamart/silver/fact_loan",
    "fact_financials":  "datamart/silver/fact_financials",
    "fact_clickstream": "datamart/silver/fact_clickstream",
    "dim_customer": "datamart/silver/dim_customer",
    "dim_credit_mix": "datamart/silver/dim_credit_mix",
    "dim_payment_behaviour": "datamart/silver/dim_payment_behaviour",
    "dim_min_payment": "datamart/silver/dim_min_payment",
    "dim_loan_type": "datamart/silver/dim_loan_type",
    "dim_feature": "datamart/silver/dim_feature",
    "dim_snapshot": "datamart/silver/dim_snapshot",
    "fact_customer_loan_type": "datamart/silver/fact_customer_loan_type"
}

GOLD_PATHS = {
    "loan_analytics":   "datamart/gold/loan_analytics_star",
    "customer_summary": "datamart/gold/customer_summary"
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
        (w.write.mode(mode)
           .partitionBy(*(partition_cols or []))
           .parquet(path))

    def build_loan_analytics_star(self):
        fact_loan        = self.read_silver("fact_loan")
        dim_customer     = self.read_silver("dim_customer")
        dim_credit_mix   = self.read_silver("dim_credit_mix")
        dim_pay_behav    = self.read_silver("dim_payment_behaviour")
        dim_min_pay      = self.read_silver("dim_min_payment")
        dim_loan_type    = self.read_silver("dim_loan_type")
        dim_snapshot     = self.read_silver("dim_snapshot")
        fact_financials  = self.read_silver("fact_financials")
        fact_cust_loan   = self.read_silver("fact_customer_loan_type")
        star = (fact_loan
        .join(dim_customer, "customer_id")
        .join(fact_financials, ["customer_id", "snapshot_id"]) 
        .join(fact_cust_loan, ["customer_id", "snapshot_id"])
        .join(dim_credit_mix, fact_financials["credit_mix_id"] == dim_credit_mix["credit_mix_id"])
        .join(dim_pay_behav, fact_financials["payment_behaviour_id"] == dim_pay_behav["payment_behaviour_id"])
        .join(dim_min_pay, fact_financials["payment_code"] == dim_min_pay["payment_code"])
        .join(dim_loan_type, "loan_type_id")
        .join(dim_snapshot, "snapshot_id")
        .select(
            "loan_id",
            "customer_id", "SSN", "Name", "Age", "Occupation",
            fact_financials["credit_mix_id"],
            fact_financials["payment_behaviour_id"],
            fact_financials["payment_code"],  # Qualified with fact_financials
            dim_min_pay["payment_code"].alias("min_pay_code"),
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
            "balance"
        )
        )
        self.write_gold(star, "loan_analytics", partition_cols=["snapshot_date"])

    def build_customer_summary(self):
        dim_customer = self.read_silver("dim_customer")
        fact_fin = self.read_silver("fact_financials")
        fact_click = self.read_silver("fact_clickstream")
        dim_snapshot = self.read_silver("dim_snapshot")
        fact_fin_with_date = fact_fin.join(
            dim_snapshot,
            "snapshot_id"
        )
        window = Window.partitionBy("customer_id").orderBy(F.desc("snapshot_date"))
        latest_fin = (fact_fin_with_date.withColumn("rn", row_number().over(window)).filter("rn = 1")
                      .drop("rn", "snapshot_date", "snapshot_id"))
        click_agg = (fact_click.groupBy("customer_id").agg(sum("value").alias("total_events")))
        summary = (dim_customer.join(latest_fin, "customer_id").join(click_agg, "customer_id", how="left")
                   .select(
                        "customer_id",
                        "name",
                        "age",
                        "occupation",
                        "Annual_Income",
                        "Monthly_Inhand_Salary",
                        "Outstanding_Debt",
                        "total_events"
                    )
        )
        self.write_gold(summary, "customer_summary")

    def run_all(self):
        self.build_loan_analytics_star()
        self.build_customer_summary()
