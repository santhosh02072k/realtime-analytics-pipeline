"""
AWS Glue ETL Job for E-Commerce Sales Pipeline
Reads raw sales data from S3, transforms, and loads into Redshift.
"""

import sys
import logging
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
S3_INPUT_PATH = "s3://ecommerce-raw-data/sales/"
REDSHIFT_CONNECTION = "redshift-ecommerce-dw"
REDSHIFT_DATABASE = "ecommerce_analytics"


def create_glue_context():
    """Initialize Glue context."""
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    return glue_context, spark, job


def extract_raw_data(spark):
    """Read raw sales data from S3."""
    logger.info(f"Reading raw data from {S3_INPUT_PATH}")

    raw_sales = spark.read.csv(
        S3_INPUT_PATH,
        header=True,
        inferSchema=True
    )

    logger.info(f"Loaded {raw_sales.count()} raw records")
    return raw_sales


def transform_data(raw_sales):
    """Apply transformations and create star schema tables."""
    logger.info("Applying transformations...")

    # Cleanse: remove nulls and duplicates
    clean_sales = (
        raw_sales
        .dropna(subset=['order_id', 'customer_id', 'product_id'])
        .dropDuplicates(['order_id'])
        .filter(F.col('total_amount') > 0)
    )

    # Create fact table
    fact_sales = (
        clean_sales
        .withColumn('date_id', F.date_format('order_date', 'yyyyMMdd').cast(IntegerType()))
        .withColumn('order_year', F.year('order_date'))
        .withColumn('order_month', F.month('order_date'))
        .select(
            'order_id', 'customer_id', 'product_id', 'date_id',
            'quantity', 'unit_price', 'total_amount', 'discount',
            'status', 'order_year', 'order_month'
        )
    )

    logger.info(f"Fact table: {fact_sales.count()} rows after transformation")
    return fact_sales


def load_to_redshift(glue_context, df, table_name):
    """Write DataFrame to Redshift table."""
    logger.info(f"Loading {table_name} to Redshift...")

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=df,
        catalog_connection=REDSHIFT_CONNECTION,
        connection_options={
            "dbtable": f"{REDSHIFT_DATABASE}.{table_name}",
            "database": REDSHIFT_DATABASE
        },
        redshift_tmp_dir="s3://ecommerce-temp/glue-tmp/"
    )

    logger.info(f"Loaded {table_name} to Redshift successfully")


def main():
    """Main ETL pipeline."""
    logger.info("Starting E-Commerce Sales ETL Pipeline")

    glue_context, spark, job = create_glue_context()

    # Extract
    raw_sales = extract_raw_data(spark)

    # Transform
    fact_sales = transform_data(raw_sales)

    # Load
    load_to_redshift(glue_context, fact_sales, "fact_sales")

    logger.info("ETL Pipeline completed successfully!")
    job.commit()


if __name__ == "__main__":
    main()
