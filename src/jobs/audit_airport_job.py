from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import sys

catalog_name = "eczachly-academy-warehouse"
spark = (
    SparkSession.builder.config("spark.sql.defaultCatalog", catalog_name)
    .config(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.rest.RESTCatalog",
    )
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", catalog_name)
    .config(
        "spark.sql.catalog.eczachly-academy-warehouse.uri", "https://api.tabular.io/ws/"
    )
    .getOrCreate()
)
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "audit_dim_airport",
        "prod_dim_airport",
    ],
)
audit_dim_airport = args["audit_dim_airport"]
prod_dim_airport = args["prod_dim_airport"]
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql(
    f"""
CREATE OR REPLACE TABLE {audit_dim_airport} (
    airport_id BIGINT,	
    city_market_id BIGINT,
    airport_abbrev STRING,
    airport_location STRING,
    airport_state STRING,
    airport_world_area_code BIGINT
)
USING iceberg
"""
)
dim_airport = spark.sql(f"select * from {prod_dim_airport}")
dim_airport.writeTo(audit_dim_airport).createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
