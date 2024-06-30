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

args = getResolvedOptions(sys.argv, ["JOB_NAME", "query"])
query = args["query"]
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
spark.sql(query)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
