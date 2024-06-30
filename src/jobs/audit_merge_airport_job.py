from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import sys


spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "staging_dim_airport", "audit_dim_airport"]
)
staging_dim_airport = args["staging_dim_airport"]
audit_dim_airport = args["audit_dim_airport"]
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql(
    f"""
    MERGE INTO {audit_dim_airport} AS target
    USING {staging_dim_airport} AS source
        ON source.airport_id = target.airport_id
    WHEN MATCHED THEN UPDATE SET * 
    WHEN NOT MATCHED THEN INSERT *
    """
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
