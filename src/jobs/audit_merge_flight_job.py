from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import sys


spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "staging_fact_flights", "audit_fact_flights"]
)
staging_fact_flights = args["staging_fact_flights"]
audit_fact_flights = args["audit_fact_flights"]
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql(
    f"""
    MERGE INTO {audit_fact_flights} AS target
    USING {staging_fact_flights} AS source
        ON source.flightdate = target.flightdate
        AND source.sch_dep_time = target.sch_dep_time
        AND source.reporting_airline = target.reporting_airline
        AND source.flight_number_reporting_airline = target.flight_number_reporting_airline
        AND source.origin_airport_id = target.origin_airport_id
        AND source.dest_airport_id = target.dest_airport_id
    WHEN MATCHED THEN UPDATE SET * 
    WHEN NOT MATCHED THEN INSERT *
    """
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
