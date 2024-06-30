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
        "audit_fact_flights",
        "prod_fact_flights",
    ],
)
audit_fact_flights = args["audit_fact_flights"]
prod_fact_flights = args["prod_fact_flights"]
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql(
    f"""
CREATE OR REPLACE TABLE {audit_fact_flights} (
    flightdate STRING,	
    reporting_airline STRING,
    tail_number STRING,
    faa_tail_number STRING,
    flight_number_reporting_airline BIGINT,
    origin_airport_id BIGINT,
    dest_airport_id BIGINT,
    sch_dep_time BIGINT,
    dep_time BIGINT,
    dep_delay BIGINT,
    taxi_out BIGINT,
    wheels_off BIGINT,
    wheels_on BIGINT,
    taxi_in BIGINT,
    sch_arr_time BIGINT,
    arr_time BIGINT,
    arr_delay BIGINT,
    cancelled INT,
    cancellation_code STRING,
    diverted INT,
    sch_elapsed_time BIGINT,
    actual_elapsed_time BIGINT,
    air_time BIGINT,
    n_flights INT,
    distance BIGINT,
    carrier_delay BIGINT,
    weather_delay BIGINT,
    nas_delay BIGINT,
    security_delay BIGINT,
    late_aircraft_delay BIGINT,
    total_away_ground_time BIGINT,
    longest_away_ground_time BIGINT,
    div_airport_landings INT,
    div_reached_dest INT,
    div_actual_elapsed_time BIGINT,
    div_arr_delay BIGINT,
    div_distance BIGINT
)
USING iceberg
PARTITIONED BY (flightdate)
"""
)
fact_bts_flights = spark.sql(f"select * from {prod_fact_flights}")
(
    fact_bts_flights.writeTo(audit_fact_flights)
    .tableProperty("write.spark.fanout.enabled", "true")
    .overwritePartitions()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
