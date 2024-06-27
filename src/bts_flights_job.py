import ast
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf, upper
import sys


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "fact_output_table",
        "dim_output_table",
        "polygon_credentials",
        "fpaths",
    ],
)
fpaths = args["fpaths"].split(",")
fact_output_table = args["fact_output_table"]
dim_output_table = args["dim_output_table"]

spark = SparkSession.builder.appName("ReadFromS3").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
polygon_credentials = ast.literal_eval(args["polygon_credentials"])
aws_access_key_id = polygon_credentials["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = polygon_credentials["AWS_SECRET_ACCESS_KEY"]

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.polygon.io/")
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Read the CSV files into a DataFrame
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(fpaths)
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE {fact_output_table} (
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


def create_faa_tail_number(tail_number):
    if isinstance(tail_number, str):
        if tail_number[0] == "N":
            return tail_number[1:]
    return tail_number


tail_num_udf = udf(create_faa_tail_number, StringType())

(
    df.withColumn("flightdate", col("flightdate").cast("date"))
    .withColumn("faa_tail_number", tail_num_udf(upper(col("tail_number"))))
    .select(
        col("flightdate"),
        col("reporting_airline"),
        col("tail_number"),
        col("faa_tail_number"),
        col("flight_number_reporting_airline"),
        col("OriginAirportID").alias("origin_airport_id"),
        col("DestAirportID").alias("dest_airport_id"),
        col("CRSDepTime").alias("sch_dep_time"),
        col("DepTime").alias("dep_time"),
        col("DepDelay").alias("dep_delay"),
        col("TaxiOut").alias("taxi_out"),
        col("WheelsOff").alias("wheels_off"),
        col("WheelsOn").alias("wheels_on"),
        col("TaxiIn").alias("taxi_in"),
        col("CRSArrTime").alias("sch_arr_time"),
        col("ArrTime").alias("arr_time"),
        col("ArrDelay").alias("arr_delay"),
        col("Cancelled"),
        col("CancellationCode").alias("cancellation_code"),
        col("Diverted"),
        col("CRSElapsedTime").alias("sch_elapsed_time"),
        col("ActualElapsedTime").alias("actual_elapsed_time"),
        col("AirTime").alias("air_time"),
        col("Flights").alias("n_flights"),
        col("Distance"),
        col("CarrierDelay").alias("carrier_delay"),
        col("WeatherDelay").alias("weather_delay"),
        col("NASDelay").alias("nas_delay"),
        col("SecurityDelay").alias("security_delay"),
        col("LateAircraftDelay").alias("late_aircraft_delay"),
        col("TotalAddGTime").alias("total_away_ground_time"),
        col("LongestAddGTime").alias("longest_away_ground_time"),
        col("DivAirportLandings").alias("div_airport_landings"),
        col("DivReachedDest").alias("div_reached_dest"),
        col("DivActualElapsedTime").alias("div_actual_elapsed_time"),
        col("DivArrDelay").alias("div_arr_delay"),
        col("DivDistance").alias("div_distance"),
    )
    .writeTo(fact_output_table)
    .tableProperty("write.spark.fanout.enabled", "true")
    .overwritePartitions()
)

# Airport dimension table
spark.sql(
    f"""
CREATE OR REPLACE TABLE {dim_output_table} (
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
origin_airport_df = df.select(
    col("OriginAirportID").alias("airport_id"),
    col("OriginCityMarketID").alias("city_market_id"),
    col("Origin").alias("airport_abbrev"),
    col("OriginCityName").alias("airport_location"),
    col("OriginState").alias("airport_state"),
    col("OriginWac").alias("airport_world_area_code"),
)
dest_airport_df = df.select(
    col("DestAirportID").alias("airport_id"),
    col("DestCityMarketID").alias("city_market_id"),
    col("Dest").alias("airport_abbrev"),
    col("DestCityName").alias("airport_location"),
    col("DestState").alias("airport_state"),
    col("DestWac").alias("airport_world_area_code"),
)
origin_airport_df.union(dest_airport_df).dropDuplicates().writeTo(
    dim_output_table
).createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
