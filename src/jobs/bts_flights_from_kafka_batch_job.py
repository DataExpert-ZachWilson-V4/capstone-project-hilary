import ast
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, upper
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
import sys


spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "fact_output_table", "dim_output_table", "kafka_credentials"]
)
fact_output_table = args["fact_output_table"]
dim_output_table = args["dim_output_table"]
kafka_credentials = ast.literal_eval(args["kafka_credentials"])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials from environment variables
if isinstance(kafka_credentials, str):
    kafka_credentials = ast.literal_eval(kafka_credentials)
kafka_key = kafka_credentials["KAFKA_FLIGHTS_KEY"]
kafka_secret = kafka_credentials["KAFKA_FLIGHTS_SECRET"]
kafka_bootstrap_servers = kafka_credentials["KAFKA_BOOTSTRAP_SERVERS"]
kafka_topic = "bts_kaftka_flights_topic"
if kafka_key is None or kafka_secret is None:
    raise ValueError(
        "KAFKA_FLIGHTS_KEY and KAFKA_FLIGHTS_SECRET must be set as environment variables."
    )


# Define the schema of the Kafka message value
kafka_schema = StructType(
    [
        StructField("FlightDate", StringType(), False),
        StructField("Reporting_Airline", StringType(), False),
        StructField("Tail_Number", StringType(), True),
        StructField("Flight_Number_Reporting_Airline", LongType(), False),
        StructField("OriginAirportID", LongType(), False),
        StructField("OriginCityMarketID", LongType(), False),
        StructField("Origin", StringType(), False),
        StructField("OriginCityName", StringType(), False),
        StructField("OriginState", StringType(), False),
        StructField("OriginWac", LongType(), False),
        StructField("DestAirportID", LongType(), False),
        StructField("DestCityMarketID", LongType(), False),
        StructField("Dest", StringType(), False),
        StructField("DestCityName", StringType(), False),
        StructField("DestState", StringType(), False),
        StructField("DestWac", LongType(), False),
        StructField("CRSDepTime", LongType(), False),
        StructField("DepTime", LongType(), True),
        StructField("DepDelay", LongType(), True),
        StructField("TaxiOut", LongType(), True),
        StructField("WheelsOff", LongType(), True),
        StructField("WheelsOn", LongType(), True),
        StructField("TaxiIn", LongType(), True),
        StructField("CRSArrTime", LongType(), False),
        StructField("ArrTime", LongType(), True),
        StructField("ArrDelay", LongType(), True),
        StructField("Cancelled", IntegerType(), False),
        StructField("CancellationCode", StringType(), True),
        StructField("Diverted", IntegerType(), False),
        StructField("CRSElapsedTime", LongType(), True),
        StructField("ActualElapsedTime", LongType(), True),
        StructField("AirTime", LongType(), nullable=True),
        StructField("Flights", IntegerType(), False),
        StructField("Distance", LongType(), False),
        StructField("CarrierDelay", LongType(), True),
        StructField("WeatherDelay", LongType(), True),
        StructField("NASDelay", LongType(), True),
        StructField("SecurityDelay", LongType(), True),
        StructField("LateAircraftDelay", LongType(), True),
        StructField("TotalAddGTime", LongType(), True),
        StructField("LongestAddGTime", LongType(), True),
        StructField("DivAirportLandings", IntegerType(), True),
        StructField("DivReachedDest", IntegerType(), True),
        StructField("DivActualElapsedTime", LongType(), True),
        StructField("DivArrDelay", LongType(), True),
        StructField("DivDistance", LongType(), nullable=True),
    ]
)

# Read from Kafka in batch mode
kafka_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
    )
    .load()
)


def decode_col(column):
    return column.decode("utf-8")


decode_udf = udf(decode_col, StringType())

# Extract the value from Kafka messages and cast to String
kafka_df = (
    kafka_df.select("key", "value", "topic")
    .withColumn("decoded_value", decode_udf(col("value")))
    .withColumn("value", from_json(col("decoded_value"), kafka_schema))
    .select("value.*")
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
    kafka_df.withColumn("flightdate", col("flightdate").cast("date"))
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
    .dropDuplicates()
    .writeTo(fact_output_table)
    .tableProperty("write.spark.fanout.enabled", "true")
    .createOrReplace()
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
origin_airport_df = kafka_df.select(
    col("OriginAirportID").alias("airport_id"),
    col("OriginCityMarketID").alias("city_market_id"),
    col("Origin").alias("airport_abbrev"),
    col("OriginCityName").alias("airport_location"),
    col("OriginState").alias("airport_state"),
    col("OriginWac").alias("airport_world_area_code"),
)
dest_airport_df = kafka_df.select(
    col("DestAirportID").alias("airport_id"),
    col("DestCityMarketID").alias("city_market_id"),
    col("Dest").alias("airport_abbrev"),
    col("DestCityName").alias("airport_location"),
    col("DestState").alias("airport_state"),
    col("DestWac").alias("airport_world_area_code"),
)
origin_airport_df.union(dest_airport_df).withColumn(
    "airport_location", upper(col("airport_location"))
).dropDuplicates().writeTo(dim_output_table).createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
