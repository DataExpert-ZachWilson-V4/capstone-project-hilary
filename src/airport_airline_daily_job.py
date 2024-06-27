from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import sys

catalog_name = 'eczachly-academy-warehouse'
spark = (SparkSession.builder.config(
    'spark.sql.defaultCatalog', catalog_name)
         .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
         .config(
    f'spark.sql.catalog.{catalog_name}.catalog-impl',
    'org.apache.iceberg.rest.RESTCatalog')
         .config(
    f'spark.sql.catalog.{catalog_name}.warehouse',
    catalog_name).config(
    'spark.sql.catalog.eczachly-academy-warehouse.uri',
    'https://api.tabular.io/ws/').getOrCreate())

args = getResolvedOptions(sys.argv, ["JOB_NAME", 'output_table'])
output_table = args["output_table"]

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
spark.sql(f"""
   CREATE TABLE IF NOT EXISTS {output_table} (
    reporting_airline STRING,
    flightdate DATE,
    airport_id STRING,
    n_flights BIGINT,
    n_dep_delay BIGINT,
    n_arr_delay BIGINT,
    n_cancelled BIGINT,
    n_diverted BIGINT,
    n_diverted_reached_destination BIGINT,
    n_carrier_delay BIGINT,
    n_weather_delay BIGINT,
    n_nas_delay BIGINT,
    n_security_delay BIGINT,
    n_late_aircraft_delay BIGINT
)
USING iceberg
PARTITIONED BY (flightdate)
""")

spark.sql(f"""
insert overwrite table {output_table}
select
    reporting_airline,
    cast(flightdate as date) as flightdate,
    origin_airport_id as airport_id,
    count(*) as n_flights,
    sum(case when arr_delay >= 15 then 1 else 0 end) as n_dep_delay,
    sum(case when arr_delay >= 15 then 1 else 0 end) as n_arr_delay,
    sum(cancelled) as n_cancelled,
    sum(diverted) as n_diverted,
    sum(case when diverted = 1 and div_reached_dest = 1 then 1 else 0 end) as n_diverted_reached_destination,
    sum(case when carrier_delay >= 15 then 1 else 0 end) as n_carrier_delay,
    sum(case when weather_delay >= 15 then 1 else 0 end) as n_weather_delay,
    sum(case when nas_delay >= 15 then 1 else 0 end) as n_nas_delay,
    sum(case when security_delay >= 15 then 1 else 0 end) as n_security_delay,
    sum(case when late_aircraft_delay >= 15 then 1 else 0 end) as n_late_aircraft_delay
from
    sarneski44638.fact_bts_flights
where flightdate is not null
group by
    reporting_airline,
    flightdate,
    origin_airport_id
"""
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
