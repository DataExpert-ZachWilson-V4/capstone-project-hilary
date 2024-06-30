import ast
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
import sys

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "output_table", "polygon_credentials"],
)
output_table = args["output_table"]

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

master_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("s3://zachwilsonsorganization-522/sarneski44638/raw_faa/MASTER.txt")
    .select(
        trim(upper(col("N-NUMBER"))).alias("faa_tail_number"),
        col("MFR MDL CODE").alias("mfr_mdl_code"),
        col("YEAR MFR").alias("mfr_year"),
    )
)
aircraft_ref_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("s3://zachwilsonsorganization-522/sarneski44638/raw_faa/ACFTREF.txt")
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE {output_table} (
    faa_tail_number STRING,	
    mfr_year INT,
    manufacturer STRING,
    model STRING,
    n_seats INT
)
USING iceberg
"""
)

(
    master_df.join(
        aircraft_ref_df, master_df["mfr_mdl_code"] == aircraft_ref_df["code"], "inner"
    )
    .withColumn("mfr_year", col("mfr_year").cast("int"))
    .select(
        col("faa_tail_number"),
        col("mfr_year"),
        col("mfr").alias("manufacturer"),
        col("model"),
        col("no-seats").alias("n_seats"),
    )
    .writeTo(output_table)
    .tableProperty("write.spark.fanout.enabled", "true")
    .overwritePartitions()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
