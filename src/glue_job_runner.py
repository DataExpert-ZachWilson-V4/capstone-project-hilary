from utils.helpers import get_secret
from glue_job_submission import create_glue_job


def create_and_run_glue_job(job_name, script_path, arguments):
    s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
    tabular_credential = get_secret("TABULAR_CREDENTIAL")
    catalog_name = get_secret("CATALOG_NAME")
    aws_region = get_secret("AWS_GLUE_REGION")
    aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
    kafka_credentials = get_secret("KAFKA_CREDENTIALS")
    glue_job = create_glue_job(
        job_name=job_name,
        script_path=script_path,
        arguments=arguments,
        aws_region=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        tabular_credential=tabular_credential,
        s3_bucket=s3_bucket,
        catalog_name=catalog_name,
        kafka_credentials=kafka_credentials,
    )


if __name__ == "__main__":
    years = [2022, 2021, 2020]
    months = list(range(1, 13))
    fpaths = [
        f"s3://zachwilsonsorganization-522/sarneski44638/raw_bts_flights/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.csv"
        for year in years
        for month in months
    ]
    polygon_credentials = get_secret("POLYGON_CREDENTIALS")
    create_and_run_glue_job(
        job_name="bts_flights",
        script_path="bts_flights_job.py",
        arguments={
            "--fact_output_table": "sarneski44638.fact_bts_flights",
            "--dim_output_table": "sarneski44638.dim_bts_airport",
            "--polygon_credentials": polygon_credentials,
            "--fpaths": ",".join(fpaths),
        },
    )
