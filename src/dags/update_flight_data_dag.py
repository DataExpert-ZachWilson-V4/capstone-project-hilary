from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator


from jobs.glue_job_submission import create_glue_job
from query_trino import (
    compare_data_counts,
    execute_query,
    run_expect_no_results_data_check,
    run_null_data_checks,
)


s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = Variable.get("KAFKA_CREDENTIALS_HILARY")
trino_creds = Variable.get("TRINO_CREDENTIALS")


@dag(
    "update_flight_data_dag",
    description="DAG to load previous day's flights to staging, perform quality checks, and publish",
    default_args={
        "owner": "Hilary",
        "start_date": datetime(2024, 6, 28),
        "retries": 0,
    },
    max_active_runs=1,
    schedule_interval="0 3 * * *",
    tags=["pyspark", "glue", "flights", "hsh"],
    catchup=False,
)
def update_flight_data_dag():
    staging_fact_flights = "sarneski44638.staging_bts_flights"
    staging_dim_airport = "sarneski44638.staging_dim_airport"
    audit_fact_flights = "sarneski44638.audit_bts_flights"
    audit_dim_airport = "sarneski44638.audit_dim_airport"
    prod_fact_flights = "sarneski44638.fact_bts_flights"
    prod_dim_airport = "sarneski44638.dim_bts_airport"
    airport_airline_summary = "sarneski44638.airport_airline_daily"

    clear_staging_bts_flights = PythonOperator(
        task_id="clear_staging_bts_flights",
        python_callable=execute_query,
        op_kwargs={
            "query": f"delete from {staging_fact_flights}",
            "trino_creds": trino_creds,
        },
    )

    clear_staging_dim_airport = PythonOperator(
        task_id="clear_staging_dim_airport",
        python_callable=execute_query,
        op_kwargs={
            "query": f"delete from {staging_dim_airport}",
            "trino_creds": trino_creds,
        },
    )

    load_staging_data = PythonOperator(
        task_id="load_staging_data",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "load_staging_data",
            "script_path": "jobs/bts_flights_from_kafka_batch_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "kafka_credentials": kafka_credentials,
            "description": "Load flight data from Kafka to staging table",
            "arguments": {
                "--fact_output_table": staging_fact_flights,
                "--dim_output_table": staging_dim_airport,
            },
        },
    )

    run_null_data_quality_checks_staging_bts_flights = PythonOperator(
        task_id="run_null_data_quality_checks_staging_bts_flights",
        python_callable=run_null_data_checks,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select
                    sum(case when flightdate is null then 1 else 0 end) = 0 as flightdate_null,
                    sum(case when sch_dep_time is null then 1 else 0 end) = 0 as sch_dep_time_null,
                    sum(case when reporting_airline is null then 1 else 0 end) = 0 as reporting_airline_null,
                    sum(case when flight_number_reporting_airline is null then 1 else 0 end) = 0 as flight_number_null,
                    sum(case when origin_airport_id is null then 1 else 0 end) = 0 as origin_airport_id_null,
                    sum(case when dest_airport_id is null then 1 else 0 end) = 0 as dest_airport_id_null
                from {staging_fact_flights}
            """,
        },
    )

    run_duplicate_data_quality_checks_staging_bts_flights = PythonOperator(
        task_id="run_duplicate_data_quality_checks_staging_bts_flights",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
            select flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            from {staging_fact_flights}
            group by flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            having count(*) > 1
            """,
        },
    )

    run_null_data_quality_checks_dim_airport = PythonOperator(
        task_id="run_null_data_quality_checks_dim_airport",
        python_callable=run_null_data_checks,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select
                    sum(case when airport_id is null then 1 else 0 end) = 0 as airport_id_null,
                    sum(case when city_market_id is null then 1 else 0 end) = 0 as city_market_id_null,
                    sum(case when airport_abbrev is null then 1 else 0 end) = 0 as airport_abbrev_null,
                    sum(case when airport_location is null then 1 else 0 end) = 0 as airport_location_null,
                    sum(case when airport_state is null then 1 else 0 end) = 0 as airport_state_null,
                    sum(case when airport_world_area_code is null then 1 else 0 end) = 0 as airport_wac_null
                from {staging_dim_airport}
            """,
        },
    )

    run_duplicate_data_quality_checks_dim_airport = PythonOperator(
        task_id="run_duplicate_data_quality_checks_dim_airport",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select airport_id
                from {staging_dim_airport}
                group by airport_id
                having count(*) > 1
            """,
        },
    )

    clear_audit_fact_flights = PythonOperator(
        task_id="clear_audit_fact_flights",
        python_callable=execute_query,
        op_kwargs={
            "query": f"delete from {audit_fact_flights}",
            "trino_creds": trino_creds,
        },
    )

    clear_audit_dim_airport = PythonOperator(
        task_id="clear_audit_dim_airport",
        python_callable=execute_query,
        op_kwargs={
            "query": f"delete from {audit_dim_airport}",
            "trino_creds": trino_creds,
        },
    )

    create_audit_fact_flight_table = PythonOperator(
        task_id="create_audit_fact_flight_table",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "create_audit_fact_flight_table",
            "script_path": "jobs/audit_flights_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--audit_fact_flights": audit_fact_flights,
                "--prod_fact_flights": prod_fact_flights,
            },
        },
    )

    merge_into_audit_flight_job = PythonOperator(
        task_id="merge_into_audit_flight_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "merge_into_audit_flight_job",
            "script_path": "jobs/audit_merge_flight_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--staging_fact_flights": staging_fact_flights,
                "--audit_fact_flights": audit_fact_flights,
            },
        },
    )

    create_audit_dim_airport_table = PythonOperator(
        task_id="create_audit_dim_airport_table",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "create_audit_dim_airport_table",
            "script_path": "jobs/audit_airport_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--audit_dim_airport": audit_dim_airport,
                "--prod_dim_airport": prod_dim_airport,
            },
        },
    )

    merge_into_audit_airport_job = PythonOperator(
        task_id="merge_into_audit_airport_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "merge_into_audit_airport_job",
            "script_path": "jobs/audit_merge_airport_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--staging_dim_airport": staging_dim_airport,
                "--audit_dim_airport": audit_dim_airport,
            },
        },
    )

    run_row_comparison_audit_fact_flights = PythonOperator(
        task_id="run_row_comparison_audit_fact_flights",
        python_callable=compare_data_counts,
        op_kwargs={
            "trino_creds": trino_creds,
            "audit_query": f"select count(*) from {audit_fact_flights}",
            "prod_query": f"select count(*) from {prod_fact_flights}",
        },
    )

    run_row_comparison_audit_dim_airport = PythonOperator(
        task_id="run_row_comparison_dim_airport",
        python_callable=compare_data_counts,
        op_kwargs={
            "trino_creds": trino_creds,
            "audit_query": f"select count(*) from {audit_dim_airport}",
            "prod_query": f"select count(*) from {prod_dim_airport}",
        },
    )

    run_duplicate_data_quality_checks_audit_fact_flights = PythonOperator(
        task_id="run_duplicate_data_quality_checks_audit_fact_flights",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
            select flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            from {audit_fact_flights}
            group by flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            having count(*) > 1
            """,
        },
    )

    run_duplicate_data_quality_checks_audit_dim_airport = PythonOperator(
        task_id="run_duplicate_data_quality_checks_audit_dim_airport",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select airport_id
                from {audit_dim_airport}
                group by airport_id
                having count(*) > 1
            """,
        },
    )

    fact_query = f"""
    insert overwrite table {prod_fact_flights}
    select * from {audit_fact_flights}
    """
    publish_audited_fact_table = PythonOperator(
        task_id="publish_audited_fact_table",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "publish_audited_fact_table",
            "script_path": "jobs/publish_audit_to_prod.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--query": fact_query,
            },
        },
    )

    dim_query = f"""
    insert overwrite table {prod_dim_airport}
    select * from {audit_dim_airport}
    """
    publish_audited_dim_table = PythonOperator(
        task_id="publish_audited_dim_table",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "publish_audited_dim_table",
            "script_path": "jobs/publish_audit_to_prod.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "arguments": {
                "--query": dim_query,
            },
        },
    )

    create_airport_airline_summary_data = PythonOperator(
        task_id="create_airport_airline_summary_data",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "create_airport_airline_summary_data",
            "script_path": "jobs/airport_airline_summary_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "description": "Backfill airport airline summary data",
            "arguments": {
                "--output_table": airport_airline_summary,
            },
        },
    )

    run_null_data_quality_checks_airport_airline_summary = PythonOperator(
        task_id="run_null_data_quality_checks_airport_airline_summary",
        python_callable=run_null_data_checks,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select
                    sum(case when reporting_airline is null then 1 else 0 end) = 0 as reporting_airline_null,
                    sum(case when flightdate is null then 1 else 0 end) = 0 as flightdate_null,
                    sum(case when airport_id is null then 1 else 0 end) = 0 as airport_id_null,
                    sum(case when n_flights is null then 1 else 0 end) = 0 as n_flights_null,
                    sum(case when n_dep_delay is null then 1 else 0 end) = 0 asn_dep_delay_null,
                    sum(case when n_arr_delay is null then 1 else 0 end) = 0 as n_arr_delay_null,
                    sum(case when n_cancelled is null then 1 else 0 end) = 0 as n_cancelled_null,
                    sum(case when n_diverted is null then 1 else 0 end) = 0 as n_diverted_null,
                    sum(case when n_diverted_reached_destination is null then 1 else 0 end) = 0 as n_diverted_reached_destination_null
                from {airport_airline_summary}
            """,
        },
    )

    start = EmptyOperator(task_id="start")
    start_staging = EmptyOperator(task_id="start_staging")
    start_audit = EmptyOperator(task_id="start_audit")
    audit_done = EmptyOperator(task_id="audit_done")
    publish_done = EmptyOperator(task_id="publish_done")
    end = EmptyOperator(task_id="end")

    (
        start
        >> [clear_staging_bts_flights, clear_staging_dim_airport]
        >> start_staging
        >> load_staging_data
        >> [
            run_null_data_quality_checks_staging_bts_flights,
            run_duplicate_data_quality_checks_staging_bts_flights,
            run_null_data_quality_checks_dim_airport,
            run_duplicate_data_quality_checks_dim_airport,
        ]
        >> start_audit
        >> [clear_audit_fact_flights, clear_audit_dim_airport]
    )
    (
        clear_audit_fact_flights
        >> create_audit_fact_flight_table
        >> merge_into_audit_flight_job
        >> [
            run_row_comparison_audit_fact_flights,
            run_duplicate_data_quality_checks_audit_fact_flights,
        ]
        >> audit_done
    )
    (
        clear_audit_dim_airport
        >> create_audit_dim_airport_table
        >> merge_into_audit_airport_job
        >> [
            run_row_comparison_audit_dim_airport,
            run_duplicate_data_quality_checks_audit_dim_airport,
        ]
        >> audit_done
    )
    (
        audit_done
        >> [publish_audited_fact_table, publish_audited_dim_table]
        >> publish_done
        >> create_airport_airline_summary_data
        >> run_null_data_quality_checks_airport_airline_summary
        >> end
    )


update_flight_data_dag()
