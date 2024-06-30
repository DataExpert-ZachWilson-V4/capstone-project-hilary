from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator


from jobs.glue_job_submission import create_glue_job
from query_trino import (
    run_expect_no_results_data_check,
    run_lte_threshold_data_check,
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
polygon_credentials = Variable.get("POLYGON_CREDENTIALS")


@dag(
    "backfill_flight_data_dag",
    description="DAG to backfill flight data and perform data quality checks",
    default_args={
        "owner": "Hilary",
        "start_date": datetime(2024, 6, 28),
        "retries": 1,
    },
    max_active_runs=1,
    schedule_interval="@once",
    tags=["pyspark", "glue", "bts_flights", "hsh", "backfill"],
    catchup=False,
)
def backfill_flight_data_dag():
    fact_bts_flights = "sarneski44638.fact_bts_flights"
    dim_bts_airport = "sarneski44638.dim_bts_airport"
    dim_airplane = "sarneski44638.dim_airplane"
    airport_airline_summary = "sarneski44638.airport_airline_daily"

    years = [2022, 2021, 2020]
    months = list(range(1, 13))
    fpaths = [
        f"s3://zachwilsonsorganization-522/sarneski44638/raw_bts_flights/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.csv"
        for year in years
        for month in months
    ]
    fpaths = ",".join(fpaths)
    backfill_bts_flights_data = PythonOperator(
        task_id="backfill_bts_flights_data",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_bts_flights_data",
            "script_path": "jobs/bts_flights_backfill_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "description": "Backfill bts flights data",
            "arguments": {
                "--fact_output_table": fact_bts_flights,
                "--dim_output_table": dim_bts_airport,
                "--polygon_credentials": polygon_credentials,
                "--fpaths": fpaths,
            },
        },
    )

    run_null_data_quality_checks_fact_bts_flights = PythonOperator(
        task_id="run_null_data_quality_checks_fact_bts_flights",
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
                from {fact_bts_flights}
            """,
        },
    )

    run_duplicate_data_quality_checks_fact_bts_flights = PythonOperator(
        task_id="run_duplicate_data_quality_checks_fact_bts_flights",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
            select flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            from {fact_bts_flights}
            group by flightdate, sch_dep_time, reporting_airline, flight_number_reporting_airline, origin_airport_id, dest_airport_id
            having count(*) > 1
            """,
        },
    )

    load_faa_airplane_data = PythonOperator(
        task_id="load_faa_airplane_data",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "load_faa_airplane_data",
            "script_path": "jobs/dim_airplane_job.py",
            "aws_region": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "description": "Load FAA data",
            "arguments": {
                "--output_table": dim_airplane,
                "--polygon_credentials": polygon_credentials,
            },
        },
    )

    run_null_data_quality_checks_dim_airplane = PythonOperator(
        task_id="run_null_data_quality_checks_dim_airplane",
        python_callable=run_null_data_checks,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select
                    sum(case when faa_tail_number is null then 1 else 0 end) = 0 as faa_tail_number_null,
                    sum(case when n_seats is null then 1 else 0 end) = 0 as n_seats_null,
                    sum(case when manufacturer is null then 1 else 0 end) = 0 as manufacturer_null,
                    sum(case when model is null then 1 else 0 end) = 0 as model_null
                from {dim_airplane}
            """,
        },
    )

    run_duplicate_data_quality_checks_dim_airplane = PythonOperator(
        task_id="run_duplicate_data_quality_checks_dim_airplane",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                select faa_tail_number
                from {dim_airplane}
                group by faa_tail_number
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
                from {dim_bts_airport}
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
                from {dim_bts_airport}
                group by airport_id
                having count(*) > 1
            """,
        },
    )

    run_most_tail_numbers_matched_data_quality_check = PythonOperator(
        task_id="run_most_tail_numbers_matched_data_quality_check",
        python_callable=run_lte_threshold_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                with bts_tails as (
                    select tail_number, faa_tail_number
                    from {fact_bts_flights}
                    group by tail_number, faa_tail_number
                )
                select avg(case when faa.faa_tail_number is null then 1 else 0 end) * 100 as pct_no_tail_num_match
                from bts_tails bts
                left join {dim_airplane} faa
                on bts.faa_tail_number = faa.faa_tail_number
            """,
            "thresholds": [10],
        },
    )

    run_airport_id_reference_check = PythonOperator(
        task_id="run_airport_id_reference_check",
        python_callable=run_expect_no_results_data_check,
        op_kwargs={
            "trino_creds": trino_creds,
            "query": f"""
                with airport_ids as (
                    select airport_id
                    from {dim_bts_airport}
                    group by airport_id
                )
                select origin_airport_id as airportid
                from {fact_bts_flights}
                where origin_airport_id not in (
                    select *
                    from airport_ids
                )
                union
                select dest_airport_id as airportid
                from {fact_bts_flights}
                where dest_airport_id not in (
                    select *
                    from airport_ids
                )
            """,
        },
    )

    backfill_airport_airline_summary_data = PythonOperator(
        task_id="backfill_airport_airline_summary_data",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_airport_airline_summary_data",
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
    completed_single_source_dq_checks = EmptyOperator(
        task_id="completed_single_source_dq_checks"
    )
    end = EmptyOperator(task_id="end")

    start >> [backfill_bts_flights_data, load_faa_airplane_data]
    (
        backfill_bts_flights_data
        >> [
            run_null_data_quality_checks_fact_bts_flights,
            run_duplicate_data_quality_checks_fact_bts_flights,
            run_null_data_quality_checks_dim_airport,
            run_duplicate_data_quality_checks_dim_airport,
        ]
        >> completed_single_source_dq_checks
    )
    (
        load_faa_airplane_data
        >> [
            run_null_data_quality_checks_dim_airplane,
            run_duplicate_data_quality_checks_dim_airplane,
        ]
        >> completed_single_source_dq_checks
    )
    (
        completed_single_source_dq_checks
        >> [
            run_most_tail_numbers_matched_data_quality_check,
            run_airport_id_reference_check,
        ]
        >> backfill_airport_airline_summary_data
        >> run_null_data_quality_checks_airport_airline_summary
        >> end
    )


backfill_flight_data_dag()
