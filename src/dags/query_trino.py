import ast
import os
from trino.auth import BasicAuthentication
from trino.dbapi import connect


def execute_query(query, trino_creds):
    if isinstance(trino_creds, str):
        trino_creds = ast.literal_eval(trino_creds)
    conn = connect(
        host=trino_creds["TRINO_HOST"],
        port=int(trino_creds["TRINO_PORT"]),
        user=trino_creds["TRINO_USERNAME"],
        catalog=trino_creds["TRINO_CATALOG"],
        http_scheme="https",
        auth=BasicAuthentication(
            trino_creds["TRINO_USERNAME"], trino_creds["TRINO_PASSWORD"]
        ),
    )
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def run_null_data_checks(query, trino_creds):
    """
    expects query for data quality checks where failed checks will return False
    """
    results = execute_query(query, trino_creds)
    if len(results) == 0:
        raise ValueError("The query returned no results!")
    for result in results:
        for col_result in result:
            if type(col_result) is bool:
                if not col_result:
                    raise Exception(
                        f"Data quality check(s) failed for {query=}; {results=}"
                    )
    print(f"Passed data quality checks! {query=}; {results=}")


def run_expect_no_results_data_check(query, trino_creds):
    """
    expects query for data quality checks to return no qualifying rows
    """
    results = execute_query(query, trino_creds)
    if len(results) > 0:
        raise Exception(f"Data quality check(s) failed for {query=}; {results=}")
    print(f"Passed data quality checks! {query=}; {results=}")


def run_lte_threshold_data_check(query, trino_creds, thresholds):
    """
    expects single result row from query
    compares threshold values to query results
    if any query result is greater than the threshold, the check fails
    """
    results = execute_query(query, trino_creds)
    if len(results) == 0:
        raise ValueError("The query returned no results")
    if len(results) > 1:
        raise ValueError("The query returned more than one result")
    if len(results[0]) != len(thresholds):
        raise ValueError(
            f"Expected {len(thresholds)} thresholds to match {len(results[0])} column values"
        )
    for col_result, threshold in zip(results[0], thresholds):
        if col_result > threshold:
            raise Exception(
                f"Data quality check(s) failed for {query=}; {results=}; {threshold}"
            )
    print(f"Passed data quality checks! {query=}; {results=}")


def compare_data_counts(audit_query, prod_query, trino_creds):
    """
    expects two queries to return a single result row with a single value
    compares the two values and raises an exception if the updated table (audit_query)
    has fewer rows than prod_query results (table before the merge into the audit table)
    """
    audit_result = execute_query(audit_query, trino_creds)
    prod_result = execute_query(prod_query, trino_creds)
    if len(audit_result) == 0 or len(prod_result) == 0:
        raise ValueError("One or both queries returned no results")
    if len(audit_result) > 1 or len(prod_result) > 1:
        raise ValueError("One or both queries returned more than one result")
    if len(audit_result[0]) != 1 or len(prod_result[0]) != 1:
        raise ValueError("One or both queries returned more than one column")
    if audit_result[0][0] < prod_result[0][0]:
        raise Exception(
            f"Data quality check(s) failed for {audit_query=}; {prod_query=}; {audit_result=}; {prod_result=}"
        )
    else:
        print(
            f"Passed comparing row counts {audit_result[0][0] - prod_result[0][0]} rows added to audit table"
        )


if __name__ == "__main__":
    trino_creds = os.environ["TRINO_CREDENTIALS"]
    audit_table = "sarneski44638.audit_dim_airport"
    prod_table = "sarneski44638.dim_bts_airport"
    audit_query = f"select count(*) from {audit_table}"
    prod_query = f"select count(*) from {prod_table}"
    compare_data_counts(audit_query, prod_query, trino_creds)
