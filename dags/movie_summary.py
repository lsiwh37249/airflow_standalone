from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint

from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )

with DAG(
        'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
        },
    max_active_tasks=3,
    max_active_runs=1,
    description='Second Part Of 영화진흥위원회 습격사건',
    #schedule=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 26),
    catchup=True,
    tags=['api', 'movie', 'part2'],
) as dag:

    apply_type = EmptyOperator(
        task_id="apply.type",
        )
    
    merge_df = EmptyOperator(
        task_id="merge.of",
        )

    de_dup = EmptyOperator(
        task_id="de.dup",
        )

    summary_df = EmptyOperator(
        task_id="summary.df",
        )


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> apply_type >> merge_df >> de_dup >> summary_df >> task_end
