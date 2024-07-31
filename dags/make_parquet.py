from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

#Pandas
import pandas as pd

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
        'MakeParquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
        },
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple','bash', 'etl','shop'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{var.value.IMPORT_CHECK_SH}} $DONE_FILE
        """,
        trigger_rule="all_success"
    )

    task_to_parquet = BashOperator(
        task_id='to.parquet',
        bash_command= """
            echo "to.parquet"

            READ_PATH="~/data/csv/{{ ds_nodash}}/csv.csv"
            SAVE_PATH="~/data"
            
            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
            
        """,
        trigger_rule="all_success"
    )

    task_done = BashOperator(
        task_id='make.done',
        bash_command="""
            DONE_PATH=~/data/parquet/done/{{ds_nodash}}
            mkdir -p ${DONE_PATH}
            touch ${DONE_PATH}/_DONE
        """,
        trigger_rule="all_success"
    )
    task_end = gen_emp('end','all_done')
    task_start = gen_emp('start')

    task_start >> task_check >> task_to_parquet >> task_done >> task_end


    

    

