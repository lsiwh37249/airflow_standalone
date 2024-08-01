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

    def get_apply_data():
        print("get_apply_data")

    def gen_empty(*ids):
        task =[ ]
        for id in ids:
            PVEO = PythonVirtualenvOperator(
                task_id=id,
                python_callable=get_apply_data,
                #requirements=["git+https://github.com/lsiwh37249/mov.git@0.3.3/api"],
                system_site_packages=False,
                trigger_rule="all_done",
                #venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
                )
        task.append(PVEO)
        return task
    
#    my_tasks = [apply_Atype, apply.Btype, apply.Ctype, apply.Dtype] 
#    for my_task,task in zip(my_tasks,gen_empty("apply.Atype","apply.Btype","apply.Ctype","apply.Dtype")):
#        my_task = task
    apply_Atype = gen_empty('apply.Atype')[0]
    apply_Btype = gen_empty('apply.Btype')[0]
    apply_Ctype = gen_empty('apply.Ctype')[0]
    apply_Dtype = gen_empty('apply.Dtype')[0]


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

    task_start >> [apply_Atype, apply_Btype, apply_Ctype, apply_Dtype] >> merge_df >> de_dup >> summary_df >> task_end
