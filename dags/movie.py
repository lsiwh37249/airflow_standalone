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

def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print("::group::All kwargs")
    pprint(kwargs)
    print("::endgroup::")
    print("::group::Context variable ds")
    print(ds)
    print("::endgroup::")
    return "Whatever you return gets printed in the logs"

with DAG(
        'movie_datapipline',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
        },
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 26),
    catchup=True,
    tags=['api', 'movie'],
) as dag:


    def save_data(ds_nodash):
        from mov.api.call import get_key,echo
        key = get_key()
        print("33")
        print(key)
        msg = echo("hello")
        print(msg)
        print(" 33")
   
    def get_data(ds_nodash):
        from mov.api.call import list2df, get_key, save2df
        df = save2df(ds_nodash)
        print(df.head(5))

    def get_data(ds_nodash):
        from mov.api.call import get_key
        key = get_key()
        print("*" * 33)
        print(key)
        
        print("*" * 33)
    
    def branch_fun(**kwargs):
        ld = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/tmp/test_parquet/load_dt={ld}'
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data","echo.task"

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
        )

    run_this = PythonOperator(
        task_id="print_the_context",
        python_callable=print_context,
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_get = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/lsiwh37249/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule="all_done",
        venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/lsiwh37249/mov.git@0.3/api"],
        venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command="""
            rm -rf ~/tmp/test_parquet/load_dt={{ds_nodash}}
        """,
        trigger_rule="all_done"
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'",
        trigger_rule="all_success"
    )


#    task_err = BashOperator(
#        bash_command="""
#            DONE_PATH=~/data/done/{{ds_nodash}}
#            mkdir -p ${DONE_PATH}
#            touch ${DONE_PATH}/_DONE
#        """,
#    )

    
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
            )


    task_start >> branch_op
    task_start >> join_task >> save_data >> task_end
    branch_op >> rm_dir >> task_get
    branch_op >> echo_task
    task_get >> save_data >> task_end
    
