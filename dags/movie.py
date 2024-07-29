from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint

from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator
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
   
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print('*'* 20)
        from mov.api.call import list2df, get_key, save2df
        #from home.kim1.code.new.mov.src.mov.api import gen_url, req, get_key, req2list, list2df, savedf
        key = get_key()
        print(key)
        YYYYMMDD = kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(5))

    run_this = PythonOperator(
        task_id="print_the_context",
        python_callable=print_context,
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_get = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/lsiwh37249/mov.git@0.2/api"],
        system_site_packages=False,
    )

    task_save = BashOperator(
        task_id='save.data',
        bash_command="""
            echo "save_date"
        """ ,
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

    task_start >> task_get >> task_save >> task_end 
    task_start >> run_this >> task_end

