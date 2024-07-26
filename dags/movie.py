from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

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

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_get = BashOperator(
        task_id='get.data',
        bash_command="""
            echo "get_date"
        """ ,
        trigger_rule="all_success"
        )

    task_save = BashOperator(
        task_id='save.data',
        bash_command="""
            echo "save_date"
        """ ,
        trigger_rule="all_success"
        )


    task_err = BashOperator(
        task_id='err.report',
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

#    task_done = BashOperator(
#        task_id='make.done',
#        bash_command="""
#            DONE_PATH=~/data/done/{{ds_nodash}}
#            mkdir -p ${DONE_PATH}
#            touch ${DONE_PATH}/_DONE
#        """,
#    )

    
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_get >> task_save >> task_end 


