from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint as pp

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

    REQUIREMENTS=[
        "git+https://github.com/lsiwh37249/mov_agg.git@0.5/agg"
                ]

    def vpython(**kw):
        task  = PythonVirtualenvOperator(
            task_id=kw['id'],
            python_callable=kw['fun_obj'],
            requirements=REQUIREMENTS,
            system_site_packages=False,
            trigger_rule="all_done",
            op_kwargs =  kw['op_kwargs']
            #op_kwargs={
            #    "url_param" : {"multiMovieYn": "y"}
            #    }
            )
        return task
    
    def pro_data2(task_name ,**params):
         print("@" * 33)
         print(task_name)
         #from pprint import pprint as pp
         ##print(params)
         #pp(params)
         print("@" * 33)         

    def pro_data3(task_name):
         print("@" * 33)
         print(task_name)
         print("@" * 33)

    def pro_merge(task_name, **params):
        load_dt = params['ds_nodash']
        from mov_agg.u import merge
        df = merge(load_dt)
        
        print("*" * 33)

        df.loc[df['multiMovieYn_n'] == 'N', 'multiMovieYn_m'] = 'N'
        print(df)
   

#    apply_Atype, apply.Btype, apply.Ctype, apply.Dtype = 0 
#    my_tasks = [apply_Atype, apply.Btype, apply.Ctype, apply.Dtype] 
#    for my_task,task in zip(my_tasks,vpython("apply.Atype","apply.Btype","apply.Ctype","apply.Dtype")):
#        my_task = task
#    apply_Atype = vpython("apply.type", pro_data, {"url_param" : {"multiMovieYn": "y"}})     
    apply_Atype = vpython(id ="apply.type", fun_obj = pro_data2, op_kwargs = {"task_name" : "apply_type!!!"})     
#    apply_Btype = vpython({'task_ID' : 'apply.Btype'})
#    apply_Ctype = vpython('apply.Ctype')
#    apply_Dtype = vpython('apply.Dtype')

    merge_df = vpython(id ="merge.df", fun_obj = pro_merge, op_kwargs = {"task_name": "merge_df!!!"})     

    de_dup = vpython(id ="de.dup", fun_obj = pro_data2, op_kwargs = {"task_name": "de_dup!!!"})     
    
    summary_df = vpython(id ="summary.df", fun_obj = pro_data2, op_kwargs = {"task_name": "summary_df!!!"})     


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

#    task_start >> [apply_Atype, apply_Btype, apply_Ctype, apply_Dtype] >> merge_df >> de_dup >> summary_df >> task_end
    task_start >> merge_df
    merge_df >> de_dup >> apply_Atype
    apply_Atype >>  summary_df >> task_end
