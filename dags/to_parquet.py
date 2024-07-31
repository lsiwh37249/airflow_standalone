import pandas as pd
import sys

def to_parquet(q):

    df = pd.read_csv('~/data/csv/20240717/csv.csv', 
                 on_bad_lines='skip', 
                 names=['dt', 'cmd', 'cnt'])

    df['dt'] = df['dt'].str.replace('^', '')
    df['cmd'] = df['cmd'].str.replace('^', '')
    df['cnt'] = df['cnt'].str.replace('^', '')

    df['cnt'] = df['cnt'].astype(int)

    df.to_parquet("~/airflow/parquet/{{ ds }}/history.parquet")


