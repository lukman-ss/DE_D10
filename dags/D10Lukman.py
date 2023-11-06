from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd
from datetime import datetime
import pytz

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 8, 1, tz='Asia/Jakarta'),
    'retries': 0,
    'retry_delay': pendulum.duration(seconds=10),
}

with DAG(
    'D10Lukman',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
):

    def run_task():
        hook = PostgresHook(postgres_conn_id='lukman-steven')
        
        conn = hook.get_conn()
        
        df = pd.read_sql("""select t.*, a.title, a2.name as vb          
            from public.tracks t 
            inner join public.albums a 
            on t.album_id = a.id 
            inner join public.artists a2 
            on a.artist_id = a2.id 
            where t.id = 1""", conn)
        df['track'] = df[['name']]
        df['DurationMinutes'] = df[['milliseconds']] / 60000
        df['etl_timestamp'] = datetime.now(pytz.timezone(
            'Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
        df['Student Name'] = 'Lukman'

        df = df[['id', 'track', 'name', 'title', 'vb', 'media_type_id', 'genre_id', 'composer',
                 'milliseconds', 'bytes', 'unit_price', 'DurationMinutes', 'etl_timestamp', 'Student Name']]
        df1 = df.values.tolist()

        google_sheet_hook = GSheetsHook(
            gcp_conn_id="g_sheet")
        google_sheet_hook.append_values(
            "1CDqjVawY8aKLQD6ycQoQ6mUQ4TSbdrO5lh6MuxdaLhQ", "Production", df1)

    task_extract = PythonOperator(
        task_id='run_taskes',
        python_callable=run_task,
    )
