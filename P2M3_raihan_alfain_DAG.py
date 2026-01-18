import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "raihan_alfain",                 
    "start_date": dt.datetime(2024, 11, 1),   # Sesuai ketentuan tugas
    "retries": 1,
    'retry_delay': dt.timedelta(minutes=10),
}

with DAG(
    dag_id="P2M3_raihan_alfain_DAG",          
    default_args=default_args,
    schedule_interval="10,20,30 9 * * 6",     # Sabtu 09:10, 09:20, 09:30
    catchup=False,
) as dag:

    python_extract = BashOperator(task_id='python_extract', bash_command='python /opt/airflow/scripts/extract.py')
    python_transform = BashOperator(task_id='python_transform', bash_command='python /opt/airflow/scripts/transform.py')
    python_load = BashOperator(task_id='python_load', bash_command='python /opt/airflow/scripts/load.py')

python_extract >> python_transform >> python_load