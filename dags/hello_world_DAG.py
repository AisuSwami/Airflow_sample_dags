from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

log = logging.getLogger(__name__)

def print_hello():
    log.warning('hello world')
    
dag = DAG('hello_world', description='Hello world', schedule_interval='0 12 * * *', start_date=datetime(2021, 12, 23), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator