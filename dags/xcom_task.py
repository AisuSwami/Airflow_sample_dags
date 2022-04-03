from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

log = logging.getLogger(__name__)

def print_hello(ti):
    msg = 'hi'
    log.warning('hi to world')
    ti.xcom_push(key='testing_increase', value=msg)
    log.warning('pass the value')

def testing_data(ti):
    log.warning("in test msg function")
    msg=ti.xcom_pull(key='testing_increase', task_ids='print_hello_task')
    log.warning(msg)
    log.warning("--done-")

dag = DAG('Pass_Data', description='Hello world', schedule_interval='20 10 * * *', start_date=datetime(2021, 12, 23)) as dag :

hello_operator = PythonOperator(task_id='print_hello_task', python_callable=print_hello, dag=dag)
data_pass_operator = PythonOperator(task_id='data_pass_task', python_callable=print_hello, dag=dag)
hello_operator >> data_pass_operator



