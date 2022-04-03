from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
log = logging.getLogger(__name__)

def print_hello(ti):
    msg = 'value passing to other taskiii'
    log.warning('hi to world')
    ti.xcom_push(key='testing_increase', value=msg)
    log.warning('pass the value')

def testing_data(tii):
    log.warning("in test msg function")
    msg=tii.xcom_pull(key='testing_increase', task_ids='print_hello_task')
    log.warning(msg)
    log.warning("--done-")

with DAG('xcom_dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=2,
         schedule_interval=timedelta(minutes=30),
         default_args=default_args,
         catchup=False
         ) as dag:

        hello_operator = PythonOperator(task_id='print_hello_task', python_callable=print_hello, dag=dag)
        data_pass_operator = PythonOperator(task_id='data_pass_task', python_callable=testing_data, dag=dag)
        hello_operator >> data_pass_operator
