from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

log = logging.getLogger(__name__)

def print_hello():
    log.warning('hello world')

def welcome():
    log.warning('welcome to world')

def thanks_msg():
    log.warning('Thankyou')    
    
dag = DAG('multi', description='Hello world', schedule_interval='0 15 10 * * *', start_date=datetime(2022, 1, 15))
#At 12 midnight on every day for five days starting on the 10th day of the month

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
#python callable :  A reference to an object that is callable

welcome_operator = PythonOperator(task_id='welcome_task', python_callable=welcome, dag=dag)

thank_operator = PythonOperator(task_id='thanks_task', python_callable=thanks_msg, dag=dag)

hello_operator >> [welcome_operator , thank_operator ]
