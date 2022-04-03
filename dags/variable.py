from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging

log = logging.getLogger(__name__)

def print_variable():
    log.warning('hello world')
    log.info(dir(Variable))
    
dag = DAG('variable', description='Variable values show', schedule_interval='0 12 * * *', start_date=datetime(2021, 12, 23))

hello_operator = PythonOperator(task_id='Variable', python_callable=print_variable, dag=dag)

hello_operator