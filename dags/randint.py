from random import randint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from random import randint  
from datetime import datetime
import logging

log = logging.getLogger(__name__)

def _function():
    log.warning(randint(1,10))
      
dag = DAG('my_dag', schedule_interval="@daily", start_date=datetime(2022, 1, 13))

task_1 = PythonOperator(task_id = "task_1", python_callable = _function,dag=dag )

task_1
