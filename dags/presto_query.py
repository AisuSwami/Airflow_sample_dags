# run presto dags for 10 queries

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import presto_hook
import json

import logging

json_path = '/opt/airflow/dags/user.json'

# json_object = json.dumps(dic, indent = 2)  

presto_hook_obj = presto_hook.PrestoHook('presto-default.prod.twilio.com',8443,'hive','public', json_path )

# with open(json_path, "r") as read_file:
#     data = json.load(read_file)
#     print(data)
log = logging.getLogger(__name__)

def print_data():
    data = presto_hook_obj.get_conn()
    cur = data.cursor()
    cur.execute('SELECT DISTINCT account_sid FROM public.account_flags limit 5')
    rows = cur.fetchall()
    
    log.warning(rows)
    

print(presto_hook_obj)    

dag = DAG('presto_data', description='return data', schedule_interval='0 12 * * *', start_date=datetime(2022, 1, 19))

presto_task = PythonOperator(task_id='presto_task01', python_callable=print_data, dag=dag)

presto_task 

 
    
