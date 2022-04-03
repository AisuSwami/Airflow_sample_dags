from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import presto_hook
import json
import sql_query

json_path = '/opt/airflow/dags/user.json'
presto_hook_obj = presto_hook.PrestoHook('presto-default.prod.twilio.com',8443,'hive','public', json_path )

log = logging.getLogger(__name__)

def print_data():
    data = presto_hook_obj.get_conn()
    cur = data.cursor()
    query = "SELECT account_sid, auto_id from public.account_flags limit 2"
    cur.execute(query)
    rows = cur.fetchall()  
    log.warning(rows)
     

dag = DAG('sms_fraud', description='return data', schedule_interval="0 6 * * *", start_date=datetime(2022, 1, 19))

presto_task= PythonOperator(task_id='sms_fraud1', python_callable=print_data, dag=dag)

presto_task