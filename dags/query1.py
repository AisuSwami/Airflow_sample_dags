# run presto dags for 10 queries

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import presto_hook
import json

import logging
import boto3

import pandas as pd

json_path = '/opt/airflow/dags/user.json'
# json_path = '/Users/arankawat/Desktop/LocalAirflowSetup/dags/user.json'
presto_hook_obj = presto_hook.PrestoHook('presto-default.prod.twilio.com',8443,'hive','public', json_path )

log = logging.getLogger(__name__)

args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}
def print_data():
    data = presto_hook_obj.get_conn()
    cursor = data.cursor()
    query = "Select * from public.account_level_regulatory_compliance_daily_metrics limit 1 "
    cursor.execute(query)
    columns = cursor.description 
    log.warning(columns)
    # result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
    # rows = cursor.fetchall()
    
    result = cursor.fetchall()
    log.warning(result)
    
    if result:
            df = pd.DataFrame(result)

            log.info("df")
            log.info(df)
            
            column_descriptions = cursor.description

            df.columns = [c[0] for c in column_descriptions]

            log.info("df.columns")
            log.info(df.columns )
            # if self.new_columns_name is None: 
            #     df.rename(columns=self.new_columns_name, inplace=True)

            # y = df.head()

            # log.info("y = df.head()")
            # log.info(y)
            
    s3 = boto3.resource('s3')
    s3_bucket ="com.twilio.dev.trust-insights"
    s3_key = "intermediate_results/dag_{}/{}".format('copy_from_intermediate_s3', '2')

        # if self.new_columns_name is not None:
    s3_object = s3.Object(s3_bucket,s3_key)
    bytes_to_write = df.to_csv(None, index=False).encode()
    s3_object.put(Body=bytes_to_write)
    # return rows
    
dag = DAG('presto_for_query1',default_args=args, description='return data', schedule_interval='15 10 * * *', start_date=datetime(2022, 1, 19))

presto_task_for_query1 = PythonOperator(task_id='simple_query', python_callable=print_data, dag=dag)

presto_task_for_query1