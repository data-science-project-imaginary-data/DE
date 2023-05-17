from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def mongo_ETL():
    try:

        # Connect MongoDB
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        db = client.myDB
        collection=db.data

        # Extract
        df = pd.read_csv('https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv')

        # Transform 
        data_dict = df.to_dict(orient='records')
        data_dict_cond = [x for x in data_dict if (x['state'] == 'เสร็จสิ้น' and (int)(x['timestamp'][0:4])>=2022 and (int)(x['timestamp'][5:7])>=7)]
        for x in data_dict_cond:
            if(type(x['type'])!=float):
                x['type'] = x['type'][1:-1].split(',')

        # Load 
        collection.delete_many({})
        collection.insert_many(data_dict_cond)

        print('Finish')
    
    except Exception as e:
        print(e)

with DAG(
    dag_id="myDag1",
    schedule_interval=None,
    start_date=datetime(2023,4,20),
    catchup=False,
    default_args={
        "owner": "Oat",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    t1 = PythonOperator(
        task_id='mongo_ETL',
        python_callable=mongo_ETL,
        provide_context=True,
        dag=dag
    )

    t1