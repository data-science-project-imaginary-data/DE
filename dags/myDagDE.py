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
        data_dict_cond = [x for x in data_dict if (x['state'] == 'เสร็จสิ้น' and (int)(x['timestamp'][0:4])>=2022 and (int)(x['timestamp'][5:7])>=7 and type(x['type'])!=float and len(x['type'])>2 and type(x['comment'])!=float and type(x['subdistrict'])!=float and type(x['district'])!=float and type(x['coords'])!=float)]
        print(len(data_dict_cond))
        for x in data_dict_cond:
            # type
            x['type'] = x['type'][1:-1].split(',')
            # organization
            org_list = ['เขตคลองเตย','เขตคลองสาน','เขตคลองสามวา','เขตคันนายาว','เขตจตุจักร','เขตจอมทอง','เขตดอนเมือง','เขตดินแดง','เขตดุสิต','เขตตลิ่งชัน','เขตทวีวัฒนา','เขตทุ่งครุ','เขตธนบุรี','เขตบางกอกน้อย','เขตบางกอกใหญ่','เขตบางกะปิ','เขตบางขุนเทียน','เขตบางเขน','เขตบางคอแหลม','เขตบางแค','เขตบางซื่อ','เขตบางนา','เขตบางบอน','เขตบางพลัด','เขตบางรัก','เขตบึงกุ่ม','เขตปทุมวัน','เขตประเวศ','เขตป้อมปราบศัตรูพ่าย','เขตพญาไท','เขตพระโขนง','เขตพระนคร','เขตภาษีเจริญ','เขตมีนบุรี','เขตยานนาวา','เขตราชเทวี','เขตราษฎร์บูรณะ','เขตลาดกระบัง','เขตลาดพร้าว','เขตวังทองหลาง','เขตวัฒนา','เขตสวนหลวง','เขตสะพานสูง','เขตสัมพันธวงศ์','เขตสาทร','เขตสายไหม','เขตหนองแขม','เขตหนองจอก','เขตหลักสี่','เขตห้วยขวาง','สำนักการคลัง กทม.','สำนักการจราจรและขนส่ง กรุงเทพมหานคร (สจส.)','สำนักการแพทย์ กทม.','สำนักการโยธา กทม.','สำนักการระบายน้ำ กทม.','สำนักการวางผังและพัฒนาเมือง กทม.','สำนักการศึกษา กทม.','สำนักงบประมาณกรุงเทพมหานคร','สำนักงานคณะกรรมการข้าราชการกรุงเทพมหานคร','สำนักเทศกิจ กทม.','สำนักป้องกันและบรรเทาสาธารณภัย กทม.','สำนักพัฒนาสังคม กทม.','สำนักยุทธศาสตร์และประเมินผล กทม.','สำนักวัฒนธรรม กีฬาและการท่องเที่ยว กทม.','สำนักสิ่งแวดล้อม กทม.','สำนักอนามัย กทม.']
            new_org = list()
            for y in org_list:
                if type(x['organization'])!=float and y in x['organization']:
                    new_org += [y]
            if len(new_org) == 0:
                new_org = ['เขต' + x['district']]
            x['organization'] = new_org
            # coords
            x['coords'] = x['coords'].split(',')

        # Load 
        collection.delete_many({})
        collection.insert_many(data_dict_cond)

        print('Finish')
    
    except Exception as e:
        print(e)

with DAG(
    dag_id="myDagDE",
    schedule_interval="@daily",
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