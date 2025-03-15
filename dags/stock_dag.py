""" 📌 패키지 불러오기 """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract.stock_extract import extract_stock_data
from scripts.load.stock_load import load_stock_data

""" 📌 DAG 실행 파라미터 """
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}   # ✅ DAG의 기본 설정값을 정의하는 딕셔너리

""" 📌 DAG 정의 """
dag = DAG(
    "stock_dag",    # ✅ DAG 이름 (고유 식별자)
    default_args=default_args,  
    description="주가 데이터 갱신", # ✅ DAG 설명 (UI 표시)
    schedule_interval="0 10 * * *", # ✅ 실행 주기 (매일 오전 10시)
    catchup=False,  # ✅ 과거 데이터 실행 여부 (실행 X)
)

""" 📌 데이터 추출 함수 정의 """
def extract():
    return extract_stock_data()

""" 📌 데이터 적재 함수 정의 """
def load(**context):
    transformed_data = context['task_instance'].xcom_pull(task_ids="extract")
    load_stock_data(transformed_data)

""" 📌 데이터 추출 Task 정의 """
extract_task = PythonOperator(
    task_id="extract",  # ✅ Task의 고유 ID 
    python_callable=extract,    # ✅ 실행할 Python 함수 
    dag=dag # ✅ 해당 DAG에 연결
)

""" 📌 데이터 적재 Task 정의 """
load_task = PythonOperator(
    task_id="load",     # ✅ Task의 고유 ID
    python_callable=load,   # ✅ 실행할 Python 함수
    dag=dag # ✅ 해당 DAG에 연결
)

""" 📌 Task 실행 순서 정의 """
extract_task >> load_task
