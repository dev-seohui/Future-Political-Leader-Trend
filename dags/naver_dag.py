""" 📌 패키지 불러오기 """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract.naver_extract import extract_naver_data
from scripts.load.naver_load import load_naver_data


""" 📌 DAG 실행 파라미터 """
default_args = {
    "owner": "airflow", # ✅ DAG 소유자 정보 (UI 표시)
    "depends_on_past": False,   # ✅ 이전 DAG 실행 결과에 의존하지 않음 (즉, 실패해도 다음 실행에 영향 없음)
    "start_date": datetime(2025, 3, 25),    # ✅ DAG 시작 날짜
    "retries": 1,   # ✅ 작업이 실패할 경우 한 번 재시도함
    "retry_delay": timedelta(minutes=5),    # ✅ 실패 후 재시도 간격 (5분 후 다시 시도)
}


""" 📌 DAG 정의 """
dag = DAG(
    "naver_dag",  # ✅ DAG 이름 (고유 식별자)
    default_args=default_args,
    description="네이버 뉴스 기사 수 & 블로그 포스팅 수 갱신",  # ✅ DAG 설명 (UI 표시)
    schedule_interval="1 0 * * *",  # ✅ 매일 00:01 실행
    catchup=False,  # ✅ 과거 데이터 실행 여부 (실행 X)
)


""" 📌 데이터 추출 함수 정의 """
def extract():
    return extract_naver_data()


""" 📌 데이터 적재 함수 정의 """
def load(**context):
    extracted_data = context['task_instance'].xcom_pull(task_ids="extract")

    if extracted_data is None:
        print("❌ No data received from XCom!")
        return

    load_naver_data(extracted_data)


""" 📌 데이터 추출 Task 정의 """
extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag
)


""" 📌 데이터 적재 Task 정의 """
load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,
    dag=dag
)


""" 📌 Task 실행 순서 정의 """
extract_task >> load_task
