""" 📌 패키지 불러오기 """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import os
from config.settings import DB_CONFIG  


""" 📌 DAG 실행 파라미터 """
default_args = {
    "owner": "airflow", # ✅ DAG 소유자 정보 (UI 표시)
    "depends_on_past": False,   # ✅ 이전 DAG 실행 결과에 의존하지 않음 (즉, 실패해도 다음 실행에 영향 없음)
    "start_date": datetime(2024, 3, 25),    # ✅ DAG 시작 날짜
    "retries": 1,   # ✅ 작업이 실패할 경우 한 번 재시도함
    "retry_delay": timedelta(minutes=5),    # ✅ 실패 후 재시도 간격 (5분 후 다시 시도)
}


""" 📌 DAG 정의 """
dag = DAG(
    "upload_to_gsheets",    # ✅ DAG 이름 (고유 식별자)
    default_args=default_args,
    description="PostgreSQL -> Google Sheet", # ✅ DAG 설명 (UI 표시)
    schedule_interval="30 0 * * *", # ✅ 매일 00:30 실행
    catchup=False,  # ✅ 과거 데이터 실행 여부 (실행 X)
)

def upload_to_gsheets():
    """PostgreSQL의 전체 데이터를 Google Sheets에 Append 방식으로 추가"""

    # ✅ Google Sheets API 인증
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # DAG 파일이 있는 폴더
    CREDENTIALS_PATH = os.path.join(BASE_DIR, "../tableau/gsheet_credential.json")  # JSON 파일 경로

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)

    # 2️⃣ PostgreSQL 연결
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    # 3️⃣ 여러 개의 테이블 리스트 (전체 데이터 업로드)
    tables = [
        "survey_info", "candidate_info", "political_party_info", "stock_info", "theme_info",
        "survey_log", "candidate_log", "political_party_log", "stock_log", "google_trend", "naver_trend"
    ]

    # 4️⃣ Google Sheets 문서 확인 (없으면 생성)
    spreadsheet_name = "postgresql"  # Google Sheets 문서 이름
    try:
        spreadsheet = client.open(spreadsheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(spreadsheet_name)
        spreadsheet.share(
            'tableau-postgresql-sync-1@applied-craft-453903-e1.iam.gserviceaccount.com', 
            perm_type='user', 
            role='writer'
        )

    # 5️⃣ 테이블별로 Google Sheets에 추가(append) 방식으로 업데이트
    for table in tables:
        query = f"SELECT * FROM {table}"  # ❗ 전체 데이터 가져오기
        df = pd.read_sql(query, engine)

        # ✅ 모든 날짜/시간 컬럼을 문자열로 변환하여 JSON 직렬화 오류 방지
        for col in df.select_dtypes(include=['datetime64[ns]', 'object']).columns:
            df[col] = df[col].astype(str)

        # 특정 테이블에 해당하는 워크시트 찾기 (없으면 생성)
        try:
            sheet = spreadsheet.worksheet(table)  # 테이블명과 동일한 시트 선택
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=table, rows="20000", cols="50")  # 충분한 크기로 시트 생성

        # 7️⃣ 기존 데이터 삭제고 새로운 데이터 추가
        sheet.clear()

        # ✅ 컬럼명 추가 후 데이터 전체 업데이트
        sheet.update([df.columns.values.tolist()] + df.values.tolist())

upload_task = PythonOperator(
    task_id="upload_data_to_gsheets",
    python_callable=upload_to_gsheets,
    dag=dag,
)

upload_task
