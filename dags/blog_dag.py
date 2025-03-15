import json
import yfinance as yf
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config.settings import DB_CONFIG

""" 📌 PostgreSQL에서 주식 종목 코드 가져오기 """
def get_stock_list():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT short_code, market_type FROM stock_info")
    stock_list = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    conn.close()

    # ✅ 시장 유형에 따라 종목 코드에 접미사 추가 (KOSPI -> .KS, KOSDAQ -> .KQ)
    formatted_list = [f"{code}.KS" if market == "KOSPI" else f"{code}.KQ" for code, market in stock_list]
    
    return formatted_list

""" 📌 yfinance에서 어제의 주가 데이터 추출 """
def extract_stock_data():
    stock_list = get_stock_list()

    # 주가 데이터 다운로드 기간 설정
    START_DATE = "2025-03-12"
    END_DATE = "2025-03-13"

    dfs = []
    for symbol in stock_list:
        df = yf.download(symbol, start=START_DATE, end=END_DATE)

        if not df.empty:
            # ✅ MultiIndex 제거
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)

            # ✅ 날짜 컬럼을 문자열로 변환하여 JSON 변환 오류 방지
            df.reset_index(inplace=True)
            df["Date"] = df["Date"].astype(str)

            # ✅ 종목 코드 추가
            df.insert(1, 'stock_code', symbol.split('.')[0])
            dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    print("📌 Extracted Data:")
    print(final_df.head())

    return final_df.to_json(orient="records")


""" 📌 주가 데이터를 PostgreSQL에 적재 """
def load_stock_data(df_json):
    if df_json is None:
        print("⚠️ No data to load!")
        return

    df = pd.read_json(df_json, orient="records")  # ✅ split → records로 변경
    if df.empty:
        print("⚠️ Loaded an empty DataFrame from XCom!")
        return

    print(f"✅ Loaded {len(df)} rows from XCom")
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO stock_log (date, stock_code, close, high, low, open, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, stock_code) DO NOTHING;
            """, (
            row["Date"], row["stock_code"], row["Close"],
            row["High"], row["Low"], row["Open"], row["Volume"]
            ))
    conn.commit()
    cur.close()
    conn.close()

""" 📌 DAG 실행 파라미터 """
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 14),  # ✅ 테스트 날짜 설정
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

""" 📌 DAG 정의 """
dag = DAG(
    "instant_stock_dag",
    default_args=default_args,
    description="주가 데이터 갱신",
    schedule_interval=None,  # ✅ 수동 실행으로 변경
    catchup=False,
)

""" 📌 데이터 추출 Task 정의 """
extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract_stock_data,
    dag=dag
)

""" 📌 데이터 적재 Task 정의 """
def load(**context):
    extracted_data_json = context['task_instance'].xcom_pull(task_ids="extract")

    if extracted_data_json is None:
        print("❌ No data received from XCom!")
        return

    load_stock_data(extracted_data_json)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,  # ✅ XCom을 가져오도록 설정
    dag=dag
)

""" 📌 Task 실행 순서 정의 """
extract_task >> load_task
