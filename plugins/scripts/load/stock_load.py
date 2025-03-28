""" 📌 패키지 불러오기 """
import json
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG


""" 📌 주가 데이터를 PostgreSQL에 적재 """
def load_stock_data(df_json):
    
    # ✅ 타겟 데이터가 없는 경우
    if df_json is None:
        print("⚠️ No data to load.")
        return

    # ✅ XCom에서 전달된 JSON 데이터를 Pandas DataFrame으로 변환
    df = pd.read_json(df_json, orient="records")

    # ✅ XCom에서 데이터가 넘어오지 않는 경우
    if df.empty:
        print("⚠️ Loaded an empty DataFrame from XCom.")
        return

    # ✅ stock_code 값 올바른 형식으로 유지하기
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)

    conn = psycopg2.connect(**DB_CONFIG)     # ✅ PostgreSQL 연결 객체
    cur = conn.cursor() # ✅ SQL 실행을 위한 커서 객체

    # ✅ 데이터 적재
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO stock_log (date, stock_code, close, high, low, open, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, stock_code) DO NOTHING;
            """, (
            row["Date"], row["stock_code"], row["Close"],
            row["High"], row["Low"], row["Open"], row["Volume"]
            ))
        
    conn.commit()   # ✅ 변경 사항 저장
    cur.close() # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기