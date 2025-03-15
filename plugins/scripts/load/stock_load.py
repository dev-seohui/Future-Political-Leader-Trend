""" 📌 패키지 불러오기 """
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG

""" 📌 주가 데이터를 PostgreSQL에 적재 """
def load_stock_data(df):
    # ✅ 저장할 데이터가 없는 경우
    if df is None or df.empty:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ✅ 데이터 적재
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO stock_log (date, stock_code, close, high, low, open, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, date) DO NOTHING;
        """, (
            row["date"], row["stock_code"], row["close"],
            row["high"], row["low"], row["open"], row["volume"]
        ))

    conn.commit()   # ✅ 변경 사항 저장
    cur.close() # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기