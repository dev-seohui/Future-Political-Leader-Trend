""" 📌 패키지 불러오기 """
import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from config.settings import DB_CONFIG


""" 📌 PostgreSQL에서 주식 종목 코드 가져오기"""
def get_stock_list():
    conn = psycopg2.connect(**DB_CONFIG)    # ✅ PostgreSQL 연결 객체
    cur = conn.cursor() # ✅ SQL 실행을 위한 커서 객체
    cur.execute("SELECT short_code FROM stock_info")    # ✅ SQL 실행
    stock_list = [row[0] for row in cur.fetchall()] # ✅ 리스트로 변환
    cur.close() # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기
    return stock_list

""" 📌 yfinance에서 어제의 주가 데이터 추출"""
def extract_stock_data():
    stock_list = get_stock_list()
    START_DATE = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    END_DATE = START_DATE
    result = []    # ✅ 리스트로 변환

    for code in stock_list:
        SYMBOL = f"{code}.KS"
        try:
            df = yf.download(SYMBOL, start=START_DATE, end=END_DATE)
            
            if not df.empty:
                df.columns = df.columns.droplevel(1) if isinstance(df.columns, pd.MultiIndex) else df.columns
                df.reset_index(inplace=True)
                df.insert(1, 'stock_code', code)
                result.append(df)
        except Exception as e:
            continue

    return pd.concat(result, ignore_index=True) if result else pd.DataFrame()

