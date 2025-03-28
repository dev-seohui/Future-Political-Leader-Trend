""" 📌 패키지 불러오기 """
import json
import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from config.settings import DB_CONFIG


""" 📌 PostgreSQL에서 주식 종목 코드 가져오기 """
def get_stock_list():
    conn = psycopg2.connect(**DB_CONFIG)    # ✅ PostgreSQL 연결 객체
    cur = conn.cursor() # ✅ SQL 실행을 위한 커서 객체

    # ✅ SQL 실행하여 종목 코드, 상장 마켓 가져오기
    cur.execute("SELECT short_code, market_type FROM stock_info")    
    stock_list = [(row[0], row[1]) for row in cur.fetchall()]   # ✅ 리스트로 변환
    
    cur.close() # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기

    # ✅ 시장 유형에 따라 종목 코드에 접미사 추가 (KOSPI -> .KS, KOSDAQ -> .KQ)
    formatted_list = [f"{code}.KS" if market == "KOSPI" else f"{code}.KQ" for code, market in stock_list]
    
    return formatted_list


""" 📌 yfinance에서 어제의 주가 데이터 추출"""
def extract_stock_data():
    stock_list = get_stock_list()

    # ✅ 주가 데이터 다운로드 기간 설정
    START_DATE = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    END_DATE = (datetime.today()).strftime('%Y-%m-%d')

    dfs = []    
    for symbol in stock_list:
        # ✅ symbol: 주식 종목 코드 (예: "GOOGL" → 구글)
        # ✅ start: 데이터를 가져올 시작 날짜
        # ✅ end: 데이터를 가져올 종료 날짜
        df = yf.download(symbol,    
                         start=START_DATE, 
                         end=END_DATE)

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

    # ✅ 모든 데이터프레임 병합  
    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return final_df.to_json(orient="records")
