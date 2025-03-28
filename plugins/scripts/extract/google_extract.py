""" 📌 패키지 불러오기 """
import time
import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from config.settings import DB_CONFIG


""" 📌 Pytrends 객체 생성 """
# ✅ hl='ko-KR' : Google Trends 데이터를 한국어(ko-KR)로 가져옴.
# ✅ tz=540 : 한국 표준시(KST, UTC+9) 기준으로 데이터를 가져옴. (540분 = 9시간)
pytrends = TrendReq(hl='ko-KR', tz=540, backoff_factor=5, retries=5)


""" 📌 PostgreSQL에서 후보자 이름 가져오기 """
def get_candidate_list():
    conn = psycopg2.connect(**DB_CONFIG)    # ✅ PostgreSQL 연결 객체
    cur = conn.cursor() # ✅ SQL 실행을 위한 커서 객체
    
    # ✅ SQL 실행하여 후보자 이름 가져오기
    cur.execute("SELECT candidate_name FROM candidate_info;")
    keywords = [row[0] for row in cur.fetchall()]
    
    cur.close()    # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기

    return keywords


""" 📌 pytrends에서 어제의 트렌드 데이터 추출"""
def extract_google_data():
    keywords = get_candidate_list()

    # ✅ 트렌드 데이터 다운로드 기간 설정
    today_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  
    PERIOD = f"{today_date} {today_date}"   # ✅ timeframe 형식 변경

    dfs = []    
    for keyword in keywords:
        # ✅ kw_list: 검색할 키워드 (리스트 형식, 한 번에 여러 개도 가능)
        # ✅ timeframe: 검색할 기간 ("today 1-m" → 최근 한 달간 데이터)
        # ✅ geo: 'KR' → 대한민국 기준 데이터 조회
        pytrends.build_payload(kw_list=[keyword], 
                               timeframe=PERIOD, 
                               geo='KR')
        df = pytrends.interest_over_time()

        if df.empty:
            continue
        
        # ✅ 날짜 컬럼 추가
        df = df.reset_index() 
        df = df[["date", keyword]]

        # ✅ 컬럼 세부 정보 변경
        df.rename(columns={keyword: "trend_score"}, inplace=True)
        df["keyword"] = keyword

        dfs.append(df)
        time.sleep(random.uniform(60, 120)) # ✅ 5~10초 랜덤 딜레이 추가

    if not dfs:
        raise ValueError("❌ No valid data retrieved from Pytrends.")

    # ✅ 모든 데이터프레임 병합  
    final_df = pd.concat(dfs, ignore_index=True)
    final_df["date"] = final_df["date"].astype(str)
    
    return final_df.to_dict(orient="records")
