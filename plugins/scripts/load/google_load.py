""" 📌 패키지 불러오기 """
import psycopg2
from config.settings import DB_CONFIG


""" 📌 트렌드 데이터를 PostgreSQL에 적재 """
def load_google_data(**context):
    
    extracted_data = context["task_instance"].xcom_pull(task_ids="extract")
    
    # ✅ 타겟 데이터가 없는 경우
    if not extracted_data:
        print("⚠️ No data to load.")  
        return
    
    conn = psycopg2.connect(**DB_CONFIG)    # ✅ PostgreSQL 연결 객체
    cur = conn.cursor() # ✅ SQL 실행을 위한 커서 객체

    # ✅ 데이터 적재
    insert_query = """
        INSERT INTO google_trend (date, keyword, trend_score)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, keyword) DO UPDATE
        SET trend_score = EXCLUDED.trend_score;
    """
    for row in extracted_data:
        cur.execute(insert_query, (row["date"], row["keyword"], row["trend_score"]))

    conn.commit()   # ✅ 변경 사항 저장
    cur.close() # ✅ 커서 닫기
    conn.close()    # ✅ PostgreSQL 연결 닫기
