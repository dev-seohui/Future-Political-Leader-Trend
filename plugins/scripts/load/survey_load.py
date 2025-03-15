import pandas as pd
import psycopg2

# PostgreSQL 연결
conn = psycopg2.connect(
    host="localhost",
    dbname="future_political_leader_trend",
    user="airflow",
    password="airflow"
)
cur = conn.cursor()

# 공통적으로 사용할 COPY 함수
def copy_to_db(file_path, table_name, columns):
    """
    CSV 파일을 PostgreSQL 테이블에 삽입 또는 업데이트하는 함수
    """
    # 2️⃣ CSV 데이터 로드
    df = pd.read_csv(file_path)
    
    # 3️⃣ 중복 제거 (등록번호 또는 기본키 기준)
    if "registration_number" in df.columns:
        df.drop_duplicates(subset=["registration_number"], keep="last", inplace=True)
    elif "survey_id" in df.columns:
        df.drop_duplicates(subset=["survey_id"], keep="last", inplace=True)
    
    # 4️⃣ 데이터 삽입 또는 업데이트
    for _, row in df.iterrows():
        values = tuple(row[col] for col in columns)
        query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
        ON CONFLICT ({columns[0]}) DO UPDATE SET
        {", ".join([f"{col} = EXCLUDED.{col}" for col in columns[1:]])};
        """
        cur.execute(query, values)

    conn.commit()
    print(f"✅ {table_name} 업데이트 완료!")

# 5️⃣ 각 테이블 업데이트 실행
copy_to_db("db/survey_data.csv", "survey_info", [
    "registration_number", "survey_agency", "client", 
    "survey_start_date", "survey_end_date", "survey_method", 
    "sample_size", "response_rate"
])

copy_to_db("db/candidate_log.csv", "candidate_log", [
    "cid", "survey_id", "candidate_name", "approval_rating"
])

copy_to_db("db/political_party_log.csv", "political_party_log", [
    "pid", "survey_id", "political_party_name", "support_rate"
])

# 6️⃣ 커서 및 연결 종료
cur.close()
conn.close()
print("✅ 모든 데이터 업데이트 완료!")
