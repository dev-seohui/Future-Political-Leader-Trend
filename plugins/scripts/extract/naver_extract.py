""" 📌 패키지 불러오기 """
import urllib.request
import json
import psycopg2
from datetime import datetime, timedelta
from config.settings import CLIENT_ID, CLIENT_SECRET, DB_CONFIG


""" 📌 네이버 API 요청으로 누적된 네이버 기사 개수 & 블로그 포스팅 개수 가져오기 """
def get_total_count(api_url):
    request = urllib.request.Request(api_url)
    request.add_header("X-Naver-Client-Id", CLIENT_ID)
    request.add_header("X-Naver-Client-Secret", CLIENT_SECRET)

    try:
        response = urllib.request.urlopen(request)
        if response.getcode() == 200:
            result = json.loads(response.read().decode("utf-8"))
            return result.get("total", 0)
    except Exception as e:
        print("❌ Error:", e)
    return 0


""" 📌 후보 목록 불러오기 (candidate_info 테이블) """
def get_candidate_list():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT candidate_name FROM candidate_info;")
    candidates = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return candidates


""" 📌 이전 누적값 불러오기 (naver_trend 테이블에서 가장 최근 날짜 기준) """
def get_previous_cumulative_counts():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT keyword, total_news_count, total_blog_count
        FROM naver_trend
        WHERE date = (SELECT MAX(date) FROM naver_trend)
    """)
    result = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    cur.close()
    conn.close()
    return result


""" 📌 최종 추출 함수: 일일/누적 뉴스 & 블로그 카운트 계산 """
def extract_naver_data():
    today = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    candidate_list = get_candidate_list()
    prev_counts = get_previous_cumulative_counts()
    data_to_insert = []

    for keyword in candidate_list:
        encoded = urllib.parse.quote(keyword)
        news_url = f"https://openapi.naver.com/v1/search/news?query={encoded}"
        blog_url = f"https://openapi.naver.com/v1/search/blog?query={encoded}"

        total_news = get_total_count(news_url)
        total_blog = get_total_count(blog_url)

        prev_news, prev_blog = prev_counts.get(keyword, (0, 0))
        daily_news = total_news - prev_news
        daily_blog = total_blog - prev_blog

        data_to_insert.append((
            today, keyword,
            daily_news, daily_blog,
            total_news, total_blog
        ))

    return data_to_insert