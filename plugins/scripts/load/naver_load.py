import psycopg2
from config.settings import DB_CONFIG

def load_naver_data(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_query = """
        INSERT INTO naver_trend (
            date, keyword, news_count, blog_count, total_news_count, total_blog_count
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, keyword) DO UPDATE
        SET news_count = EXCLUDED.news_count,
            blog_count = EXCLUDED.blog_count,
            total_news_count = EXCLUDED.total_news_count,
            total_blog_count = EXCLUDED.total_blog_count;
    """

    for row in data:
        date, keyword, news_count, blog_count, total_news, total_blog = row
        cur.execute(insert_query, row)
        
    conn.commit()
    cur.close()
    conn.close()