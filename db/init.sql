-- 해당 데이터베이스를 사용
\c future_political_leader_trend;

-- survey_info 
CREATE TABLE IF NOT EXISTS survey_info (
    survey_id SERIAL PRIMARY KEY,
    registration_number INT NOT NULL,
    survey_agency TEXT NOT NULL,
    client TEXT NOT NULL,
    survey_start_date DATE NOT NULL,
    survey_end_date DATE NOT NULL,
    survey_method TEXT NOT NULL,
    sampling_frame TEXT NOT NULL,
    sample_size INT NOT NULL,
    contact_rate FLOAT NOT NULL,
    response_rate FLOAT NOT NULL,
    margin_of_error_95ci TEXT NOT NULL
);

-- candidate_info
CREATE TABLE IF NOT EXISTS candidate_info (
    candidate_id SERIAL PRIMARY KEY,
    candidate_name TEXT UNIQUE NOT NULL
);

-- political_party_info
CREATE TABLE IF NOT EXISTS political_party_info (
    political_party_id SERIAL PRIMARY KEY, 
    political_party_name TEXT UNIQUE NOT NULL
);

-- stock_info
CREATE TABLE IF NOT EXISTS stock_info (
    stock_id SERIAL PRIMARY KEY,
    standard_code TEXT UNIQUE NOT NULL,
    short_code TEXT UNIQUE NOT NULL,
    kor_stock_name TEXT NOT NULL,
    kor_stock_abbr TEXT UNIQUE NOT NULL,
    eng_stock_name TEXT NOT NULL,
    listing_date DATE NOT NULL,
    market_type TEXT NOT NULL,
    security_type TEXT NOT NULL,
    affiliated_dept TEXT,
    stock_type TEXT NOT NULL,
    listed_shares BIGINT NOT NULL
);

-- survey_log
CREATE TABLE IF NOT EXISTS survey_log (
    survey_id INT NOT NULL,
    survey_start_date DATE NOT NULL,
    survey_end_date DATE NOT NULL,
    date DATE NOT NULL,
    FOREIGN KEY (survey_id) REFERENCES survey_info(survey_id) ON DELETE CASCADE,
    PRIMARY KEY (survey_id, date)
);

-- candidate_log
CREATE TABLE IF NOT EXISTS candidate_log (
    cid SERIAL,
    survey_id INT NOT NULL,
    candidate_name TEXT NOT NULL,
    approval_rating FLOAT NOT NULL,
    FOREIGN KEY (survey_id) REFERENCES survey_info(survey_id) ON DELETE CASCADE,
    PRIMARY KEY (survey_id, candidate_name)
);

-- political_party_log
CREATE TABLE IF NOT EXISTS political_party_log (
    pid SERIAL,
    survey_id INT NOT NULL,
    political_party_name TEXT NOT NULL,
    support_rate FLOAT NOT NULL,
    FOREIGN KEY (survey_id) REFERENCES survey_info(survey_id) ON DELETE CASCADE,
    PRIMARY KEY (survey_id, political_party_name)
);

-- theme_info
CREATE TABLE IF NOT EXISTS theme_info (
    tid SERIAL,
    stock_name TEXT NOT NULL,
    candidate_name TEXT NOT NULL,
    FOREIGN KEY (stock_name) REFERENCES stock_info(kor_stock_abbr) ON DELETE CASCADE,
    FOREIGN KEY (candidate_name) REFERENCES candidate_info(candidate_name) ON DELETE CASCADE,
    PRIMARY KEY (stock_name, candidate_name)
);

-- stock_log
CREATE TABLE IF NOT EXISTS stock_log (
    sid SERIAL,  -- ✅ 단순 증가하는 값
    date DATE NOT NULL,
    stock_code TEXT NOT NULL,
    close FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    open FLOAT NOT NULL,
    volume INT NOT NULL,
    FOREIGN KEY (stock_code) REFERENCES stock_info(short_code) ON DELETE CASCADE,
    PRIMARY KEY (date, stock_code)  -- ✅ 중복을 방지할 유일한 키 설정
);

-- google_trend
CREATE TABLE IF NOT EXISTS google_trend (
    gid SERIAL,  -- ✅ 단순 증가하는 값
    date DATE NOT NULL,
    keyword TEXT NOT NULL,
    trend_score INT NOT NULL,
    FOREIGN KEY (keyword) REFERENCES candidate_info(candidate_name) ON DELETE CASCADE,
    PRIMARY KEY (date, keyword)  -- ✅ 중복을 방지할 유일한 키 설정
);

-- naver_trend
CREATE TABLE IF NOT EXISTS naver_trend (
    nid SERIAL, -- ✅ 단순 증가하는 값
    date DATE NOT NULL,
    keyword TEXT NOT NULL,
    news_count INT NOT NULL,
    blog_count INT NOT NULL,
    total_news_count INT NOT NULL,
    total_blog_count INT NOT NULL,
    FOREIGN KEY (keyword) REFERENCES candidate_info(candidate_name) ON DELETE CASCADE,
    PRIMARY KEY (date, keyword)
);
