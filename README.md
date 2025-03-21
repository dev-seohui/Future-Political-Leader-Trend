### 📈 장래 정치 지도자 트렌드 분석 프로젝트
---
#### 📌 1. 프로젝트 개요
- 타겟 고객
  - 여론조사 기관 및 의뢰자에 따라 결과가 다른 여론조사 수치를 비교분석하고 싶은 국민
  - 장래 정치 지도자 트렌드와 관련 정치 테마주를 비교분석하고 싶은 투자자 
- 제공 가치
  - 동일 기간 내 실시된 여론조사를 시각화하여 분포를 확인하 수 있게 함으로써 서로 다른 데이터 뭉치를 쉽게 비교할 수 있다.
  - 여론조사 뿐 아니라 주가 정보와 웹사이트 언급량 등을 함께 제공함으로써 여론조사와 빅데이터 2가지의 사회조사방법을 비교할 수 있다. 
- 핵심 기능
---
#### 📌 2. 일정
- 프로젝트 기간 : 2025.02.21 ~ 2025.??.??
- 프로젝트 일정 
---
#### 📌 3. 활용 기술
- Programming : Python, SQL
- Databases : PostgreSQL
- Big Data Tools : Airflow
- Version Control : Git, GitHub
- Visualisation Tools: Tableau
- Etc : Docker
---
#### 📌 4. 파이프라인
- 배치 데이터 출처
  - [중앙선거여론조사심의위원회](https://nesdc.go.kr/portal/main.do) : 장래 정치 지도자 후보자 적합도 및 정당 지지율 추출
  - [Yahoo Finance API](https://github.com/ranaroussi/yfinance) : 주식 시가 및 종가 데이터 추출
  - [Pytrends](https://github.com/GeneralMills/pytrends) : 구글 트랜드 지수 추출
  - [Naver Developers](https://developers.naver.com/docs/serviceapi/search/blog/blog.md#%EB%B8%94%EB%A1%9C%EA%B7%B8) : 네이버 뉴스 맟 불로그 언급량 추출
- ETL 파이프라인
  
---
#### 📌 5. ERD
- 
---
#### 📌 6. User Interface
- 
---
#### 📌 7. 참고 자료
- [KRX | 정보데이터시스템](http://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd) : 주식 종목 코드 수집
- [핀업](https://stock.finup.co.kr/) : 정치 테마주 수집
- [뉴시스(신문), 정치테마주 기승…23곳 무더기 상한가, 2024.12.05](https://www.donga.com/news/Economy/article/all/20241205/130572929/1) : 위와 동일
- [김혜인 기자, 정치 테마주, 전례 없는 급등락... 얼마나 올랐길래?, 뉴스톱, 2025.01.27](https://www.newstof.com/news/articleView.html?idxno=26266) : 위와 동일 
- [김경민 기자, 계엄 후 정치테마주 ‘기승’···투자주의종목 4년8개월만에 최대, 경향신문, 2025.01.26](https://www.khan.co.kr/article/202501261156001) : 위와 동일 
- [신민경 기자, "정치 판갈이한다" 이준석 사실상 대선 출마선언…테마주 급등, 한국경제, 2025.02.03](https://www.hankyung.com/article/2025020387376) : 위와 동일 
---
#### 📌 8. 회고
- 블로그
- 동영상
