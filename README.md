### 📈 장래 정치 지도자 트렌드 분석 프로젝트
---
#### 📌 1. 프로젝트 개요
- **타겟 고객**
  - 여론조사 기관 및 의뢰자에 따라 결과가 다른 여론조사 수치를 비교분석하고 싶은 유권자
  - 여론조사 수치를 빅데이터와 비교분석하고 싶은 유권자
  - 정치 테마주와 정치인 지지도를 비교 분석하고 싶은 투자자
- **제공 가치**
  - 후보자의 여론조사 지지율과 포털의 트렌드 지표 (구글 트렌드. 네이버 뉴스, 네이버 블로그, 야후 주가)를 비교 분석
- **핵심 기능**
  - 분석 결과를 대시보드 형태로 제공
---
#### 📌 2. 일정
- **프로젝트 기간** : 2025.02.21 - 진행중
- **프로젝트 일정** : [Jira 타임라인](https://devseohui.atlassian.net/jira/software/projects/KAN/boards/1/timeline?shared=&atlOrigin=eyJpIjoiYWMxY2I1NjgyOTkzNDNlNTg5YWM2NjMxOGNlNTQ3YWUiLCJwIjoiaiJ9)
---
#### 📌 3. 활용 기술
- **Programming** : Python, SQL
- **Databases** : PostgreSQL
- **Big Data Tools** : Airflow
- **Version Control** : Git, GitHub
- **Visualisation Tools** : Tableau
- **Etc** : Docker
---
#### 📌 4. 파이프라인
- **배치 데이터 출처**
  - [중앙선거여론조사심의위원회](https://nesdc.go.kr/portal/main.do) : 장래 정치 지도자 후보자 적합도 및 정당 지지율 추출
  - [Yahoo Finance API](https://github.com/ranaroussi/yfinance) : 주식 시가 및 종가 데이터 추출
  - [Pytrends](https://github.com/GeneralMills/pytrends) : 구글 트랜드 지수 추출
  - [Naver Developers](https://developers.naver.com/docs/serviceapi/search/blog/blog.md#%EB%B8%94%EB%A1%9C%EA%B7%B8) : 네이버 뉴스 맟 불로그 언급량 추출
- **Data Flow**
  
---
#### 📌 5. ERD
![Image](https://github.com/user-attachments/assets/457cf35f-d548-4684-8cbe-b2bf03d57f1e)

---
#### 📌 6. Visualization
- [개별 정치인 세부 분석](https://public.tableau.com/app/profile/seohui.cho/viz/2_17429160891360/2025)
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
- [블로그](https://velog.io/@toughcookie/series/%EC%9E%A5%EB%9E%98-%EC%A0%95%EC%B9%98-%EC%A7%80%EB%8F%84%EC%9E%90-%ED%8A%B8%EB%A0%8C%EB%93%9C-%EB%B6%84%EC%84%9D-%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8)
- 동영상
