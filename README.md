## 🗳️ 장래 정치 지도자 트렌드 분석 프로젝트

#### ✅ 페르소나
<img src="https://github.com/user-attachments/assets/0f322674-12a0-48dd-920b-72f27f95ab95" width="777"/>

#### ✅ 일정 
- 프로젝트 기간 : 2025.02.21 - 2025.04.27
- 프로젝트 일정 관리 : Jira 
  
#### ✅ Data Flow
<img src="https://github.com/user-attachments/assets/9c70340b-7872-42e1-81bb-b1feb1590023" width="777"/>

#### ✅ 활용 기술
- **Language** : Python3 (Selenium, Pandas)
- **Big Data Tool** : Airflow
- **DBMS** : PostgreSQL
- **Visualization** : Tableau 
- **Cloud** : GCP
- **ETC** : Git, Docker, Jira

#### ✅ 구현 세부사항
- **네이버 Open API** : 키워드 관련 뉴스·블로그 포스팅 수의 누적값만 조회 가능하여, 매일 00:01에 DAG를 실행해 오늘 누적 수치와 어제 누적 수치의 차이를 계산해 일별 데이터를 추출. 초기 데이터는 Selenium으로 웹 스크래핑하여 수집.
- **yfinance, pytrends** : 비공식 Python 라이브러리를 사용해 매일 주가 데이터와 구글 트렌드 데이터를 수집.
- **중앙선거여론조사심의위원회** : 정당 지지율은 주 1회 제공되는 xlsx 파일로 수집. 정치인 지지율은 제공 문서가 없어 직접 결과표를 조회하여 수집. 정치인 지지율의 경우 다자대결의 결과만 수집. (양자대결, 3자대결, 진영 내 대결 제외)
- PostgreSQL에는 Incremental Update 방식으로 저장. Google Sheet에는 데이터 일관성 유지를 위해 Full Update 방식으로 저장.

#### ✅ ERD
<img src="https://github.com/user-attachments/assets/4ebb96f9-7c5d-4b4d-a629-7cd529960ad1" width="777"/>

#### ✅ Visualization
- [태블로](https://public.tableau.com/app/profile/seohui.cho/viz/2_17429160891360/2025)

#### ✅ 출처
- [중앙선거여론조사심의위원회](https://nesdc.go.kr/portal/main.do) : 여론조사 데이터 수집
- [네이버 개발자 센터](https://developers.naver.com/main/) : 네이버 뉴스 기사 수, 블로그 포스팅 수 추출
- [yfinance](https://finance.yahoo.com/) : 주가 데이터 추출
- [pytrends](https://trends.google.com/trends/) : 구글 트렌드 지수 추출
- [핀업](https://stock.finup.co.kr/) : 정치 테마주 종목 정보 수집
- [한국거래소](http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101) : 주식 종목코드 수집

#### ✅ 회고
- [블로그](https://velog.io/@toughcookie/series/%EC%9E%A5%EB%9E%98-%EC%A0%95%EC%B9%98-%EC%A7%80%EB%8F%84%EC%9E%90-%ED%8A%B8%EB%A0%8C%EB%93%9C-%EB%B6%84%EC%84%9D-%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8)

