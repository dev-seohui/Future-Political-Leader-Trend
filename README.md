DOCKER-AIRFLOW/
├── dags/
│   ├── gsheet_dag.py
│   ├── google_dag.py
│   ├── naver_dag.py
│   └── stock_dag.py
├── db/
├── logs/
├── plugins/
│   ├── config/
│   │   └── settings.py
│   └── scripts/
│       ├── extract/
│       │   ├── google_extract.py
│       │   ├── naver_extract.py
│       │   └── stock_extract.py
│       └── load/
│           ├── google_load.py
│           ├── naver_load.py
│           └── stock_load.py
├── tableau/
│   └── gsheet_credentials.json
├── .env
├── .gitignore
└── docker-compose.yaml
