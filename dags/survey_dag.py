from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract.survey_extract import SurveyExtractor
from scripts.transform.survey_transform import SurveyTransformer
from scripts.load.survey_load import PostgresLoader

default_args = {"owner": "airflow", "start_date": datetime(2024, 3, 10)}

dag = DAG("survey_pipeline", default_args=default_args, schedule_interval="@daily")

extractor = SurveyExtractor()
transformer = SurveyTransformer()
loader = PostgresLoader()

fetch_task = PythonOperator(task_id="fetch_data", python_callable=extractor.load_data, dag=dag)
transform_task = PythonOperator(task_id="transform_data", python_callable=lambda: transformer.transform(extractor.load_data()), dag=dag)
load_task = PythonOperator(task_id="load_data", python_callable=lambda: loader.insert_data(*transformer.transform(extractor.load_data())), dag=dag)

fetch_task >> transform_task >> load_task
