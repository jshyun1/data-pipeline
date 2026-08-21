from datetime import datetime

import requests

import _pipeline_svc_auth  # noqa: F401  # import 만으로 pipeline-api 서비스 토큰 자동주입(P4 §7.6)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Kafka 웹 API에 Airflow가 네트워크로 닿는지 확인하는 읽기 전용 스파이크 검증.
# 실제 start/stop/restart를 호출하는 운영 DAG는 이번 범위 밖 (계획 파일 9차 증분 참고).
PIPELINE_API_BASE_URL = "http://pipeline-api:8081"


def call_pipeline_api():
    response = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines", timeout=10)
    response.raise_for_status()
    body = response.json()
    print(f"pipeline-api 응답: {len(body.get('data', []))}개 파이프라인 조회됨")


with DAG(
    dag_id="test_pipeline_api_read",
    description="Airflow -> pipeline-api(8081) 네트워크 연결 및 API 응답 확인용 스파이크 검증 DAG (읽기 전용)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["verification"],
) as dag:
    PythonOperator(task_id="read_pipelines", python_callable=call_pipeline_api)
