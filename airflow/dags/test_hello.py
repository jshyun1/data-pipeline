from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_hello",
    description="스케줄러가 ./airflow/dags 바인드 마운트를 읽어서 실제로 태스크를 실행하는지 확인하는 배선 검증용 DAG",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["verification"],
) as dag:
    BashOperator(task_id="hello", bash_command="echo hello from airflow")
