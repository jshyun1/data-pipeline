# 보관된 DAG 파일

여기 있는 파일은 **Airflow가 읽지 않는다**(dags 폴더 밖이다). 지우지 않고 남겨두는 이유는
되돌릴 일이 생기면 파일 하나를 `airflow/dags/`로 옮기는 것으로 끝나기 때문이다.

## nifi_pipelines_dynamic.py (2026-08-30 보관)

NiFi root 직하 프로세스 그룹마다 DAG를 하나씩 만들던 옛 ETL 팩토리다.
`etl_workflows_dynamic.py`(워크플로우 캔버스가 게시한 spec으로 DAG를 만든다)로 대체됐다.

보관 시점의 상태:

- DW, DZ — 워크플로우로 옮겨졌다(`etl_wf_dw`, `etl_wf_dz`). 공존 필터가 이미 건너뛰고 있었다.
- Template — DAG가 필요 없다고 정리됐다. 템플릿은 실행 대상이 아니라 복제 원본이라
  잡 미러도 Template 하위를 등록하지 않는다.

같이 정리한 것: 이 파일만 쓰던 Airflow Variable 두 개
(`nifi_process_groups_cache`, `nifi_root_connections_cache`).

되돌리려면 이 파일을 `airflow/dags/`로 옮기면 된다. 캐시 Variable은 없으면 NiFi에서
다시 만든다. 다만 DW/DZ는 공존 필터가 계속 건너뛰므로, 옛 DAG로 완전히 되돌리려면
워크플로우 게시부터 내려야 한다.
