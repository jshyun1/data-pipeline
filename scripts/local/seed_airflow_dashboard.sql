INSERT INTO airflow_dag_catalog
    (dag_id, business_group, business_folder, display_name, description, sort_order)
VALUES
    ('test_hello',                  'CDC', '관세청', '관세청 연결 확인',       '로컬 Airflow 실행 확인용 DAG', 10),
    ('sample_cdc_customs_import',   'CDC', '관세청', '수입신고 수집',          '수입신고 변경 데이터 수집', 20),
    ('sample_cdc_customs_export',   'CDC', '관세청', '수출신고 수집',          '수출신고 변경 데이터 수집', 30),
    ('sample_cdc_logistics_track',  'CDC', '물류',   '배송추적 수집',           '배송 상태 변경 데이터 수집', 40),
    ('test_pipeline_api_read',      'ETL', '공통',   'Pipeline API 연결 확인',  'Airflow와 Pipeline API 연결 확인', 10),
    ('sample_etl_dz_load',          'ETL', '관세청', 'ETL_DZ',                '관세청 DZ 적재 작업', 20),
    ('sample_etl_dw_load',          'ETL', '관세청', 'ETL_DW',                '관세청 DW 적재 작업', 30),
    ('sample_etl_dm_daily',         'ETL', '매출',   'ETL_DM_DAILY',          '일별 매출 마트 생성', 40),
    ('sample_etl_logfile',          'ETL', '로그',   'ETL_LOGFILE',           '로그 파일 적재', 50),
    ('sample_etl_customer',         'ETL', '고객',   'ETL_CUSTOMER',          '고객 기준정보 적재', 60)
ON CONFLICT (dag_id) DO UPDATE SET
    business_group = EXCLUDED.business_group,
    business_folder = EXCLUDED.business_folder,
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    sort_order = EXCLUDED.sort_order,
    enabled = TRUE,
    updated_at = now();
