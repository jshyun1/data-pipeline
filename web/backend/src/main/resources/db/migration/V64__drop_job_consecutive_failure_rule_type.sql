-- 알림 규칙 유형 «연속 실패»(JOB_CONSECUTIVE_FAILURE) 제거.
--
-- «ETL Job 실패»(JOB_FAILURE)와 판정이 겹쳐 어느 쪽이 울린 것인지 알기 어려웠다. 실패 감지는
-- JOB_FAILURE 하나로 모은다.
--
-- ⚠️ Airflow 이상 감지 설정의 consecutive_failure_threshold 는 다른 기능이라 건드리지 않는다
--    (airflow_dag_catalog. 그쪽은 DAG 단위 이상 감지이고 알림 규칙과 별개다).
--
-- alert_rule 을 참조하는 테이블이 5개라 자식부터 지운다.
DELETE FROM alert_rule_eval_log
 WHERE rule_id IN (SELECT id FROM alert_rule WHERE rule_type_code = 'JOB_CONSECUTIVE_FAILURE');
DELETE FROM notification_recipient_rule
 WHERE rule_id IN (SELECT id FROM alert_rule WHERE rule_type_code = 'JOB_CONSECUTIVE_FAILURE');
DELETE FROM alert_mute
 WHERE rule_id IN (SELECT id FROM alert_rule WHERE rule_type_code = 'JOB_CONSECUTIVE_FAILURE');
DELETE FROM alert_instance
 WHERE rule_id IN (SELECT id FROM alert_rule WHERE rule_type_code = 'JOB_CONSECUTIVE_FAILURE');
DELETE FROM alert_rule            WHERE rule_type_code = 'JOB_CONSECUTIVE_FAILURE';
DELETE FROM alert_rule_type       WHERE code           = 'JOB_CONSECUTIVE_FAILURE';
