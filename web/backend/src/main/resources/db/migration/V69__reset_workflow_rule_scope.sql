-- 워크플로우 알림 규칙의 «감시 범위»를 초기화한다.
--
-- 예전에는 워크플로우도 NiFi 업무 그룹에 속해서, 감시 범위를 그룹째 고를 수 있었다
-- (scope_json.groupPgIds). V68 에서 그 개념을 걷어내 그룹은 더 이상 워크플로우와 무관하다.
--
-- 남겨 두면 조용히 잘못 동작한다. groupPgIds 는 평가 시점에 «그 그룹 아래의 etl_job id»로
-- 펼쳐지는데, 워크플로우 규칙은 그 값을 «워크플로우 id»로 비교한다. 종류가 다른 id 를
-- 견주므로 사실상 아무 워크플로우도 걸리지 않는다 - 규칙은 켜져 있는데 영원히 조용한 상태다.
--
-- 그래서 전체 감시(kind=ALL)로 되돌린다. 좁혀 보고 싶으면 화면의 감시대상 트리(워크플로우
-- 계층)에서 다시 고르면 되고, 그 사이에는 «전부 감시»라 놓치는 쪽으로 기울지 않는다.
UPDATE alert_rule
   SET scope_json = '{"kind": "ALL"}'::jsonb,
       updated_at = CURRENT_TIMESTAMP
 WHERE deleted_at IS NULL
   AND rule_type_code IN ('WORKFLOW_FAILURE', 'WORKFLOW_NOT_COMPLETED')
   AND scope_json ? 'groupPgIds';
