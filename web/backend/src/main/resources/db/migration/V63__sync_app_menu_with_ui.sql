-- app_menu 를 실제 화면 메뉴(AppLayout.NAV_ITEMS)와 맞춘다.
--
-- 이 표는 화면 렌더링에 쓰이지 않는다(메뉴는 프런트가 갖고 있고 시스템 권한으로 게이팅한다).
-- 다만 «역할별 메뉴 오버라이드» 기능이 이 표를 보고 목록을 만들기 때문에, 값이 낡으면
-- 관리자가 존재하지 않는 메뉴에 권한을 주게 된다. 그래서 주기적으로 맞춰 준다.
--
-- 이번에 어긋나 있던 것:
--   * 연결정보가 CDC 하위로 남아 있었다 - 실제로는 «설정» 하위이고, CDC(KAFKA)와
--     ETL(NIFI)이 함께 쓰는 공용 정보라 어느 한쪽 권한만 있어도 필요하다.
--   * AirFlow 그룹명이 «워크플로우» 로 바뀌고 하위가 스케줄링/실시간 모니터링/관리가 됐다.
--   * 알림·발송 관리 메뉴가 아예 없었다(API 는 ADMIN 이다).
--   * CDC/ETL 의 «생성» 은 쓰기(7)가 있어야 하는데 CDC 쪽이 빠져 있었다.

DELETE FROM app_role_menu_override;
DELETE FROM app_menu;

INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, icon, system_code, required_bits, sort_ord, use_yn) VALUES
  ('DASHBOARD',      NULL,        '대시보드',        '/dashboard',           'dashboard', 'COMMON',  1, 10, 'Y'),

  ('WORKFLOW',       NULL,        '워크플로우',      NULL,                   'flow',      'NIFI',    1, 20, 'Y'),
  ('WORKFLOW_DESIGN','WORKFLOW',  '스케줄링',        '/workflows/design',    NULL,        'NIFI',    1, 21, 'Y'),
  ('WORKFLOW_MON',   'WORKFLOW',  '실시간 모니터링', '/airflow/dashboard',   NULL,        'AIRFLOW', 1, 22, 'Y'),
  ('WORKFLOW_MANAGE','WORKFLOW',  '관리',            '/airflow/manage',      NULL,        'AIRFLOW', 1, 23, 'Y'),

  ('ETL',            NULL,        'ETL',             NULL,                   'etl',       'NIFI',    1, 30, 'Y'),
  ('ETL_CREATE',     'ETL',       '생성',            '/etl/create',          NULL,        'NIFI',    7, 31, 'Y'),
  ('ETL_MANAGE',     'ETL',       '관리',            '/etl/manage',          NULL,        'NIFI',    1, 32, 'Y'),
  ('ETL_LOGS',       'ETL',       '로그',            '/etl/logs',            NULL,        'NIFI',    1, 33, 'Y'),

  ('CDC',            NULL,        'CDC',             NULL,                   'cdc',       'KAFKA',   1, 40, 'Y'),
  ('CDC_CREATE',     'CDC',       '생성',            '/cdc/create',          NULL,        'KAFKA',   7, 41, 'Y'),
  ('CDC_MANAGE',     'CDC',       '관리',            '/cdc/pipelines',       NULL,        'KAFKA',   1, 42, 'Y'),
  ('CDC_LOGS',       'CDC',       '로그',            '/cdc/logs',            NULL,        'KAFKA',   1, 43, 'Y'),

  -- «설정» 그룹. 연결정보만 KAFKA 기준으로 두는데, 화면은 KAFKA/NIFI 중 하나만 있어도
  -- 보여준다(공용 정보). 이 표에는 시스템을 하나만 담을 수 있어 대표값으로 KAFKA 를 쓴다.
  ('SETTINGS',       NULL,        '설정',            NULL,                   'setting',   'ADMIN',   1, 50, 'Y'),
  ('SET_NOTIFY',     'SETTINGS',  '알림/발송 관리',  '/settings',            NULL,        'ADMIN',   1, 51, 'Y'),
  ('SET_CONN',       'SETTINGS',  '연결정보',        '/settings/connections',NULL,        'KAFKA',   1, 52, 'Y'),
  ('SET_USERS',      'SETTINGS',  '계정 관리',       '/admin/users',         NULL,        'ADMIN',   1, 53, 'Y'),
  ('SET_ROLES',      'SETTINGS',  '역할 및 권한',    '/admin/roles',         NULL,        'ADMIN',   1, 54, 'Y'),
  ('SET_AUDIT',      'SETTINGS',  '감사 로그',       '/admin/audit',         NULL,        'ADMIN',   1, 55, 'Y');
