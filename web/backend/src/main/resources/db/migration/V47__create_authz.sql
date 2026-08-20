-- =============================================================================
-- V47__create_authz.sql  (사용자/권한 설계서 §6, 2026-08-19 개정 §10)
-- =============================================================================
-- 역할·권한·메뉴·감사 체계. 계정 원장(app_user)은 이미 V7/V8/V24에 있으므로 여기서
-- 다시 만들지 않는다. 설계서 원안(§6.1)이 제안한 app_user ALTER(login_fail_count/
-- locked_until/pw_updated_at)는 V24가 이미 추가했으므로 여기서 반복하지 않는다(중복 실패 방지).
--
-- access_bits 비트마스크: 1=READ, 2=WRITE, 4=EXECUTE (nd_suite와 동일 규칙). 2단계 UI에서는
-- 0/1/7 만 생성되지만 저장은 3비트라 나중에 실행을 분리해도 스키마 변경이 없다.
-- -----------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 역할
-- ---------------------------------------------------------------------------
CREATE TABLE app_role (
    role_id    VARCHAR(40)  PRIMARY KEY,               -- ROLE_ETL_ADMIN 등
    role_nm    VARCHAR(100) NOT NULL,
    role_desc  VARCHAR(300),
    -- 마이그레이션이 심는 기본 역할은 삭제/개명을 막는다(운영 중 지워지면 전원이 권한을 잃는다).
    built_in   BOOLEAN      NOT NULL DEFAULT false,
    use_yn     VARCHAR(1)   NOT NULL DEFAULT 'Y',
    created_at TIMESTAMP    NOT NULL DEFAULT now(),
    created_by VARCHAR(100),
    updated_at TIMESTAMP    NOT NULL DEFAULT now(),
    updated_by VARCHAR(100)
);

-- ---------------------------------------------------------------------------
-- 사용자 → 역할 (N:M). 계정이 같은 DB에 있으므로 FK를 건다(고아 행 걱정 없음).
-- ---------------------------------------------------------------------------
CREATE TABLE app_user_role (
    user_id     VARCHAR(255) NOT NULL REFERENCES app_user (user_id) ON DELETE CASCADE,
    role_id     VARCHAR(40)  NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    assigned_at TIMESTAMP    NOT NULL DEFAULT now(),
    assigned_by VARCHAR(100),
    PRIMARY KEY (user_id, role_id)
);
CREATE INDEX idx_app_user_role_user ON app_user_role (user_id);

-- ---------------------------------------------------------------------------
-- 역할 → 시스템 권한. system_code: COMMON | NIFI | AIRFLOW | KAFKA | ADMIN.
-- ---------------------------------------------------------------------------
CREATE TABLE app_role_system_permission (
    role_id     VARCHAR(40) NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    system_code VARCHAR(20) NOT NULL,
    access_bits INTEGER     NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP   NOT NULL DEFAULT now(),
    updated_by  VARCHAR(100),
    PRIMARY KEY (role_id, system_code),
    CONSTRAINT ck_app_role_system_permission_bits CHECK (access_bits BETWEEN 0 AND 7)
);

-- ---------------------------------------------------------------------------
-- 메뉴 카탈로그. 화면 라우트와 1:1인 고정 목록이라 마이그레이션이 심고, 관리자는
-- "노출/숨김"만 바꾼다. required_bits = "이 메뉴를 보려면 해당 시스템에 최소 이만큼 필요".
-- ---------------------------------------------------------------------------
CREATE TABLE app_menu (
    menu_id       VARCHAR(40)  PRIMARY KEY,
    parent_id     VARCHAR(40)  REFERENCES app_menu (menu_id),
    menu_nm       VARCHAR(100) NOT NULL,
    menu_url      VARCHAR(200),                          -- 그룹 노드는 NULL
    icon          VARCHAR(50),
    system_code   VARCHAR(20)  NOT NULL,
    required_bits INTEGER      NOT NULL DEFAULT 1,
    sort_ord      INTEGER      NOT NULL DEFAULT 0,
    use_yn        VARCHAR(1)   NOT NULL DEFAULT 'Y'
);

-- 역할별 메뉴 재정의. 비워두면 시스템 권한에서 자동 계산된다(설계서 §4.5).
-- "권한은 주되 메뉴는 감추고 싶다" 같은 예외에만 행이 생긴다.
CREATE TABLE app_role_menu_override (
    role_id VARCHAR(40) NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    menu_id VARCHAR(40) NOT NULL REFERENCES app_menu (menu_id) ON DELETE CASCADE,
    visible BOOLEAN     NOT NULL,
    PRIMARY KEY (role_id, menu_id)
);

-- ---------------------------------------------------------------------------
-- 감사 로그 - "언제 누가 누구에게 무슨 권한을 줬나"는 사후에 반드시 질문이 들어온다.
-- actor_id/target_id 는 user_id(VARCHAR255)를 담을 수 있게 폭을 맞춘다(현행화 §10.1).
-- 서비스 호출은 actor_id='svc:airflow' / 'svc:portal' 처럼 접두어로 구분한다.
-- ---------------------------------------------------------------------------
CREATE TABLE permission_audit_log (
    id           BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMP    NOT NULL DEFAULT now(),
    actor_id     VARCHAR(255) NOT NULL,
    action       VARCHAR(40)  NOT NULL,   -- LOGIN_SUCCESS/LOGIN_FAIL/ACCOUNT_LOCKED/UNLOCK/
                                          -- CREATE_USER/UPDATE_USER/DISABLE_USER/ENABLE_USER/RESET_PASSWORD/
                                          -- CREATE_ROLE/DELETE_ROLE/GRANT_SYSTEM/REVOKE_SYSTEM/
                                          -- ASSIGN_ROLE/UNASSIGN_ROLE/SET_MENU/
                                          -- PROVISION_USER/SYNC_NIFI_USER/SYNC_AIRFLOW_USER/DENIED
    target_type  VARCHAR(20),             -- USER | ROLE | MENU | API
    target_id    VARCHAR(255),
    before_value VARCHAR(200),
    after_value  VARCHAR(200),
    detail       TEXT,
    client_ip    VARCHAR(45)
);
CREATE INDEX idx_permission_audit_log_time  ON permission_audit_log (occurred_at DESC);
CREATE INDEX idx_permission_audit_log_actor ON permission_audit_log (actor_id, occurred_at DESC);

-- ===========================================================================
-- 초기 데이터 (설계서 §6.2, D6=역할 2개)
-- ===========================================================================
INSERT INTO app_role (role_id, role_nm, role_desc, built_in, created_by) VALUES
  ('ROLE_ETL_ADMIN',  'ETL 관리자', 'NiFi·Airflow·CDC 전체 쓰기 및 계정/권한 관리', true, 'system'),
  ('ROLE_ETL_VIEWER', 'ETL 조회자', '전체 조회 전용',                                true, 'system');

-- 시스템 권한 (bits: 1=READ, 7=WRITE(변경+실행))
INSERT INTO app_role_system_permission (role_id, system_code, access_bits, updated_by) VALUES
  ('ROLE_ETL_ADMIN',  'COMMON',  1, 'system'),
  ('ROLE_ETL_ADMIN',  'NIFI',    7, 'system'),
  ('ROLE_ETL_ADMIN',  'AIRFLOW', 7, 'system'),
  ('ROLE_ETL_ADMIN',  'KAFKA',   7, 'system'),
  ('ROLE_ETL_ADMIN',  'ADMIN',   7, 'system'),
  ('ROLE_ETL_VIEWER', 'COMMON',  1, 'system'),
  ('ROLE_ETL_VIEWER', 'NIFI',    1, 'system'),
  ('ROLE_ETL_VIEWER', 'AIRFLOW', 1, 'system'),
  ('ROLE_ETL_VIEWER', 'KAFKA',   1, 'system'),
  ('ROLE_ETL_VIEWER', 'ADMIN',   0, 'system');

-- 메뉴 카탈로그 (현재 라우트 + 신규 관리 4메뉴). 그룹 노드는 url NULL.
-- 'ETL 생성'만 required_bits=7 (조회자에게 생성 메뉴를 보일 이유가 없음).
INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, system_code, required_bits, sort_ord) VALUES
  ('m_dashboard',       NULL,          '대시보드',        '/dashboard',        'COMMON',  1, 10),
  ('m_airflow',         NULL,          'AirFlow',         NULL,                'AIRFLOW', 1, 20),
  ('m_airflow_manage',  'm_airflow',   '생성/관리',       '/airflow/manage',   'AIRFLOW', 1, 21),
  ('m_etl',             NULL,          'ETL',             NULL,                'NIFI',    1, 30),
  ('m_etl_create',      'm_etl',       '생성',            '/etl/create',       'NIFI',    7, 31),
  ('m_etl_manage',      'm_etl',       '관리',            '/etl/manage',       'NIFI',    1, 32),
  ('m_etl_logs',        'm_etl',       '로그',            '/etl/logs',         'NIFI',    1, 33),
  ('m_cdc',             NULL,          'CDC',             NULL,                'KAFKA',   1, 40),
  ('m_cdc_pipelines',   'm_cdc',       '파이프라인',      '/cdc/pipelines',    'KAFKA',   1, 41),
  ('m_cdc_connections', 'm_cdc',       '연결정보',        '/cdc/connections',  'KAFKA',   1, 42),
  ('m_cdc_logs',        'm_cdc',       '처리 로그',       '/cdc/logs',         'KAFKA',   1, 43),
  ('m_admin',           NULL,          '설정',            NULL,                'ADMIN',   1, 90),
  ('m_admin_users',     'm_admin',     '계정 관리',       '/admin/users',      'ADMIN',   1, 91),
  ('m_admin_roles',     'm_admin',     '역할 및 권한',    '/admin/roles',      'ADMIN',   1, 92),
  ('m_admin_assign',    'm_admin',     '사용자 역할 배정','/admin/assign',     'ADMIN',   1, 93),
  ('m_admin_audit',     'm_admin',     '감사 로그',       '/admin/audit',      'ADMIN',   1, 94);

-- 부트스트랩 관리자: 특정 계정에 ROLE_ETL_ADMIN을 부여하는 건 운영 계정을 알 수 없어
-- 여기서 하지 않는다. 최초 관리자는 app_user.admin_yn='Y' 백도어(설계서 §6.4)로 들어오거나
-- P2 관리 화면/계정 이관 단계에서 지정한다.
