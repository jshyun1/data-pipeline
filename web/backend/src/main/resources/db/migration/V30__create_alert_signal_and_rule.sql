-- =============================================================================
-- V30__create_alert_signal_and_rule.sql  (설계서 4-6절, U8/U9)
-- =============================================================================
-- 신호 표본 - 평가기가 읽는 유일한 입력. 평가기는 외부 시스템을 절대 호출하지 않는다(외부 호출은
-- AlertSignalCollector 한 곳에만, 거기에만 타임아웃). NiFi 가 무한 블록돼도 평가기는 계속 돌고
-- "신호가 늙었다"는 사실 자체가 알림이 된다. 시리즈 폭증(파이프라인 50개 ≈ 50만행/일) 때문에
-- 보존은 7일, 정리는 시간 예산 기반(4-3절).
-- -----------------------------------------------------------------------------
CREATE TABLE alert_signal_sample (
    id           BIGSERIAL    PRIMARY KEY,
    signal_key   VARCHAR(60)  NOT NULL,
    -- "PIPELINE:57" / "JOB:11" / "HOST:local" / "MOUNT:/..." / "SERVICE:NIFI" / "COLLECTOR:nifi-counter"
    dimension    VARCHAR(200) NOT NULL DEFAULT '-',
    value_num    DOUBLE PRECISION,
    value_text   VARCHAR(60),   -- enum 신호를 숫자로 억지 변환하지 않는다
    -- 값이 실제로 관측된 시각. collected_at(우리가 읽은 시각)과 구분해야 "수집은 돌았는데 원본이
    -- 늙었다"를 판정할 수 있다.
    observed_at  TIMESTAMPTZ  NOT NULL,
    collected_at TIMESTAMPTZ  NOT NULL
);
CREATE INDEX idx_alert_signal_lookup ON alert_signal_sample (signal_key, dimension, observed_at DESC);
CREATE INDEX idx_alert_signal_purge  ON alert_signal_sample (observed_at);
INSERT INTO retention_policy (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES ('alert_signal_sample', 'observed_at', 7, 3, 'DELETE_BATCH', 'system')
ON CONFLICT (table_name) DO NOTHING;

-- 규칙 타입 카탈로그. 조건식을 문자열 DSL 로 저장하지 않는다 - 타입 코드 = Java 전략 클래스이고
-- 고객이 만지는 것은 params_json 뿐이다. 부팅 시 BuiltInRuleRegistry 가 upsert 한다(코드가 원천).
CREATE TABLE alert_rule_type (
    code              VARCHAR(60)  PRIMARY KEY,
    label             VARCHAR(150) NOT NULL,
    category          VARCHAR(30)  NOT NULL,   -- JOB/SERVICE/HOST/DATA/ENGINE
    kpi_axis          VARCHAR(20)  NOT NULL,   -- CURRENT(진행형) / DAILY(누적형)
    mandatory         BOOLEAN      NOT NULL DEFAULT FALSE,
    default_severity  VARCHAR(20)  NOT NULL,
    min_severity      VARCHAR(20)  NOT NULL,
    suppressible      BOOLEAN      NOT NULL DEFAULT TRUE,
    global_scope      BOOLEAN      NOT NULL DEFAULT FALSE,
    -- 소비 신호. required 하나라도 stale 이면 UNKNOWN 이지만, optional 은 없어도 판정한다
    -- (재기동 직후 15분 창 신호가 MISSING 이라 규칙이 통째로 눈머는 것을 막는다).
    signal_keys_req   VARCHAR(500) NOT NULL,
    signal_keys_opt   VARCHAR(500),
    eval_priority     SMALLINT     NOT NULL DEFAULT 100,
    description       TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE alert_rule (
    id                 BIGSERIAL    PRIMARY KEY,
    rule_type_code     VARCHAR(60)  NOT NULL REFERENCES alert_rule_type (code),
    builtin_key        VARCHAR(60),   -- 내장 규칙 재정의면 키. NULL 이면 사용자 정의.
    name               VARCHAR(150) NOT NULL,
    enabled            BOOLEAN      NOT NULL DEFAULT TRUE,
    severity           VARCHAR(20)  NOT NULL,
    scope_json         JSONB        NOT NULL DEFAULT '{"kind":"ALL"}'::jsonb,
    params_json        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    for_seconds        INTEGER      NOT NULL DEFAULT 120,
    clear_seconds      INTEGER      NOT NULL DEFAULT 300,   -- 해소는 발생보다 길게(성급한 복구 방지)
    renotify_seconds   INTEGER      NOT NULL DEFAULT 1800,
    escalate_seconds   INTEGER      NOT NULL DEFAULT 3600,
    mandatory          BOOLEAN      NOT NULL DEFAULT FALSE,
    last_evaluated_at  TIMESTAMPTZ,
    last_eval_ms       INTEGER,
    last_eval_error    TEXT,          -- 파라미터/타임아웃 오류(REQUIRES_NEW 로 기록)
    created_by         VARCHAR(255),
    updated_by         VARCHAR(255),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at         TIMESTAMPTZ
);
CREATE UNIQUE INDEX uq_alert_rule_name    ON alert_rule (name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX uq_alert_rule_builtin ON alert_rule (builtin_key) WHERE builtin_key IS NOT NULL AND deleted_at IS NULL;
-- 우선순위(eval_priority)는 alert_rule_type 에 있으므로 평가 쿼리에서 타입 조인으로 정렬한다.
-- 이 인덱스는 "살아있는 활성 규칙" 필터용.
CREATE INDEX idx_alert_rule_live ON alert_rule (enabled) WHERE deleted_at IS NULL;
