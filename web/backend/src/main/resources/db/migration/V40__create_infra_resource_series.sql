-- =============================================================================
-- V40__create_infra_resource_series.sql  (설계서 4-8절, U36)
-- =============================================================================
-- 시리즈 레지스트리. "그 관측이 지금 어떤 상태인가"를 담는다(목록 자체는 코드
-- ResourceSeriesRegistry 가 원천, 기동 시 system_heartbeat 처럼 UPSERT). 기본값(임계)은 여기
-- 없다 - 라벨/등급/사용여부/실측 주기/적응형 주기만. 세 가지가 JVM 필드로는 안 된다:
--   1) 적응형 주기(볼륨 워크 300->900->1800초, 재기동마다 리셋되면 감시가 대상을 갉아먹음)
--   2) disabled_reason(스왑 없는 호스트/PSI 미지원은 고장이 아니라 구성 사실 → UNUSED 로 표시)
--   3) priority_class(상한 초과 시 무엇을 먼저 버릴지가 재기동마다 달라지면 안 됨)
-- -----------------------------------------------------------------------------
CREATE TABLE infra_resource_series (
    id                       BIGSERIAL    PRIMARY KEY,
    scope                    VARCHAR(20)  NOT NULL,   -- HOST/CONTAINER/FILESYSTEM/VOLUME/STRUCTURAL (DB 없음, R2)
    target_key               VARCHAR(200) NOT NULL,
    metric                   VARCHAR(30)  NOT NULL,   -- CPU/MEMORY/SWAP/LOAD1/MEM_PSI/IO_PSI/DISK/DISK_DIR/OOM_KILL/CPU_THROTTLE/OVERCOMMIT
    label                    VARCHAR(150) NOT NULL,
    -- P0 = 상한 제외(호스트·컨테이너·구조). P1 = 마운트·볼륨, 상한 대상.
    priority_class           VARCHAR(2)   NOT NULL DEFAULT 'P1',
    enabled                  BOOLEAN      NOT NULL DEFAULT TRUE,
    disabled_reason          VARCHAR(300),   -- 값 있으면 "고장"이 아니라 "안 쓰기로 한 것"
    quota_bytes              BIGINT,         -- 고객이 볼륨 한도 넣은 경우만. NULL 이면 증가속도만
    current_interval_seconds INTEGER,        -- 적응형 주기 현재값(부팅 시 복원)
    interval_reason          VARCHAR(200),
    first_observed_at        TIMESTAMPTZ,
    last_sampled_at          TIMESTAMPTZ,
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_infra_series_priority CHECK (priority_class IN ('P0','P1'))
);
CREATE UNIQUE INDEX uq_infra_resource_series ON infra_resource_series (scope, target_key, metric);
CREATE INDEX idx_infra_resource_series_live ON infra_resource_series (priority_class, enabled);
