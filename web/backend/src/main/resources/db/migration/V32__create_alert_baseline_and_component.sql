-- =============================================================================
-- V32__create_alert_baseline_and_component.sql  (설계서 4-6절, 판매후 U28/U20 사용)
-- =============================================================================
-- 베이스라인. 고정 임계는 모르는 고객 환경에서 오탐·미탐을 동시에 낸다. 평균이 아니라 중앙값+MAD
-- (적재는 대량 배치로 분포가 극단적이라 평균이 무의미). 버킷 = 요일그룹(평일/주말) x 시(0~23) = 48개.
-- 학습 소스는 원본이 아니라 pipeline_load_rollup(HOUR).
CREATE TABLE alert_baseline (
    target_key     VARCHAR(200)     NOT NULL,
    metric_key     VARCHAR(60)      NOT NULL,
    bucket_key     VARCHAR(20)      NOT NULL,   -- 'WD-14' / 'WE-03'
    sample_count   INTEGER          NOT NULL,
    median_per_min DOUBLE PRECISION NOT NULL,
    p10_per_min    DOUBLE PRECISION NOT NULL,
    mad_per_min    DOUBLE PRECISION NOT NULL,
    trained_from   DATE             NOT NULL,
    trained_to     DATE             NOT NULL,
    -- 학습 제외 구간(알림 FIRING 중) 비율. 0.5 초과면 신뢰 안 함(장애 중 0건을 평시로 학습 방지).
    excluded_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    trained_at     TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (target_key, metric_key, bucket_key)
);

-- 연쇄 억제용 의존 그래프. 억제는 삭제가 아니다 - 하위도 인스턴스로 남고 화면에서 접힌 채 카운트.
CREATE TABLE alert_component (
    code       VARCHAR(60)  PRIMARY KEY,
    kind       VARCHAR(20)  NOT NULL,
    label      VARCHAR(150) NOT NULL,
    depth      SMALLINT     NOT NULL DEFAULT 0,   -- max(부모 depth)+1, 최대 4
    managed    BOOLEAN      NOT NULL DEFAULT TRUE,
    deep_link  VARCHAR(300),
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE TABLE alert_component_edge (
    child_code  VARCHAR(60) NOT NULL REFERENCES alert_component (code) ON DELETE CASCADE,
    parent_code VARCHAR(60) NOT NULL REFERENCES alert_component (code) ON DELETE CASCADE,
    PRIMARY KEY (child_code, parent_code),
    CONSTRAINT ck_component_edge_self CHECK (child_code <> parent_code)
);
-- 순환 검사는 서버가 간선 저장 시 DFS 로 수행(순환이면 400). 런타임 ancestors() 에도 방문집합 +
-- 최대 깊이 8 하드 상한(데이터 오염 시 평가 스레드 무한루프로 엔진 전체 정지 방지).
