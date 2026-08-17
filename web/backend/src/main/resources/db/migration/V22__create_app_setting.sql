-- =============================================================================
-- V22__create_app_setting.sql  (통합 운영 대시보드 개편 / 설계서 4-2절)
-- =============================================================================
-- 고객 환경별로 달라지는 운영 파라미터(임계값·주기·보존기간)의 "재정의" 저장소.
--
-- ■ 왜 기본값을 시드로 INSERT 하지 않는가 (override-only)
--   지금까지 임계값은 전부 private static final 상수였다(ProcessHealthService의
--   3분/10분, consumer lag 1000 등). 이걸 DB 시드로 옮기면 두 가지가 깨진다.
--     1) 업그레이드로 기본값을 개선해도 이미 시드된 고객에게는 반영되지 않는다.
--     2) 고객이 행을 지우면 "기본값 없음"이 되어 판정이 멈춘다.
--   그래서 이 테이블은 "고객이 실제로 바꾼 값"만 담는다. 행이 없으면 코드의
--   SettingKey.defaultValue 가 쓰인다. "기본값으로 초기화"는 DELETE 한 줄이다.
--
-- ■ 시크릿을 넣지 않는다
--   이 테이블은 메타DB 덤프에 그대로 실려 나간다. 알림 채널 비밀번호만 예외로
--   notification_channel_config.encrypted_secret 에 암호화해 둔다(V36, 키는 .env).
--
-- ■ 값의 범위 검증은 SettingKey 카탈로그(코드)가 하고 DB 는 타입만 잡는다.
--   범위를 CHECK 제약으로 박으면 기본값 개선 때마다 마이그레이션이 필요해진다.
--
-- 시각 규약: 신규 테이블은 전부 TIMESTAMPTZ (설계서 3-3절). 기존 테이블의 TIMESTAMP 와
-- 의도적으로 다르다 - 재정의 시각을 TZ 없이 저장하면 KST/UTC 혼동으로 낙관적 락이 깨진다.
-- =============================================================================
CREATE TABLE app_setting (
    setting_key   VARCHAR(120) PRIMARY KEY,
    setting_value TEXT         NOT NULL,
    -- SettingKey 선언과 일치해야 한다. 불일치는 폴백하지 않고 부팅 시 경고로 표면화한다.
    value_type    VARCHAR(20)  NOT NULL,
    -- 낙관적 락. TIMESTAMP 비교는 TZ/정밀도 문제가 있어 쓰지 않는다.
    -- "재정의 없음"은 클라이언트가 expectedVersion=0 으로 표현한다.
    version       BIGINT       NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- 현재 값의 책임자. "지금 이 임계값을 누가 마지막에 바꿨나"를 설정 화면에서 바로 본다.
    updated_by    VARCHAR(255) NOT NULL,
    -- app_setting_alias 이관으로 값이 넘어온 경우 그 출처 키(롤백 안전, 아래 참고).
    migrated_from VARCHAR(120),
    CONSTRAINT ck_app_setting_value_type
        CHECK (value_type IN ('INT','DECIMAL','BOOLEAN','STRING','DURATION_SECONDS','BYTES','JSON'))
);

COMMENT ON TABLE app_setting IS
    '고객이 기본값에서 변경한 운영 파라미터만 저장(override-only). 행이 없으면 코드 기본값.';

-- -----------------------------------------------------------------------------
-- 설정 키 rename 시 고객이 바꿔둔 값을 잃지 않기 위한 이관표.
--
-- 등록하지 않고 rename 하면, 고객이 75%로 낮춰둔 메모리 임계가 업그레이드 순간 조용히
-- 기본값 80%로 돌아간다. 알림이 늦게 오는 원인이 되고 어디에도 에러가 남지 않는다.
--
-- SettingService 가 기동 시 1회 읽어 old_key 행이 있고 new_key 행이 없으면 값을 옮긴다.
-- 이관은 파괴적 연산이라 "이미지 태그만 되돌리는" 롤백에서 값이 증발하므로, old_key 행을
-- 지우지 않고 app_setting.migrated_from 만 찍는다(롤백 안전). 이관 사실은 애플리케이션
-- 로그(WARN) + 자가진단 "구성 점검" 항목으로 남긴다.
-- -----------------------------------------------------------------------------
CREATE TABLE app_setting_alias (
    old_key       VARCHAR(120) PRIMARY KEY,
    new_key       VARCHAR(120) NOT NULL,
    since_version VARCHAR(20)  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
