-- =============================================================================
-- V24__local_auth.sql  (통합 운영 대시보드 개편 / 설계서 4-4절)
-- =============================================================================
-- 로컬 인증 (판매 차단 요소 해소).
--   실측: AccountDataSourceConfig 의 account-db.host 기본값이 없어 ST_USER(MySQL) 가
--         없는 고객사는 로그인은커녕 부팅조차 안 된다. authz.provider=LOCAL 은 인가만
--         바꿀 뿐 인증을 바꾸지 않으므로 별도 해소가 필요하다.
--   app_user.user_pw 컬럼은 있으나 로그인 검증에 쓰이지 않았다(Keycloak 시대 잔재).
--
-- 아래 4컬럼은 bcrypt 로그인 검증과 실패 잠금의 저장소다. 계정 종류(account_kind)와
-- 공유 계정 정책은 이 문서에서 정의하지 않는다(설계서 R1 - 권한 문서 소관).
-- 하위호환 규칙 2: 기존 행이 있는 app_user 에 NOT NULL 을 추가하므로 DEFAULT 를 반드시 준다.
-- -----------------------------------------------------------------------------
ALTER TABLE app_user ADD COLUMN pw_updated_at    TIMESTAMPTZ;
ALTER TABLE app_user ADD COLUMN pw_must_change   BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE app_user ADD COLUMN login_fail_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE app_user ADD COLUMN locked_until     TIMESTAMPTZ;

COMMENT ON COLUMN app_user.login_fail_count IS
    '연속 로그인 실패 횟수. 임계·잠금시간은 SettingKey(auth.login.*)로 조정한다(원칙 D).';
