-- 발송 채널 설정을 화면에서 넣는다 + 카카오 알림톡 채널 추가(2026-09-20 설계).
--
-- 이 제품이 들어갈 곳(폐쇄망·사내망·외부망)이 정해져 있지 않다. 지금은 발송 설정이 .env 에만 있어서
-- 현장마다 파일을 고치고 재기동해야 한다. 값을 DB 에 두면 화면에서 넣고 바로 쓸 수 있다.
-- config_json·encrypted_secret 컬럼은 V36 에서 이미 만들어 두고 아무도 쓰지 않던 자리다.

-- 화면 표시용 비밀값 끝자리(예: ****a1b2). GPT 제공자(llm_provider_config.api_key_hint)와 같은 방식이다.
ALTER TABLE notification_channel_config ADD COLUMN IF NOT EXISTS secret_hint VARCHAR(10);

COMMENT ON COLUMN notification_channel_config.config_json IS
    '채널 설정(비밀 아닌 값). EMAIL: host/port/username/from/auth/starttls, SMS·KAKAO: url/headers/contentType/bodyTemplate/sender 등';
COMMENT ON COLUMN notification_channel_config.encrypted_secret IS
    '채널 비밀값 1개(메일 비밀번호 · 게이트웨이 토큰 · 카카오 API 키). PasswordCryptoService 로 암호화';

-- 카카오 알림톡. 대행사는 현장마다 다르므로 코드가 아니라 설정(config_json)으로 규격을 흡수한다.
-- 처음엔 꺼 둔다 - 설정을 넣고 테스트 발송이 성공한 뒤 화면에서 켠다.
INSERT INTO notification_channel_config (channel_type, enabled, rate_critical_per_min, rate_other_per_min, hard_limit_per_hour)
VALUES ('KAKAO', false, 5, 5, 50)
ON CONFLICT (channel_type) DO NOTHING;
