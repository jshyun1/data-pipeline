-- 스케줄 규칙의 «반복 점검». V51 은 하루 한 번만 돌 수 있었다.
--
-- 현장 요구: 03:00 에 점검해 실패가 있으면 보내고, 5분 뒤 다시 점검해 여전히 실패면 또 보내고,
-- 이를 3회까지. 재발송(같은 알림을 다시 쏘기)이 아니라 «점검을 다시 하는» 것이라, 그 사이
-- 복구된 대상은 두 번째 점검에서 빠진다.
--
-- 간격/횟수는 새 컬럼을 만들지 않고 기존 필드를 재사용한다 - 화면의 "재발송 간격(분)"과
-- "최대 발송 횟수"가 이미 그 두 값이고, 상시 규칙에서 쓰던 의미(간격마다 최대 N회 발송)와
-- 사람이 읽는 뜻이 같다. renotify_seconds = 점검 간격, params_json.notify_max = 점검 횟수.
ALTER TABLE alert_rule
    -- 오늘 몇 번째 점검까지 돌았는지. 날짜가 바뀌면 첫 점검에서 1 로 되돌아간다.
    ADD COLUMN schedule_run_count  integer NOT NULL DEFAULT 0,
    -- 마지막 점검을 «시작»한 시각. 다음 점검 시점 계산과, 그 점검 중에 이미 발송한
    -- 알림인지 가려내는 기준(last_notified_at 비교)으로 함께 쓴다.
    ADD COLUMN schedule_last_run_at timestamptz;

COMMENT ON COLUMN alert_rule.schedule_run_count IS '오늘 돈 점검 횟수(날짜 바뀌면 1부터)';
COMMENT ON COLUMN alert_rule.schedule_last_run_at IS '마지막 점검 시작 시각';
