-- 규칙별 수신자는 «켬/끔»만 남긴다.
--
-- V51 에서 수신자마다 대상(Job)까지 좁힐 수 있게 scope_json 을 뒀는데, 수신자 선택 창에서
-- Job 을 또 고르는 구성이 실제로 써 보니 너무 복잡했다. 감시 대상을 정하는 곳은 규칙의
-- «감시 범위»(alert_rule.scope_json) 하나로 충분하고, 수신자 화면은 누가 받는지만 정한다.
-- V51 이 오늘 들어간 것이라 이 컬럼을 실제로 쓴 설정은 없다.
ALTER TABLE notification_recipient_rule DROP COLUMN scope_json;
