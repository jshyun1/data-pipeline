-- 포털 인증이 Keycloak(OIDC)으로 이관되면서 app_user는 "프로필/권한 저장소"로 역할이 바뀐다.
-- - user_pw: 로컬 비밀번호를 더 이상 저장하지 않음(자격증명은 Keycloak이 보유) → nullable
-- - hq_cd/position_cd: 첫 로그인 자동 프로비저닝 시 Keycloak이 제공하지 않을 수 있음
--   (관리자가 나중에 채움) → nullable
ALTER TABLE app_user ALTER COLUMN user_pw DROP NOT NULL;
ALTER TABLE app_user ALTER COLUMN hq_cd DROP NOT NULL;
ALTER TABLE app_user ALTER COLUMN position_cd DROP NOT NULL;
