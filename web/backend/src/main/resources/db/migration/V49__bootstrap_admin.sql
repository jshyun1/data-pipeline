-- =============================================================================
-- V49__bootstrap_admin.sql  (사용자/권한 설계서 §6.4 부트스트랩 관리자)
-- =============================================================================
-- 최초 관리자 부트스트랩. 로그인 계정 'admin' 이 있으면 관리자로 승격하고 ROLE_ETL_ADMIN 을
-- 부여한다(둘 다 "있을 때만" 이라 계정이 없는 환경에서는 아무 것도 하지 않는다 - 안전).
--
-- 상용 단독 배포에서 관리자 아이디가 'admin' 이 아니면, 아래와 같은 한 줄로 승격하면 된다:
--   UPDATE app_user SET admin_yn='Y' WHERE user_id='<아이디>';
-- admin_yn='Y' 는 역할과 무관하게 전 시스템 권한을 주는 비상 백도어이기도 하다(§6.4).
-- -----------------------------------------------------------------------------

UPDATE app_user SET admin_yn = 'Y' WHERE user_id = 'admin';

INSERT INTO app_user_role (user_id, role_id, assigned_by)
SELECT 'admin', 'ROLE_ETL_ADMIN', 'system'
WHERE EXISTS (SELECT 1 FROM app_user WHERE user_id = 'admin')
  AND EXISTS (SELECT 1 FROM app_role WHERE role_id = 'ROLE_ETL_ADMIN')
  AND NOT EXISTS (SELECT 1 FROM app_user_role WHERE user_id = 'admin' AND role_id = 'ROLE_ETL_ADMIN');
