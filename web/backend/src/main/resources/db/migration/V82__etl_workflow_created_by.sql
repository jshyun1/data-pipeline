-- 워크플로우 등록자(2026-10-13). 이전에는 칸이 없어 AI 가 «누가 만들었는지»를 말할 수 없었다 - 게시자만 있었다.
-- 이미 있는 워크플로우는 비워 둔다(되찾을 기록이 없고, 채우지 않기로 함). 새로 만드는 워크플로우부터 로그인 사용자 ID 가 들어간다.
ALTER TABLE etl_workflow ADD COLUMN IF NOT EXISTS created_by VARCHAR(100);
