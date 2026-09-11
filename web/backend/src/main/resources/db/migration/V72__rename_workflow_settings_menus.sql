-- 상단 메뉴 이름을 화면(AppLayout·WorkflowPageHeader)과 다시 맞춘다. 2026-09-10.
--
-- 워크플로우 하위를 ETL·CDC 와 같은 «생성 / 관리 / 로그» 꼴로 바꾸면서 이름이 달라졌다.
--   스케줄링 → 관리, 실시간 모니터링 → 로그, 관리 → 상세 관리
-- 설정 하위는 «관리»·«로그» 접미사를 뗐다(상위가 이미 «설정»이다).
--   알림/발송 관리 → 알림/발송, 계정 관리 → 계정, 감사 로그 → 감사
--
-- 이 표는 «역할 및 권한» 화면이 메뉴 이름을 보여줄 때 쓴다. 노출 자체는 system_code·required_bits 로
-- 정해지고 app_role_menu_override 는 예외만 담으므로, 새 메뉴에 역할별 시드는 필요 없다
-- (ETL_CREATE 도 override 행이 없다).

UPDATE app_menu SET menu_nm = '관리' WHERE menu_id = 'WORKFLOW_DESIGN';
UPDATE app_menu SET menu_nm = '로그' WHERE menu_id = 'WORKFLOW_MON';
UPDATE app_menu SET menu_nm = '상세 관리' WHERE menu_id = 'WORKFLOW_MANAGE';
UPDATE app_menu SET menu_nm = '알림/발송' WHERE menu_id = 'SET_NOTIFY';
UPDATE app_menu SET menu_nm = '계정' WHERE menu_id = 'SET_USERS';
UPDATE app_menu SET menu_nm = '감사' WHERE menu_id = 'SET_AUDIT';

-- 워크플로우 «생성». ETL_CREATE(required_bits 7, 상위 바로 다음 순번)와 같은 꼴로 둔다.
-- 기존 자식이 21·22·23 이라 20 으로 맨 앞에 선다.
INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, icon, system_code, required_bits, sort_ord, use_yn)
VALUES ('WORKFLOW_CREATE', 'WORKFLOW', '생성', '/workflows/create', NULL, 'NIFI', 7, 20, 'Y')
ON CONFLICT (menu_id) DO UPDATE
SET parent_id = EXCLUDED.parent_id,
    menu_nm = EXCLUDED.menu_nm,
    menu_url = EXCLUDED.menu_url,
    system_code = EXCLUDED.system_code,
    required_bits = EXCLUDED.required_bits,
    sort_ord = EXCLUDED.sort_ord,
    use_yn = EXCLUDED.use_yn;
