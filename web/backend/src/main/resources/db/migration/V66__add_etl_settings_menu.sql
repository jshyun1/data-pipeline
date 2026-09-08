-- ETL 하위에 NiFi 관리 설정 진입점을 추가한다.
-- 이미 적용된 V63/V65 는 수정하지 않고, 운영 반영용 증분으로 둔다.
INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, icon, system_code, required_bits, sort_ord, use_yn)
VALUES ('ETL_SETTINGS', 'ETL', '관리 설정', '/etl/settings', NULL, 'NIFI', 1, 33, 'Y')
ON CONFLICT (menu_id) DO UPDATE
SET parent_id = EXCLUDED.parent_id,
    menu_nm = EXCLUDED.menu_nm,
    menu_url = EXCLUDED.menu_url,
    icon = EXCLUDED.icon,
    system_code = EXCLUDED.system_code,
    required_bits = EXCLUDED.required_bits,
    sort_ord = EXCLUDED.sort_ord,
    use_yn = EXCLUDED.use_yn;

UPDATE app_menu
   SET sort_ord = 34
 WHERE menu_id = 'ETL_LOGS';
