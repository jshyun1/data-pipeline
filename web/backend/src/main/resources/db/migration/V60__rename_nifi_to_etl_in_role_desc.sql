-- 역할 설명의 "NiFi" 를 "ETL" 로. 화면 전반에서 NiFi 라는 제품명 대신 ETL 로 부르기로 한
-- 명칭 정리(대시보드 라벨 등)와 같은 맥락이고, 여기만 남아 있었다.
--
-- V48 의 INSERT 문을 고치지 않고 UPDATE 로 잡는다 - 이미 적용된 마이그레이션의 내용을
-- 바꾸면 Flyway 체크섬 검증에서 기동이 막힌다. 신규 설치는 V48 이 넣은 뒤 이 문장이 고친다.
--
-- 운영자가 설명을 직접 손봤다면 건드리지 않도록, 기본 문구 그대로일 때만 바꾼다.
UPDATE app_role
   SET role_desc = 'ETL·Airflow·CDC 전체 쓰기 및 계정/권한 관리',
       updated_at = now()
 WHERE role_id = 'ROLE_ETL_ADMIN'
   AND role_desc = 'NiFi·Airflow·CDC 전체 쓰기 및 계정/권한 관리';
