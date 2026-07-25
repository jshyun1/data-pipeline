-- Airflow 메타데이터 저장용 데이터베이스. 별도 컨테이너(airflow-db) 대신
-- metadata-db(Postgres 인스턴스)에 database만 하나 더 두는, Keycloak과 동일한
-- 패턴(컨테이너 하나 아끼려는 목적, 논리적으로는 pipeline_meta/keycloak과 완전히 분리).
--
-- METADATA_DB_USER를 재사용하지 않고 전용 계정(airflow_app)을 쓴다: Airflow의
-- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN은 AIRFLOW_CONN_*처럼 JSON으로 못 쓰고 URL
-- 문자열 하나뿐이라, METADATA_DB_PASSWORD에 든 URL 예약문자(!@#)가 파싱을 깨뜨린다.
-- 아래 비밀번호는 .env.example의 AIRFLOW_DB_PASSWORD 기본값과 반드시 맞춰야 한다
-- (바꾸려면 여기와 .env를 같이 수정할 것 - URL 예약문자 없는 값으로만).
--
-- 주의: 이 스크립트는 볼륨이 새로 만들어질 때 1회만 실행된다(기존 볼륨엔 수동 생성 필요).
CREATE ROLE airflow_app WITH LOGIN PASSWORD 'ChangeMe_Airflow_2026!';
CREATE DATABASE airflow OWNER airflow_app;
\connect airflow
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow_app;
