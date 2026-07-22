-- Keycloak 메타데이터 저장용 데이터베이스. 컨테이너를 하나 더 늘리지 않으려고
-- metadata-db(Postgres 인스턴스)에 별도 database로 둔다. pipeline_meta(파이프라인
-- 웹서비스용)와는 논리적으로 완전히 분리된다.
-- 주의: 이 스크립트는 볼륨이 새로 만들어질 때 1회만 실행된다(기존 볼륨엔 수동 생성 필요).
CREATE DATABASE keycloak;
