-- gvenzl/oracle-xe 컨테이너 최초 기동 시 SYSDBA 권한으로 실행됩니다.
-- Debezium(LogMiner)이 redo log를 읽을 수 있도록 ARCHIVELOG 모드와
-- 전체 컬럼 보충 로깅(Supplemental Logging)을 활성화합니다.
-- 주의: 이 스크립트는 인스턴스를 재시작하므로 최초 초기화 시 1회만 실행되어야 합니다.

SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- CDB 재기동 후 PDB가 자동으로 열리지 않을 수 있으므로 명시적으로 오픈
ALTER PLUGGABLE DATABASE ALL OPEN;

ALTER SYSTEM SET ENABLE_GOLDENGATE_REPLICATION = TRUE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
