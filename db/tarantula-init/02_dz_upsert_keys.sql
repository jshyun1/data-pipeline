-- DZ 변경적재(UPSERT)용 유니크 인덱스 (타란툴라DB: 192.168.50.12:7432 / database=postgres)
--
-- NiFi "DZ_UPSERT" 프로세스그룹의 PutDatabaseRecord(Statement Type=UPSERT)는 PostgreSQL에서
-- INSERT ... ON CONFLICT (키) DO UPDATE 로 변환된다. ON CONFLICT 는 해당 컬럼 조합에
-- 유니크 인덱스(또는 PK)가 있어야만 동작하므로, 이 스크립트를 먼저 적용해야 UPSERT가 돈다.
-- 인덱스가 없으면 런타임에 아래 오류로 실패한다:
--   ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
--
-- 이 파일은 자동 실행되지 않는다(타란툴라DB는 우리 compose가 띄우는 DB가 아님).
-- 적용:
--   PGPASSWORD=... psql -h 192.168.50.12 -p 7432 -U tarandb -d postgres -f 02_dz_upsert_keys.sql
--
-- ---------------------------------------------------------------------------
-- 사전 확인 (2026-07-31 실측): 현재 DZ 테이블에는 "완전히 동일한 행"이 반복 적재돼 있다.
--   dz_com002l  4,598행 / 서로 다른 행 418개      (11배)
--   dz_com003m 99,594행 / 서로 다른 행 9,054개    (11배)
--   dz_pop003l 1,660,590행 / 서로 다른 행 276,765개 (6배)
--   dz_com001m 43행 / 43개 (중복 없음), dz_com004m 0행
-- 중복이 남아 있으면 유니크 인덱스 생성이 실패한다. 아래 1) 중복 정리를 먼저 수행할 것.
-- 중복 정리는 되돌릴 수 없으므로 반드시 백업 후 실행한다.
-- ---------------------------------------------------------------------------

BEGIN;

-- ---------------------------------------------------------------------------
-- 1) 중복 정리 - 완전히 동일한 행을 1건만 남긴다 (ctid = 물리 행 식별자)
-- ---------------------------------------------------------------------------
DELETE FROM dz_com002l a
 USING dz_com002l b
 WHERE a.ctid > b.ctid
   AND a.* IS NOT DISTINCT FROM b.*;

DELETE FROM dz_com003m a
 USING dz_com003m b
 WHERE a.ctid > b.ctid
   AND a.* IS NOT DISTINCT FROM b.*;

-- ---------------------------------------------------------------------------
-- 2) UPSERT 충돌 키 - NiFi 파라미터 DZ_UPSERT_KEYS_* 값과 반드시 일치해야 한다
-- ---------------------------------------------------------------------------

-- COM001M: 테이블/컬럼 메타 (실측상 (tbl_id, col_id) 유일)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dz_com001m_key
    ON dz_com001m (tbl_id, col_id);

-- COM002L: 코드 목록 (중복 제거 후 (col_id, code) 유일)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dz_com002l_key
    ON dz_com002l (col_id, code);

-- COM003M: 법정동 (중복 제거 후 lawd_cd 유일)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dz_com003m_key
    ON dz_com003m (lawd_cd);

-- COM004M: 행정동 - 현재 0행이라 실데이터로 키를 검증하지 못했다.
--          원천 TB_COM004M 적재 후 아래로 유일성을 먼저 확인할 것.
--            SELECT count(*), count(distinct (admd_cd, base_yy)) FROM dz_com004m;
CREATE UNIQUE INDEX IF NOT EXISTS ux_dz_com004m_key
    ON dz_com004m (admd_cd, base_yy);

COMMIT;

-- ---------------------------------------------------------------------------
-- 3) POP003L - 인덱스를 만들지 않았다
--
-- 통계 마이크로데이터라 자연키가 없다. 중복 제거 후에도 서로 다른 행이 276,765개인데
-- (crtr_yy, dclr_yy, dclr_mm, dclr_dt) 조합은 773가지뿐이라 어떤 컬럼 조합도 키가 되지 않는다.
-- 두 가지 중 하나를 택해야 UPSERT가 성립한다.
--
--   (a) 원천 PK 사용 - Oracle TB_POP003L 에 PK/식별 컬럼이 있으면 그 컬럼을 타깃에 추가하고
--       유니크 인덱스를 만든 뒤 NiFi 파라미터 DZ_UPSERT_KEYS_POP003L 을 그 컬럼으로 바꾼다.
--
--   (b) 기간 단위 재적재 - 자연키가 없으면 행 단위 UPSERT 자체가 의미가 없다.
--       DZ_UPSERT_WHERE_POP003L 에 "WHERE CRTR_YY = '2026'" 처럼 기간 조건을 주고,
--       upsert-dz-POP003L 대신 "해당 기간 DELETE 후 INSERT"로 바꾸는 편이 정확하다.
--       (NiFi에서는 PutSQL(DELETE) -> PutDatabaseRecord(INSERT) 2단으로 구성)
--
-- 조치 전까지 DZ_UPSERT 체인을 POP003L 앞에서 끊으려면 NiFi 파라미터
-- DZ_UPSERT_START_FROM 을 COM001M 로 두고 upsert-dz-COM004M 의 success 연결을 끊거나,
-- POP003L 레인 4개 프로세서를 정지 상태로 둔다.
-- ---------------------------------------------------------------------------
