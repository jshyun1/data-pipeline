# -*- coding: utf-8 -*-
"""
K-BDS 온톨로지 9종 변경 감지 및 Staging 기반 초고속 SQL Diff 증분 적재 모듈
"""

import sys
import time
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path

current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None
    execute_values = None



from config import get_db_connection, ONTOLOGIES, logger
from download_ontology import check_remote_header, download_obo_file
from parse_ontology import parse_obo_file
from load_initial import resolve_relationship_foreign_keys_pass2

def sync_single_ontology_incremental(conn, ontology_code: str) -> Dict[str, Any]:
    """
    단일 온톨로지의 변경 여부를 3단계로 검증하고, 변경 시 Staging SQL Diff를 통해 증분 적재합니다.
    """
    onto_info = ONTOLOGIES[ontology_code]
    url = onto_info["url"]
    started_at = datetime.now(timezone.utc)
    start_time = time.time()

    # 0. 기존 DB 메타 정보 조회
    with conn.cursor() as cur:
        cur.execute("""
            SELECT etag, last_modified, file_md5, version
            FROM tb_ontology_meta1
            WHERE ontology_code = %s
        """, (ontology_code,))
        row = cur.fetchone()
        stored_etag, stored_last_mod, stored_md5, stored_ver = row if row else (None, None, None, None)

    # [1단계] HTTP HEAD 조건부 요청 검증
    head_res = check_remote_header(url, stored_etag, stored_last_mod)
    if head_res["status_code"] == 304 or (not head_res["is_modified"] and stored_etag):
        logger.info(f"[{ontology_code}] [1단계 통과] HTTP 304 Not Modified -> 변경 없음 (NO_CHANGE)")
        return {
            "status": "NO_CHANGE",
            "inserted": 0, "updated": 0, "obsoleted": 0, "synonyms": 0,
            "duration": round(time.time() - start_time, 2),
            "started_at": started_at, "finished_at": datetime.now(timezone.utc),
            "error": None
        }

    # [2단계] OBO 스트리밍 다운로드 및 MD5 체크섬 검증
    file_path, file_md5, meta_info = download_obo_file(ontology_code)
    if not file_path or not file_md5:
        raise RuntimeError(f"OBO 다운로드 실패: {ontology_code}")

    if stored_md5 and stored_md5 == file_md5:
        logger.info(f"[{ontology_code}] [2단계 통과] 파일 MD5 체크섬 일치 ({file_md5}) -> 실제 내용 변경 없음 (NO_CHANGE)")
        return {
            "status": "NO_CHANGE",
            "inserted": 0, "updated": 0, "obsoleted": 0, "synonyms": 0,
            "duration": round(time.time() - start_time, 2),
            "started_at": started_at, "finished_at": datetime.now(timezone.utc),
            "error": None
        }

    # [3단계] 파싱 및 Staging 임시 테이블 기반 SQL Diff 집합 연산
    logger.info(f"[{ontology_code}] [3단계 진입] 원천 파일 변경 감지 -> 파싱 및 Staging Diff 연산 시작...")
    parsed_data = parse_obo_file(file_path, ontology_code)
    terms = parsed_data["terms"]
    synonyms = parsed_data["synonyms"]
    xrefs = parsed_data["xrefs"]
    relationships = parsed_data["relationships"]
    new_version = parsed_data.get("version") or stored_ver

    inserted_count = 0
    updated_count = 0
    obsoleted_count = 0

    with conn.cursor() as cur:
        # 3-1. 임시 Staging 테이블 생성 (트랜잭션 커밋 시 자동 제거)
        cur.execute("""
            CREATE TEMP TABLE staging_ontology_term (
                ontology_code VARCHAR(20),
                standard_id VARCHAR(100),
                preferred_term TEXT,
                definition TEXT,
                iri TEXT,
                term_type VARCHAR(50),
                is_obsolete CHAR(1),
                replaced_by VARCHAR(100)
            ) ON COMMIT DROP;
        """)

        # 3-2. 새 데이터를 Staging 테이블에 대용량 적재
        staging_records = [
            (
                t["ontology_code"], t["standard_id"], t["preferred_term"],
                t["definition"], t["iri"], t["term_type"], t["is_obsolete"], t["replaced_by"]
            )
            for t in terms
        ]
        execute_values(cur, """
            INSERT INTO staging_ontology_term (
                ontology_code, standard_id, preferred_term, definition, iri, term_type, is_obsolete, replaced_by
            ) VALUES %s
        """, staging_records, page_size=5000)

        # 3-3. SQL 기반 Diff 분석: 신규(INSERT) 반영
        cur.execute("""
            INSERT INTO tb_ontology_term1 (
                ontology_code, standard_id, preferred_term, definition, iri, term_type, is_obsolete, replaced_by
            )
            SELECT s.ontology_code, s.standard_id, s.preferred_term, s.definition, s.iri, s.term_type, s.is_obsolete, s.replaced_by
            FROM staging_ontology_term s
            LEFT JOIN tb_ontology_term1 t 
                   ON s.ontology_code = t.ontology_code AND s.standard_id = t.standard_id
            WHERE t.standard_id IS NULL;
        """)
        inserted_count = cur.rowcount

        # 3-4. SQL 기반 Diff 분석: 수정(UPDATE) 반영 (내용 변경 감지)
        cur.execute("""
            UPDATE tb_ontology_term1 t
            SET preferred_term = s.preferred_term,
                definition = s.definition,
                iri = s.iri,
                term_type = s.term_type,
                is_obsolete = s.is_obsolete,
                replaced_by = s.replaced_by,
                updated_at = CURRENT_TIMESTAMP
            FROM staging_ontology_term s
            WHERE t.ontology_code = s.ontology_code 
              AND t.standard_id = s.standard_id
              AND md5(COALESCE(t.preferred_term, '') || COALESCE(t.definition, '') || t.is_obsolete || COALESCE(t.replaced_by, '')) !=
                  md5(COALESCE(s.preferred_term, '') || COALESCE(s.definition, '') || s.is_obsolete || COALESCE(s.replaced_by, ''));
        """)
        updated_count = cur.rowcount

        # 3-5. 폐기 상태 갱신 카운트
        cur.execute("""
            SELECT COUNT(*) FROM staging_ontology_term s
            JOIN tb_ontology_term1 t ON s.ontology_code = t.ontology_code AND s.standard_id = t.standard_id
            WHERE s.is_obsolete = 'Y' AND t.is_obsolete = 'N';
        """)
        obsoleted_count = cur.fetchone()[0]

        logger.info(f"[{ontology_code}] Diff 연산 결과 -> 신규: {inserted_count:,}건, 수정: {updated_count:,}건 (폐기갱신: {obsoleted_count:,}건)")

        # 3-6. 변경 사항이 있을 때 하위 테이블(Synonym, Xref, Rel) 최신화
        if inserted_count > 0 or updated_count > 0:
            # 동의어 최신화 (해당 온톨로지 삭제 후 재생성)
            cur.execute("DELETE FROM tb_ontology_synonym1 WHERE ontology_code = %s", (ontology_code,))
            
            # term_pk 매핑 맵
            cur.execute("SELECT standard_id, term_pk FROM tb_ontology_term1 WHERE ontology_code = %s", (ontology_code,))
            id_to_pk = dict(cur.fetchall())

            syn_records = [
                (id_to_pk[s["standard_id"]], s["ontology_code"], s["synonym_term"], s["synonym_type"])
                for s in synonyms if s["standard_id"] in id_to_pk
            ]
            if syn_records:
                execute_values(cur, "INSERT INTO tb_ontology_synonym1 (term_pk, ontology_code, synonym_term, synonym_type) VALUES %s", syn_records, page_size=5000)

            # 교차참조 최신화
            cur.execute("DELETE FROM tb_ontology_xref1 WHERE source_ontology_code = %s", (ontology_code,))
            xref_records = [
                (id_to_pk[x["standard_id"]], x["source_ontology_code"], x["target_db_code"], x["target_accession"], x["description"])
                for x in xrefs if x["standard_id"] in id_to_pk
            ]
            if xref_records:
                execute_values(cur, "INSERT INTO tb_ontology_xref1 (term_pk, source_ontology_code, target_db_code, target_accession, description) VALUES %s", xref_records, page_size=5000)

            # 관계 최신화
            cur.execute("""
                DELETE FROM tb_ontology_relationship1 
                WHERE subject_term_pk IN (SELECT term_pk FROM tb_ontology_term1 WHERE ontology_code = %s)
            """, (ontology_code,))
            rel_records = [
                (id_to_pk[r["subject_standard_id"]], r["predicate"], r["object_standard_id"])
                for r in relationships if r["subject_standard_id"] in id_to_pk
            ]
            if rel_records:
                execute_values(cur, "INSERT INTO tb_ontology_relationship1 (subject_term_pk, predicate, object_standard_id) VALUES %s", rel_records, page_size=5000)

        # 3-7. tb_ontology_meta1 갱신
        cur.execute("""
            UPDATE tb_ontology_meta1
            SET version = %s,
                etag = %s,
                last_modified = %s,
                file_md5 = %s,
                term_count = (SELECT COUNT(*) FROM tb_ontology_term1 WHERE ontology_code = %s AND is_obsolete = 'N'),
                synonym_count = (SELECT COUNT(*) FROM tb_ontology_synonym1 WHERE ontology_code = %s),
                last_synced_at = NOW(),
                updated_at = NOW()
            WHERE ontology_code = %s;
        """, (
            new_version, meta_info.get("etag"), meta_info.get("last_modified"), file_md5,
            ontology_code, ontology_code, ontology_code
        ))

    duration = round(time.time() - start_time, 2)
    logger.info(f"[{ontology_code}] 증분 적재 완료! (소요시간: {duration}초)")

    return {
        "status": "SUCCESS",
        "inserted": inserted_count,
        "updated": updated_count,
        "obsoleted": obsoleted_count,
        "synonyms": len(synonyms),
        "duration": duration,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc),
        "error": None
    }

def run_incremental_sync(ontology_codes: Optional[List[str]] = None) -> bool:
    """9개 온톨로지 전체 증분 적재 동기화 실행기"""
    targets = ontology_codes or list(ONTOLOGIES.keys())
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("  K-BDS 국제 온톨로지(9종) 자동 변경 감지 및 증분 동기화 시작")
    logger.info("=" * 70)

    conn = None
    all_success = True
    any_updated = False

    try:
        conn = get_db_connection()

        for code in targets:
            try:
                res = sync_single_ontology_incremental(conn, code)
                conn.commit()

                if res["status"] == "SUCCESS" and (res["inserted"] > 0 or res["updated"] > 0):
                    any_updated = True

                # 이력 로깅
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tb_ontology_sync_history1 (
                            ontology_code, sync_type, status, inserted_terms, updated_terms,
                            obsoleted_terms, total_synonyms, duration_seconds, error_message,
                            started_at, finished_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        code, "INCREMENTAL_UPDATE", res["status"], res["inserted"], res["updated"],
                        res["obsoleted"], res["synonyms"], res["duration"], res["error"],
                        res["started_at"], res["finished_at"]
                    ))
                conn.commit()

            except Exception as e:
                conn.rollback()
                all_success = False
                logger.error(f"[{code}] 증분 동기화 실패: {e}")
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO tb_ontology_sync_history1 (
                                ontology_code, sync_type, status, error_message, started_at, finished_at
                            ) VALUES (%s, %s, %s, %s, NOW(), NOW())
                        """, (code, "INCREMENTAL_UPDATE", "FAILED", str(e)))
                    conn.commit()
                except Exception:
                    pass

        # 변경 발생 시 2-Pass 외래키 재매핑
        if any_updated:
            resolve_relationship_foreign_keys_pass2(conn)

        total_duration = round(time.time() - start_time, 2)
        logger.info("=" * 70)
        logger.info(f"★ 온톨로지 증분 동기화 완료 (총 소요시간: {total_duration}초)")
        logger.info("=" * 70)
        return all_success

    except Exception as e:
        logger.error(f"증분 동기화 프로세스 치명적 에러: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="K-BDS 온톨로지 9종 증분 동기화 CLI")
    parser.add_argument("--ontology", type=str, default="ALL", help="동기화 대상 온톨로지 코드 (예: CL, HP, ALL)")
    args = parser.parse_args()

    target_ontos = list(ONTOLOGIES.keys()) if args.ontology.upper() == "ALL" else [args.ontology.upper()]
    success = run_incremental_sync(target_ontos)
    sys.exit(0 if success else 1)
