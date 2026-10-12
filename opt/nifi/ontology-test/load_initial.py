# -*- coding: utf-8 -*-
"""
K-BDS 온톨로지 9종 초기 대용량 전체 적재 (Full Load) 스크립트 (2-Pass 로딩 방식 적용)
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
from download_ontology import download_obo_file
from parse_ontology import parse_obo_file

BATCH_SIZE = 5000

def load_single_ontology_pass1(conn, parsed_data: Dict[str, Any], meta_info: Dict[str, Any], file_md5: str) -> Dict[str, int]:
    """
    [1-Pass] 단일 온톨로지의 Term, Synonym, Xref, Relationship(Standard ID 기준)을 벌크 적재합니다.
    """
    onto_code = parsed_data["ontology_code"]
    terms = parsed_data["terms"]
    synonyms = parsed_data["synonyms"]
    xrefs = parsed_data["xrefs"]
    relationships = parsed_data["relationships"]
    version = parsed_data.get("version")

    with conn.cursor() as cur:
        # tb_ontology_term1.ontology_code FK가 tb_ontology_meta1을 참조하므로
        # term 적재 전에 기준 meta 행을 먼저 보장한다.
        upsert_meta_sql = """
            INSERT INTO tb_ontology_meta1 (
                ontology_code, ontology_name, source_url, version, etag, last_modified, file_md5,
                term_count, synonym_count, last_synced_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (ontology_code) DO UPDATE
            SET ontology_name = EXCLUDED.ontology_name,
                source_url = EXCLUDED.source_url,
                version = EXCLUDED.version,
                etag = EXCLUDED.etag,
                last_modified = EXCLUDED.last_modified,
                file_md5 = EXCLUDED.file_md5,
                term_count = EXCLUDED.term_count,
                synonym_count = EXCLUDED.synonym_count,
                last_synced_at = NOW(),
                updated_at = NOW();
        """
        cur.execute(upsert_meta_sql, (
            onto_code,
            ONTOLOGIES[onto_code]["name"],
            ONTOLOGIES[onto_code]["url"],
            version,
            meta_info.get("etag"),
            meta_info.get("last_modified"),
            file_md5,
            0,
            0
        ))

        # 기존 데이터 삭제 (해당 온톨로지만 정리 - CASCADE로 하위 테이블 동시 삭제)
        cur.execute("DELETE FROM tb_ontology_term1 WHERE ontology_code = %s", (onto_code,))

        # 1. tb_ontology_term1 벌크 적재
        term_records = [
            (
                t["ontology_code"],
                t["standard_id"],
                t["preferred_term"],
                t["definition"],
                t["iri"],
                t["term_type"],
                t["is_obsolete"],
                t["replaced_by"]
            )
            for t in terms
        ]
        
        insert_term_sql = """
            INSERT INTO tb_ontology_term1 (
                ontology_code, standard_id, preferred_term, definition, iri, term_type, is_obsolete, replaced_by
            ) VALUES %s
            RETURNING standard_id, term_pk;
        """
        
        # ID -> PK 매핑 딕셔너리 구축 (하위 테이블 FK 연결용)
        id_to_pk = {}
        for i in range(0, len(term_records), BATCH_SIZE):
            chunk = term_records[i:i + BATCH_SIZE]
            result = execute_values(cur, insert_term_sql, chunk, fetch=True)
            for std_id, term_pk in result:
                id_to_pk[std_id] = term_pk

        logger.info(f"[{onto_code}] 1-Pass: 용어 {len(term_records):,}건 적재 완료")

        # 2. tb_ontology_synonym1 벌크 적재
        syn_records = [
            (
                id_to_pk[s["standard_id"]],
                s["ontology_code"],
                s["synonym_term"],
                s["synonym_type"]
            )
            for s in synonyms if s["standard_id"] in id_to_pk
        ]
        if syn_records:
            insert_syn_sql = """
                INSERT INTO tb_ontology_synonym1 (
                    term_pk, ontology_code, synonym_term, synonym_type
                ) VALUES %s
            """
            for i in range(0, len(syn_records), BATCH_SIZE):
                chunk = syn_records[i:i + BATCH_SIZE]
                execute_values(cur, insert_syn_sql, chunk)
            logger.info(f"[{onto_code}] 1-Pass: 동의어 {len(syn_records):,}건 적재 완료")

        # 3. tb_ontology_xref1 벌크 적재
        xref_records = [
            (
                id_to_pk[x["standard_id"]],
                x["source_ontology_code"],
                x["target_db_code"],
                x["target_accession"],
                x["description"]
            )
            for x in xrefs if x["standard_id"] in id_to_pk
        ]
        if xref_records:
            insert_xref_sql = """
                INSERT INTO tb_ontology_xref1 (
                    term_pk, source_ontology_code, target_db_code, target_accession, description
                ) VALUES %s
            """
            for i in range(0, len(xref_records), BATCH_SIZE):
                chunk = xref_records[i:i + BATCH_SIZE]
                execute_values(cur, insert_xref_sql, chunk)
            logger.info(f"[{onto_code}] 1-Pass: 교차참조 {len(xref_records):,}건 적재 완료")

        # 4. tb_ontology_relationship1 벌크 적재 (object_term_pk는 NULL로 기록)
        rel_records = [
            (
                id_to_pk[r["subject_standard_id"]],
                r["predicate"],
                r["object_standard_id"]
            )
            for r in relationships if r["subject_standard_id"] in id_to_pk
        ]
        if rel_records:
            insert_rel_sql = """
                INSERT INTO tb_ontology_relationship1 (
                    subject_term_pk, predicate, object_standard_id
                ) VALUES %s
            """
            for i in range(0, len(rel_records), BATCH_SIZE):
                chunk = rel_records[i:i + BATCH_SIZE]
                execute_values(cur, insert_rel_sql, chunk)
            logger.info(f"[{onto_code}] 1-Pass: 계층/관계 {len(rel_records):,}건 적재 완료")

        # 5. tb_ontology_meta1 최종 건수 갱신
        cur.execute(upsert_meta_sql, (
            onto_code,
            ONTOLOGIES[onto_code]["name"],
            ONTOLOGIES[onto_code]["url"],
            version,
            meta_info.get("etag"),
            meta_info.get("last_modified"),
            file_md5,
            len(term_records),
            len(syn_records)
        ))

    return {
        "terms": len(term_records),
        "synonyms": len(syn_records),
        "xrefs": len(xref_records),
        "relationships": len(rel_records)
    }

def resolve_relationship_foreign_keys_pass2(conn) -> int:
    """
    [2-Pass] 모든 온톨로지가 적재된 후, tb_ontology_relationship1의 object_term_pk 및 target_ontology_code를
    tb_ontology_term1과 조인하여 일괄 매핑합니다.
    """
    logger.info("=" * 65)
    logger.info("  [2-Pass] 온톨로지 간 상호 참조 관계(FK) 일괄 매핑 시작...")
    logger.info("=" * 65)

    update_fk_sql = """
        UPDATE tb_ontology_relationship1 r
        SET object_term_pk = t.term_pk,
            target_ontology_code = t.ontology_code
        FROM tb_ontology_term1 t
        WHERE r.object_standard_id = t.standard_id
          AND (r.object_term_pk IS NULL OR r.object_term_pk != t.term_pk);
    """

    with conn.cursor() as cur:
        cur.execute(update_fk_sql)
        updated_count = cur.rowcount
        conn.commit()

    logger.info(f"  -> [2-Pass] 총 {updated_count:,}건의 온톨로지 간 관계(FK) 연결 완료!")
    return updated_count

def run_initial_load(ontology_codes: Optional[List[str]] = None) -> bool:
    """9개 온톨로지 전체 초기 적재 실행기"""
    targets = ontology_codes or list(ONTOLOGIES.keys())
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("  K-BDS 국제 온톨로지(9종) 대용량 초기 전체 적재(Full Load) 시작")
    logger.info("=" * 70)

    conn = None
    all_success = True

    try:
        conn = get_db_connection()

        for code in targets:
            onto_start = time.time()
            started_at = datetime.now(timezone.utc)
            status = "SUCCESS"
            error_msg = None
            counts = {"terms": 0, "synonyms": 0, "xrefs": 0, "relationships": 0}

            try:
                # 1. 다운로드
                file_path, file_md5, meta_info = download_obo_file(code)
                if not file_path or not file_md5:
                    raise RuntimeError(f"OBO 파일 다운로드 실패: {code}")

                # 2. 파싱
                parsed_data = parse_obo_file(file_path, code)

                # 3. 1-Pass 적재
                counts = load_single_ontology_pass1(conn, parsed_data, meta_info, file_md5)
                conn.commit()

            except Exception as e:
                conn.rollback()
                status = "FAILED"
                error_msg = str(e)
                all_success = False
                logger.error(f"[{code}] 초기 적재 중 오류 발생: {e}")

            finally:
                duration = round(time.time() - onto_start, 2)
                finished_at = datetime.now(timezone.utc)

                # 4. 이력 기록
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO tb_ontology_sync_history1 (
                                ontology_code, sync_type, status, inserted_terms, updated_terms,
                                obsoleted_terms, total_synonyms, duration_seconds, error_message,
                                started_at, finished_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            code, "INITIAL_FULL", status, counts["terms"], 0, 0,
                            counts["synonyms"], duration, error_msg, started_at, finished_at
                        ))
                    conn.commit()
                except Exception as log_err:
                    logger.warning(f"[{code}] 이력 로깅 실패: {log_err}")

        # 5. 전체 적재 완료 후 2-Pass 실행
        if all_success:
            resolve_relationship_foreign_keys_pass2(conn)

        total_duration = round(time.time() - start_time, 2)
        logger.info("=" * 70)
        logger.info(f"★ 온톨로지 초기 전체 적재 완료 (총 소요시간: {total_duration}초)")
        logger.info("=" * 70)
        return all_success

    except Exception as e:
        logger.error(f"초기 적재 프로세스 치명적 에러: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="K-BDS 온톨로지 9종 초기 적재 CLI")
    parser.add_argument("--ontology", type=str, nargs="+", default=["ALL"], help="적재 대상 온톨로지 코드 (예: CL HP ALL). 여러 개 공백으로 지정 가능.")
    args = parser.parse_args()

    codes = [c.upper() for c in args.ontology]
    target_ontos = list(ONTOLOGIES.keys()) if "ALL" in codes else codes
    success = run_initial_load(target_ontos)
    sys.exit(0 if success else 1)
