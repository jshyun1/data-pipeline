# -*- coding: utf-8 -*-
"""
K-BDS 온톨로지 OBO 포맷 고속 스트리밍 파싱 및 정규화 모듈
"""

import re
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from config import logger

# 정규식 패턴 사전 컴파일 (초고속 파싱)
RE_DEF = re.compile(r'\"((?:\\.|[^\"])*)\"')
RE_SYNONYM = re.compile(r'\"((?:\\.|[^\"])*)\"\s*([A-Z]+)?')
RE_XREF = re.compile(r'^([A-Za-z0-9_\-\.]+):(.+)$')
RE_RELATIONSHIP = re.compile(r'^(\S+)\s+(\S+)')

def parse_obo_file(filepath: Path, ontology_code: str) -> Dict[str, Any]:
    """
    OBO 파일을 1줄씩 스트리밍 파싱하여 정규화된 용어, 동의어, 교차참조, 관계 데이터를 추출합니다.
    """
    terms: List[Dict[str, Any]] = []
    synonyms: List[Dict[str, Any]] = []
    xrefs: List[Dict[str, Any]] = []
    relationships: List[Dict[str, Any]] = []
    
    obo_version: Optional[str] = None
    in_header = True
    current_term: Optional[Dict[str, Any]] = None

    def flush_term(t: Dict[str, Any]):
        """완성된 단일 Term 데이터를 각 테이블 레코드 구조로 변환하여 수집"""
        if not t or 'id' not in t:
            return

        std_id = t['id']
        pref_name = t.get('name') or std_id
        definition = t.get('def')
        is_obsolete = 'Y' if t.get('is_obsolete', False) else 'N'
        replaced_by = t.get('replaced_by')
        iri = f"http://purl.obolibrary.org/obo/{std_id.replace(':', '_')}"

        # 1. Term 마스터 레코드
        terms.append({
            "ontology_code": ontology_code,
            "standard_id": std_id,
            "preferred_term": pref_name,
            "definition": definition,
            "iri": iri,
            "term_type": "CLASS",
            "is_obsolete": is_obsolete,
            "replaced_by": replaced_by
        })

        # 2. 대표어 (EXACT) 기본 동의어로 1건 추가
        synonyms.append({
            "ontology_code": ontology_code,
            "standard_id": std_id,
            "synonym_term": pref_name,
            "synonym_type": "EXACT"
        })

        # 3. 추가 동의어 등록
        for syn_val, scope in t.get('synonyms', []):
            if syn_val and syn_val != pref_name:
                synonyms.append({
                    "ontology_code": ontology_code,
                    "standard_id": std_id,
                    "synonym_term": syn_val,
                    "synonym_type": scope or "RELATED"
                })

        # 4. 독립된 정식 교차 참조(xref) 등록
        for raw_xref in t.get('xrefs', []):
            raw_xref = raw_xref.strip()
            # 메타데이터 태그 ({...}) 제거
            if '{' in raw_xref:
                raw_xref = raw_xref.split('{')[0].strip()
            if not raw_xref:
                continue

            # DB코드와 식별자 분리
            m = RE_XREF.match(raw_xref)
            if m:
                db_code = m.group(1).upper()
                accession = raw_xref
                xrefs.append({
                    "source_ontology_code": ontology_code,
                    "standard_id": std_id,
                    "target_db_code": db_code,
                    "target_accession": accession,
                    "description": None
                })
            else:
                xrefs.append({
                    "source_ontology_code": ontology_code,
                    "standard_id": std_id,
                    "target_db_code": "OTHER",
                    "target_accession": raw_xref,
                    "description": None
                })

        # 5. 계층(is_a) 및 관계(relationship) 등록
        for obj_id in t.get('is_a', []):
            # 주석 제거 (예: is_a: CL:0000738 ! leukocyte -> CL:0000738)
            clean_obj_id = obj_id.split('!')[0].strip()
            if clean_obj_id:
                relationships.append({
                    "subject_standard_id": std_id,
                    "predicate": "is_a",
                    "object_standard_id": clean_obj_id
                })

        for rel_str in t.get('relationships', []):
            clean_rel = rel_str.split('!')[0].strip()
            m_rel = RE_RELATIONSHIP.match(clean_rel)
            if m_rel:
                pred = m_rel.group(1)
                obj_id = m_rel.group(2)
                relationships.append({
                    "subject_standard_id": std_id,
                    "predicate": pred,
                    "object_standard_id": obj_id
                })

    logger.info(f"[{ontology_code}] OBO 파일 파싱 시작: {filepath}")

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # 헤더 구간 data-version 추출
            if in_header:
                if line.startswith('data-version:'):
                    obo_version = line[13:].strip()
                elif line == '[Term]' or line.startswith('['):
                    in_header = False

            # Term 블록 시작
            if line == '[Term]':
                if current_term:
                    flush_term(current_term)
                current_term = {
                    'synonyms': [],
                    'xrefs': [],
                    'is_a': [],
                    'relationships': [],
                    'is_obsolete': False
                }

            # 다른 블록 시작 (Typedef 등)
            elif line.startswith('[') and line.endswith(']'):
                if current_term:
                    flush_term(current_term)
                current_term = None

            # Term 속성 파싱
            elif current_term is not None:
                if line.startswith('id:'):
                    current_term['id'] = line[3:].strip()
                elif line.startswith('name:'):
                    current_term['name'] = line[5:].strip()
                elif line.startswith('def:'):
                    m = RE_DEF.search(line)
                    if m:
                        current_term['def'] = m.group(1).replace('\\"', '"').replace('\\n', ' ')
                    else:
                        current_term['def'] = line[4:].strip()
                elif line.startswith('is_obsolete:'):
                    if 'true' in line.lower():
                        current_term['is_obsolete'] = True
                elif line.startswith('replaced_by:'):
                    current_term['replaced_by'] = line[12:].strip()
                elif line.startswith('synonym:'):
                    m = RE_SYNONYM.search(line)
                    if m:
                        syn_val = m.group(1).replace('\\"', '"').strip()
                        scope = m.group(2) if m.group(2) else "EXACT"
                        current_term['synonyms'].append((syn_val, scope))
                elif line.startswith('xref:'):
                    xref_val = line[5:].strip()
                    if xref_val:
                        current_term['xrefs'].append(xref_val)
                elif line.startswith('is_a:'):
                    current_term['is_a'].append(line[5:].strip())
                elif line.startswith('relationship:'):
                    current_term['relationships'].append(line[13:].strip())

        # 마지막 Term 저장
        if current_term:
            flush_term(current_term)

    logger.info(f"[{ontology_code}] 파싱 완료 -> 용어 {len(terms):,}건, 동의어 {len(synonyms):,}건, 교차참조 {len(xrefs):,}건, 관계 {len(relationships):,}건 (버전: {obo_version or 'N/A'})")

    return {
        "ontology_code": ontology_code,
        "version": obo_version,
        "terms": terms,
        "synonyms": synonyms,
        "xrefs": xrefs,
        "relationships": relationships
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OBO 파서 단독 테스트 CLI")
    parser.add_argument("--file", type=str, required=True, help="파싱할 OBO 파일 경로")
    parser.add_argument("--ontology", type=str, default="CL", help="온톨로지 코드")
    args = parser.parse_args()

    data = parse_obo_file(Path(args.file), args.ontology.upper())
    print(f"용어 수: {len(data['terms']):,}")
    print(f"동의어 수: {len(data['synonyms']):,}")
    print(f"Xref 수: {len(data['xrefs']):,}")
    print(f"관계 수: {len(data['relationships']):,}")
    if data['terms']:
        print("샘플 용어 1:", data['terms'][0])
