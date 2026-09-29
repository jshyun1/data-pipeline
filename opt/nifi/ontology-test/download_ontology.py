# -*- coding: utf-8 -*-
"""
K-BDS 온톨로지 원천 OBO 파일 다운로드 및 HTTP 조건부 변경 감지 모듈
"""

import os
import sys
import time
import hashlib
import tempfile
import argparse
import urllib.request
import urllib.error
import ssl
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

from config import ONTOLOGIES, USER_AGENT, logger

# SSL 인증서 검증 우회 컨텍스트
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

def get_temp_download_dir() -> Path:
    """다운로드 임시 디렉터리 반환 및 생성"""
    download_dir = Path(tempfile.gettempdir()) / "kbds_ontology_cache"
    download_dir.mkdir(parents=True, exist_ok=True)
    return download_dir

def check_remote_header(url: str, etag: Optional[str] = None, last_modified: Optional[str] = None) -> Dict[str, Any]:
    """
    원격 서버에 HTTP HEAD 또는 조건부 요청을 보내 변경 여부를 초고속으로 확인합니다.
    """
    headers = {'User-Agent': USER_AGENT}
    if etag:
        headers['If-None-Match'] = etag
    if last_modified:
        headers['If-Modified-Since'] = last_modified

    req = urllib.request.Request(url, headers=headers, method='HEAD')
    result = {
        "status_code": 200,
        "is_modified": True,
        "etag": None,
        "last_modified": None,
        "content_length": 0
    }

    try:
        with urllib.request.urlopen(req, context=ssl_context, timeout=15) as resp:
            result["status_code"] = resp.status
            result["etag"] = resp.headers.get("ETag")
            result["last_modified"] = resp.headers.get("Last-Modified")
            result["content_length"] = int(resp.headers.get("Content-Length", 0))
            return result
    except urllib.error.HTTPError as e:
        if e.code == 304:
            # 304 Not Modified: 내용이 전혀 바뀌지 않음
            result["status_code"] = 304
            result["is_modified"] = False
            result["etag"] = etag
            result["last_modified"] = last_modified
            return result
        logger.warning(f"HTTP HEAD 요청 실패 ({url}): {e}")
        # HEAD 미지원 서버의 경우 다운로드 단계에서 MD5로 재검증
        return result
    except Exception as e:
        logger.warning(f"원격 헤더 조회 예외 ({url}): {e}")
        return result

def download_obo_file(ontology_code: str, max_retries: int = 3) -> Tuple[Optional[Path], Optional[str], Dict[str, Any]]:
    """
    온톨로지 OBO 파일을 스트리밍으로 다운로드하고 MD5 체크섬 및 HTTP 헤더를 반환합니다.
    재시도 백오프(Retry Backoff) 적용.
    """
    if ontology_code not in ONTOLOGIES:
        raise ValueError(f"알 수 없는 온톨로지 코드입니다: {ontology_code}")

    onto_info = ONTOLOGIES[ontology_code]
    url = onto_info["url"]
    download_dir = get_temp_download_dir()
    local_file = download_dir / f"{ontology_code.lower()}.obo"

    logger.info(f"[{ontology_code}] 원천 OBO 다운로드 시작: {url}")
    headers = {'User-Agent': USER_AGENT}
    req = urllib.request.Request(url, headers=headers)

    for attempt in range(1, max_retries + 1):
        try:
            md5_hash = hashlib.md5()
            total_bytes = 0

            with urllib.request.urlopen(req, context=ssl_context, timeout=60) as resp, open(local_file, "wb") as f:
                remote_etag = resp.headers.get("ETag")
                remote_last_modified = resp.headers.get("Last-Modified")

                while True:
                    chunk = resp.read(65536)  # 64KB 단위 스트리밍
                    if not chunk:
                        break
                    f.write(chunk)
                    md5_hash.update(chunk)
                    total_bytes += len(chunk)

            file_md5 = md5_hash.hexdigest()
            logger.info(f"[{ontology_code}] 다운로드 성공: {total_bytes:,} bytes (MD5: {file_md5})")
            
            meta_info = {
                "etag": remote_etag,
                "last_modified": remote_last_modified,
                "file_size": total_bytes
            }
            return local_file, file_md5, meta_info

        except Exception as e:
            logger.warning(f"[{ontology_code}] 다운로드 시도 {attempt}/{max_retries} 실패: {e}")
            if attempt < max_retries:
                time.sleep(attempt * 2)  # 지수 백오프
            else:
                logger.error(f"[{ontology_code}] 최종 다운로드 실패!")
                return None, None, {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="K-BDS 온톨로지 원천 파일 다운로드 CLI")
    parser.add_argument("--ontology", type=str, default="CL", help="다운로드할 온톨로지 코드 (예: CL, HP, ALL)")
    args = parser.parse_args()

    target_list = list(ONTOLOGIES.keys()) if args.ontology.upper() == "ALL" else [args.ontology.upper()]
    all_success = True

    for code in target_list:
        file_path, md5_val, meta = download_obo_file(code)
        if not file_path:
            all_success = False

    sys.exit(0 if all_success else 1)
