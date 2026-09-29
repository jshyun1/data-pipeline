# -*- coding: utf-8 -*-
"""
K-BDS 온톨로지 파이프라인 전역 환경 설정 및 온톨로지 메타 정의 모듈
"""

import os
import sys
import logging
from typing import Dict, Any
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import pool
except ImportError:
    psycopg2 = None


# 1. 환경 변수 로드 (.env 파일 수동 파싱 또는 python-dotenv 사용)
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

try:
    from dotenv import load_dotenv
    if ENV_PATH.exists():
        load_dotenv(dotenv_path=ENV_PATH)
    else:
        load_dotenv()
except ImportError:
    # python-dotenv가 없을 경우 간단한 수동 .env 파서
    if ENV_PATH.exists():
        with open(ENV_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())


# 2. 로깅 설정
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OntologyPipeline")

# 3. 데이터베이스 접속 설정
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ontology_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "options": f"-c search_path={DB_SCHEMA}"
}

# 4. EBI API 및 HTTP 요청 설정
EBI_OLS_BASE_URL = os.getenv("EBI_OLS_BASE_URL", "https://www.ebi.ac.uk/ols4/api")
EBI_TIMEOUT_SEC = int(os.getenv("EBI_TIMEOUT_SEC", "10"))
USER_AGENT = os.getenv("USER_AGENT", "KBDS-Ontology-Pipeline/1.0 (contact: admin@k-bds.kr)")

# 5. 수집 대상 9대 EBI OBO 온톨로지 정의
ONTOLOGIES: Dict[str, Dict[str, Any]] = {
    "CL": {
        "code": "CL",
        "name": "Cell Ontology",
        "url": "http://purl.obolibrary.org/obo/cl/cl-basic.obo",
        "domain": "Cell Type",
        "description": "세포 종류 및 세포 계층 구조"
    },
    "HP": {
        "code": "HP",
        "name": "Human Phenotype Ontology",
        "url": "http://purl.obolibrary.org/obo/hp.obo",
        "domain": "Phenotype / Clinical Feature",
        "description": "인간 질환 표현형 및 임상 특징"
    },
    "SO": {
        "code": "SO",
        "name": "Sequence Ontology",
        "url": "http://purl.obolibrary.org/obo/so.obo",
        "domain": "Sequence Feature / Variant Type",
        "description": "유전체 구조 및 서열 변이 피처"
    },
    "EDAM": {
        "code": "EDAM",
        "name": "EDAM Ontology",
        "url": "https://edamontology.org/EDAM.obo",
        "domain": "Data Format / Topic / Operation",
        "description": "생물정보학 데이터 포맷, 연산, 토픽"
    },
    "NCIT": {
        "code": "NCIT",
        "name": "NCI Thesaurus",
        "url": "http://purl.obolibrary.org/obo/ncit.obo",
        "domain": "Cancer / Disease / Drug",
        "description": "암, 질환, 임상 약물 및 바이오마커"
    },
    "EFO": {
        "code": "EFO",
        "name": "Experimental Factor Ontology",
        "url": "http://www.ebi.ac.uk/efo/efo.obo",
        "domain": "Experimental Factor / Disease",
        "description": "실험 요인, 질환, 측정 항목 통합"
    },
    "CHEBI": {
        "code": "CHEBI",
        "name": "Chemical Entities of Biological Interest",
        "url": "http://purl.obolibrary.org/obo/chebi/chebi_lite.obo",
        "domain": "Chemical Entity / Metabolite",
        "description": "화학 물질, 생체 분자, 대사체"
    },
    "OBI": {
        "code": "OBI",
        "name": "Ontology for Biomedical Investigations",
        "url": "http://purl.obolibrary.org/obo/obi.obo",
        "domain": "Assay / Instrument / Protocol",
        "description": "실험 장비, 기법, 분석 프로토콜"
    },
    "UBERON": {
        "code": "UBERON",
        "name": "Uber-anatomy Ontology",
        "url": "http://purl.obolibrary.org/obo/uberon/basic.obo",
        "domain": "Tissue / Anatomical Site",
        "description": "해부학 장기 및 조직 부위"
    }
}

# 6. DB 커넥션 헬퍼
def get_db_connection():
    """PostgreSQL 데이터베이스 연결 객체 생성 및 반환"""
    if psycopg2 is None:
        error_msg = (
            "PostgreSQL 드라이버(psycopg2)가 설치되어 있지 않습니다.\n"
            "  -> 해결 방법: pip install psycopg2-binary 또는 pip install -r Ontology/requirements.txt 를 실행해 주세요."
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL 연결 실패 ({DB_HOST}:{DB_PORT}/{DB_NAME}): {e}")
        logger.error(
            "  -> 로컬에 PostgreSQL이 실행 중인지 확인해 주세요. "
            "(Docker 사용 시: cd Ontology && docker-compose up -d)"
        )
        raise

