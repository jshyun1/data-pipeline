-- =============================================================================
-- V73__create_llm_provider_config.sql  (GPT 설정 1단계 / docs/2026-09-13-gpt-settings-design.md)
-- =============================================================================
-- 속성창 GPT 탭·오류 분석이 쓸 LLM 을 등록하고 «하나만» 고르는 표.
--
-- ■ 배치 위치와 과금 구분을 나눈 이유
--   외부(Claude·ChatGPT·Gemini)는 무료 등급이어도 API 키가 필수다(웹 화면은 API 가 아니다).
--   키 필수 여부는 배치 위치가 정하고, 과금 구분은 표시·관리용일 뿐이다.
--
-- ■ 선택은 최대 하나, 0개도 허용
--   화면 라디오만으로는 관리자 둘이 동시에 누르면 뚫린다. 부분 유니크 인덱스로 DB 가 강제한다
--   (선례: etl_job_run 의 uq_etl_job_run_open). 선택이 없으면 GPT 기능 자체가 화면에서 사라진다.
--
-- ■ API 키
--   AesPasswordCryptoService(AES-256/GCM)로 암호화해 둔다. 응답에는 끝 4자리 힌트만 내린다.
--   app_setting 에 넣지 않는 이유는 V22 주석(덤프에 실려 나감) 참고.
--
-- 시각 규약: 신규 테이블은 TIMESTAMPTZ(V22 와 같다).
-- =============================================================================
CREATE TABLE llm_provider_config (
    id                 BIGSERIAL    PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    deployment         VARCHAR(20)  NOT NULL,              -- EXTERNAL | INTERNAL
    provider           VARCHAR(30)  NOT NULL,              -- ANTHROPIC | OPENAI | GEMINI | OPENAI_COMPATIBLE
    billing_type       VARCHAR(10)  NOT NULL DEFAULT 'FREE',
    base_url           VARCHAR(500),                       -- 비어 있으면 제공자 기본 주소(외부만)
    model_name         VARCHAR(200) NOT NULL,
    encrypted_api_key  TEXT,
    api_key_hint       VARCHAR(10),
    mask_sensitive     BOOLEAN      NOT NULL,              -- 전송 시 개인정보 마스킹(2단계에서 사용)
    max_input_chars    INTEGER      NOT NULL,              -- 수집 정보 자르기 기준(2단계에서 사용)
    is_active          BOOLEAN      NOT NULL DEFAULT FALSE,
    last_test_status   VARCHAR(30),
    last_test_message  VARCHAR(500),
    last_tested_at     TIMESTAMPTZ,
    description        TEXT,
    created_by         VARCHAR(255),
    updated_by         VARCHAR(255),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_llm_deployment CHECK (deployment IN ('EXTERNAL', 'INTERNAL')),
    CONSTRAINT ck_llm_billing CHECK (billing_type IN ('FREE', 'PAID'))
);

CREATE UNIQUE INDEX uq_llm_provider_config_name ON llm_provider_config (name);
CREATE UNIQUE INDEX uq_llm_provider_config_active ON llm_provider_config (is_active) WHERE is_active;

-- 메뉴. 대메뉴 순번은 CDC(40)와 설정(50) 사이. 형태는 V63·V72 를 따른다.
-- «역할 및 권한» 화면이 메뉴 이름을 보여줄 때 쓰는 표이고, 노출은 system_code·required_bits 로 정해진다.
INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, icon, system_code, required_bits, sort_ord, use_yn) VALUES
  ('GPT',          NULL,  'GPT 설정', NULL,            'gpt', 'ADMIN', 1, 45, 'Y'),
  ('GPT_REGISTER', 'GPT', '등록',     '/gpt/register', NULL,  'ADMIN', 7, 46, 'Y'),
  ('GPT_MANAGE',   'GPT', '관리',     '/gpt/manage',   NULL,  'ADMIN', 1, 47, 'Y')
ON CONFLICT (menu_id) DO UPDATE
SET parent_id = EXCLUDED.parent_id,
    menu_nm = EXCLUDED.menu_nm,
    menu_url = EXCLUDED.menu_url,
    icon = EXCLUDED.icon,
    system_code = EXCLUDED.system_code,
    required_bits = EXCLUDED.required_bits,
    sort_ord = EXCLUDED.sort_ord,
    use_yn = EXCLUDED.use_yn;
