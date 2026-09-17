-- llm_usage_log 를 GPT 2단계 구현이 기대하는 모양으로 맞춘다.
--
-- 같은 기능을 두 갈래로 만들면서 V74 가 양쪽에 각각 생겼다. 사내 쪽 V74 가 먼저 적용돼
-- 로컬·운영 DB 에 이미 들어가 있으므로 그 파일은 고치지 않는다 - 적용된 마이그레이션을
-- 수정하면 체크섬이 어긋나 Flyway 가 기동을 막는다. 차이만 여기서 메운다.
--
-- 새로 설치하는 환경은 V74(사내본) + 이 파일을 순서대로 적용해 같은 결과에 도달한다.

-- 전송 글자 수. usage/LlmUsageLog 엔티티가 input_chars 로 매핑하므로 컬럼이 없으면 기동이 막힌다.
ALTER TABLE llm_usage_log ADD COLUMN IF NOT EXISTS input_chars INTEGER;

-- 대상 식별자. 워크플로우 태스크 키처럼 긴 값이 들어와 100자로는 모자란다.
ALTER TABLE llm_usage_log ALTER COLUMN target_id TYPE VARCHAR(300);

-- 조회 축이 «대상» 에서 «사용자» 로 바뀌었다(사용량·비용을 사람 단위로 본다).
DROP INDEX IF EXISTS ix_llm_usage_log_target;
ALTER INDEX IF EXISTS ix_llm_usage_log_created_at RENAME TO ix_llm_usage_log_created;
CREATE INDEX IF NOT EXISTS ix_llm_usage_log_user ON llm_usage_log (user_id, created_at DESC);
