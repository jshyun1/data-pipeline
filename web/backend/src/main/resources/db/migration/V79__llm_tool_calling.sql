-- GPT 도구 호출(function calling) 설정 · 감지 결과(2026-10-02).
--
-- tool_calling : AUTO(감지 결과를 따른다) · ON(항상 쓴다) · OFF(쓰지 않는다)
-- tool_support : 연결 테스트 · 실제 호출에서 감지한 결과. SUPPORTED · UNSUPPORTED, 모르면 NULL
-- 모델을 바꾸면(수정) 감지 결과를 지운다 - 다음 테스트나 첫 호출에서 다시 정해진다.
ALTER TABLE llm_provider_config ADD COLUMN tool_calling VARCHAR(10) NOT NULL DEFAULT 'AUTO';
ALTER TABLE llm_provider_config ADD COLUMN tool_support VARCHAR(20);
ALTER TABLE llm_provider_config ADD COLUMN tool_checked_at TIMESTAMP WITH TIME ZONE;

COMMENT ON COLUMN llm_provider_config.tool_calling IS '도구 호출 사용: AUTO(감지 결과를 따름) · ON · OFF';
COMMENT ON COLUMN llm_provider_config.tool_support IS '감지한 도구 호출 지원 여부: SUPPORTED · UNSUPPORTED · NULL(모름)';
COMMENT ON COLUMN llm_provider_config.tool_checked_at IS '도구 호출 지원 여부를 마지막으로 확인한 시각';
