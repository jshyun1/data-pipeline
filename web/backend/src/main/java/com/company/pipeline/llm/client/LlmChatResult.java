package com.company.pipeline.llm.client;

/** 한 번의 호출 결과. 토큰 수는 제공자가 알려 줄 때만 채워진다(OpenAI 호환 서버는 스트리밍에서 안 주는 경우가 많다). */
public record LlmChatResult(Integer inputTokens, Integer outputTokens, long durationMs) {
}
