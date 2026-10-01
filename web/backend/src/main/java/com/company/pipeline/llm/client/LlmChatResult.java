package com.company.pipeline.llm.client;

import java.util.List;

/**
 * 한 번의 호출 결과. 토큰 수는 제공자가 알려 줄 때만 채워진다(OpenAI 호환 서버는 스트리밍에서 안 주는 경우가 많다).
 *
 * @param text      흘려보낸 답 전체(도구 호출 판정 · 재시도에 쓴다)
 * @param toolCalls 모델이 부른 도구. 없으면 빈 목록
 */
public record LlmChatResult(Integer inputTokens, Integer outputTokens, long durationMs, String text,
                            List<LlmMessage.ToolCall> toolCalls) {

    public LlmChatResult(Integer inputTokens, Integer outputTokens, long durationMs) {
        this(inputTokens, outputTokens, durationMs, "", List.of());
    }

    public boolean hasToolCalls() {
        return toolCalls != null && !toolCalls.isEmpty();
    }
}
