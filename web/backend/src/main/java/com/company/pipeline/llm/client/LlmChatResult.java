package com.company.pipeline.llm.client;

public record LlmChatResult(String answer, Integer inputTokens, Integer outputTokens) {
}
