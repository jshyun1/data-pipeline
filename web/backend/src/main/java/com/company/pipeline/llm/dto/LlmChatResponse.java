package com.company.pipeline.llm.dto;

public record LlmChatResponse(
        String answer,
        String providerName,
        String modelName,
        boolean masked,
        int durationMs
) {
}
