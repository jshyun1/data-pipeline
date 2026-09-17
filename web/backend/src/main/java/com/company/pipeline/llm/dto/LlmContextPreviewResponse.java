package com.company.pipeline.llm.dto;

public record LlmContextPreviewResponse(
        String targetTitle,
        String providerName,
        boolean masked,
        String context
) {
}
