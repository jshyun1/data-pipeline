package com.company.pipeline.llm.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record LlmChatMessage(
        @NotBlank @Size(max = 20) String role,
        @NotBlank @Size(max = 8_000) String content
) {
}
