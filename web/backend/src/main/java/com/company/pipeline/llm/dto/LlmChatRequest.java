package com.company.pipeline.llm.dto;

import com.company.pipeline.llm.LlmTargetType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

public record LlmChatRequest(
        @NotNull LlmTargetType targetType,
        @Size(max = 100) String targetId,
        @NotBlank @Size(max = 8_000) String question,
        @Valid @Size(max = 12) List<LlmChatMessage> messages
) {
    public List<LlmChatMessage> safeMessages() {
        return messages == null ? List.of() : messages;
    }
}
