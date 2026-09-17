package com.company.pipeline.llm.client;

import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.dto.LlmChatMessage;
import java.util.List;

public record LlmChatPrompt(
        LlmProvider provider,
        String baseUrl,
        String modelName,
        String apiKey,
        String systemPrompt,
        List<LlmChatMessage> history,
        String question
) {
}
