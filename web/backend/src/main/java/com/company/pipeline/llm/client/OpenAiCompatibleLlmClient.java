package com.company.pipeline.llm.client;

import com.company.pipeline.llm.LlmProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.springframework.stereotype.Component;

@Component
public class OpenAiCompatibleLlmClient extends OpenAiLlmClient {

    public OpenAiCompatibleLlmClient(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public boolean supports(LlmProvider provider) {
        return provider == LlmProvider.OPENAI_COMPATIBLE;
    }

    @Override
    public LlmChatResult stream(LlmChatPrompt prompt, Consumer<String> onDelta) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", prompt.modelName());
        payload.put("messages", openAiMessages(prompt.systemPrompt(), prompt.history(), prompt.question()));
        payload.put("temperature", 0.2);
        payload.put("max_tokens", 1200);
        payload.put("stream", true);
        return streamChatCompletions(prompt, payload, onDelta);
    }
}
