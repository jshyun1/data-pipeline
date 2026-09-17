package com.company.pipeline.llm.client;

import com.company.pipeline.llm.LlmProvider;
import java.util.function.Consumer;

public interface LlmClient {

    boolean supports(LlmProvider provider);

    LlmChatResult stream(LlmChatPrompt prompt, Consumer<String> onDelta);
}
