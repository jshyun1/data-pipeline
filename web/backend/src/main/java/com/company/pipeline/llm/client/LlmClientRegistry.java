package com.company.pipeline.llm.client;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.llm.LlmProvider;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class LlmClientRegistry {

    private final List<LlmClient> clients;

    public LlmClientRegistry(List<LlmClient> clients) {
        this.clients = clients;
    }

    public LlmClient get(LlmProvider provider) {
        return clients.stream()
                .filter(client -> client.supports(provider))
                .findFirst()
                .orElseThrow(() -> new BusinessException(ErrorCode.LLM_CHAT_ERROR, "지원하지 않는 GPT 제공자입니다."));
    }
}
