package com.company.pipeline.llm.dto;

import com.company.pipeline.llm.LlmBillingType;
import com.company.pipeline.llm.LlmDeployment;
import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.LlmProviderConfig;
import java.time.OffsetDateTime;

/** 목록·상세 응답. API 키는 절대 내리지 않는다 - 설정 여부와 끝 4자리 힌트만. */
public record LlmProviderResponse(
        Long id,
        String name,
        LlmDeployment deployment,
        LlmProvider provider,
        LlmBillingType billingType,
        String baseUrl,
        String effectiveBaseUrl,
        String modelName,
        boolean apiKeySet,
        String apiKeyHint,
        boolean maskSensitive,
        Integer maxInputChars,
        boolean active,
        String lastTestStatus,
        String lastTestMessage,
        OffsetDateTime lastTestedAt,
        String description,
        String createdBy,
        OffsetDateTime createdAt,
        String updatedBy,
        OffsetDateTime updatedAt) {

    public static LlmProviderResponse from(LlmProviderConfig c) {
        return new LlmProviderResponse(
                c.getId(), c.getName(), c.getDeployment(), c.getProvider(), c.getBillingType(),
                c.getBaseUrl(), c.effectiveBaseUrl(), c.getModelName(),
                c.getEncryptedApiKey() != null, c.getApiKeyHint(),
                c.isMaskSensitive(), c.getMaxInputChars(), c.isActive(),
                c.getLastTestStatus(), c.getLastTestMessage(), c.getLastTestedAt(),
                c.getDescription(), c.getCreatedBy(), c.getCreatedAt(), c.getUpdatedBy(), c.getUpdatedAt());
    }
}
