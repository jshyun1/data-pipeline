package com.company.pipeline.llm.dto;

import com.company.pipeline.llm.LlmBillingType;
import com.company.pipeline.llm.LlmProvider;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * 등록·수정·저장 전 연결 테스트가 같이 쓰는 입력.
 *
 * <p>배치 위치는 받지 않는다 - 제공자가 정한다({@link LlmProvider#getDeployment()}). 둘을 따로 받으면
 * «내부 + Claude» 같은 모순된 조합이 들어올 수 있다.
 *
 * <p>{@code apiKey} 가 비어 있으면: 등록은 (외부면) 거절, 수정·저장된 항목 테스트는 기존 키를 쓴다.
 */
public record LlmProviderRequest(
        @NotBlank @Size(max = 100) String name,
        @NotNull LlmProvider provider,
        @NotNull LlmBillingType billingType,
        @Size(max = 500) String baseUrl,
        @NotBlank @Size(max = 200) String modelName,
        String apiKey,
        @NotNull Boolean maskSensitive,
        @Min(1000) @Max(2_000_000) Integer maxInputChars,
        String description,
        /** 도구 호출 사용: AUTO · ON · OFF. 비우면 AUTO(2026-10-02). */
        @jakarta.validation.constraints.Pattern(regexp = "AUTO|ON|OFF") String toolCalling) {

    public LlmProviderRequest(String name, LlmProvider provider, LlmBillingType billingType, String baseUrl,
                              String modelName, String apiKey, Boolean maskSensitive, Integer maxInputChars,
                              String description) {
        this(name, provider, billingType, baseUrl, modelName, apiKey, maskSensitive, maxInputChars, description, null);
    }
}
