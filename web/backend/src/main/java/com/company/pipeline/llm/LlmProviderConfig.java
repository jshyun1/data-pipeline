package com.company.pipeline.llm;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 등록된 LLM 하나. {@code active} 인 행은 DB 에서 최대 하나로 강제된다(uq_llm_provider_config_active).
 *
 * <p>시각은 BaseAuditEntity(LocalDateTime/TIMESTAMP)를 쓰지 않는다 - 신규 테이블 규약이 TIMESTAMPTZ 다(V22).
 */
@Entity
@Table(name = "llm_provider_config")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LlmProviderConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private LlmDeployment deployment;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private LlmProvider provider;

    @Enumerated(EnumType.STRING)
    @Column(name = "billing_type", nullable = false, length = 10)
    private LlmBillingType billingType;

    /** 비어 있으면 제공자 기본 주소를 쓴다(외부만). */
    @Column(name = "base_url", length = 500)
    private String baseUrl;

    @Column(name = "model_name", nullable = false, length = 200)
    private String modelName;

    @Column(name = "encrypted_api_key", columnDefinition = "TEXT")
    private String encryptedApiKey;

    @Column(name = "api_key_hint", length = 10)
    private String apiKeyHint;

    @Column(name = "mask_sensitive", nullable = false)
    private boolean maskSensitive;

    @Column(name = "max_input_chars", nullable = false)
    private Integer maxInputChars;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @Column(name = "last_test_status", length = 30)
    private String lastTestStatus;

    @Column(name = "last_test_message", length = 500)
    private String lastTestMessage;

    @Column(name = "last_tested_at")
    private OffsetDateTime lastTestedAt;

    @Column(columnDefinition = "TEXT")
    private String description;

    /** 도구 호출 사용: AUTO(감지 결과를 따름) · ON · OFF (V79). */
    @Column(name = "tool_calling", nullable = false, length = 10)
    private String toolCalling = TOOL_AUTO;

    /** 감지한 도구 호출 지원 여부: SUPPORTED · UNSUPPORTED · null(모름). */
    @Column(name = "tool_support", length = 20)
    private String toolSupport;

    @Column(name = "tool_checked_at")
    private OffsetDateTime toolCheckedAt;

    @Column(name = "created_by", length = 255)
    private String createdBy;

    @Column(name = "updated_by", length = 255)
    private String updatedBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public static LlmProviderConfig create(String name, LlmProvider provider, String actor) {
        LlmProviderConfig config = new LlmProviderConfig();
        OffsetDateTime now = OffsetDateTime.now();
        config.name = name;
        config.provider = provider;
        config.deployment = provider.getDeployment();
        config.createdBy = actor;
        config.updatedBy = actor;
        config.createdAt = now;
        config.updatedAt = now;
        return config;
    }

    /** 주소가 비어 있으면 제공자 기본값. 끝의 / 는 뗀다(경로를 이어 붙이므로). */
    public String effectiveBaseUrl() {
        String url = baseUrl == null || baseUrl.isBlank() ? provider.getDefaultBaseUrl() : baseUrl.trim();
        if (url == null) {
            return null;
        }
        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }

    public static final String TOOL_AUTO = "AUTO";
    public static final String TOOL_ON = "ON";
    public static final String TOOL_OFF = "OFF";
    public static final String TOOL_SUPPORTED = "SUPPORTED";
    public static final String TOOL_UNSUPPORTED = "UNSUPPORTED";

    /**
     * 이번 호출에 도구를 실을까. 도구 형식은 OpenAI · OpenAI 호환만 구현했다 - 다른 제공자는 언제나 «질문 속 대상 상세»만 쓴다.
     * AUTO 는 «안 된다고 확인된 경우»만 끈다(모르면 한 번 시도해 보고, 거절되면 그때 UNSUPPORTED 로 적는다).
     */
    public boolean toolsEnabled() {
        if (provider != LlmProvider.OPENAI && provider != LlmProvider.OPENAI_COMPATIBLE) {
            return false;
        }
        if (TOOL_OFF.equals(toolCalling)) {
            return false;
        }
        return TOOL_ON.equals(toolCalling) || !TOOL_UNSUPPORTED.equals(toolSupport);
    }

    public void recordToolSupport(String support) {
        this.toolSupport = support;
        this.toolCheckedAt = OffsetDateTime.now();
    }

    public void touch(String actor) {
        this.updatedBy = actor;
        this.updatedAt = OffsetDateTime.now();
    }
}
