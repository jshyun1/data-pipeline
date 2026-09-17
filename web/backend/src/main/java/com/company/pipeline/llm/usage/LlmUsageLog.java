package com.company.pipeline.llm.usage;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** GPT 호출 기록 한 건(V74). 질문 · 답변 · 전송 정보 본문은 두지 않는다. */
@Entity
@Table(name = "llm_usage_log")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class LlmUsageLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "agent_type", nullable = false, length = 30)
    private String agentType;

    @Column(name = "target_type", nullable = false, length = 30)
    private String targetType;

    @Column(name = "target_id", length = 300)
    private String targetId;

    @Column(name = "provider_config_id")
    private Long providerConfigId;

    @Column(nullable = false, length = 30)
    private String provider;

    @Column(name = "model_name", nullable = false, length = 200)
    private String modelName;

    @Column(nullable = false, length = 20)
    private String deployment;

    @Column(nullable = false)
    private boolean masked;

    @Column(name = "input_chars")
    private Integer inputChars;

    @Column(name = "input_tokens")
    private Integer inputTokens;

    @Column(name = "output_tokens")
    private Integer outputTokens;

    @Column(name = "duration_ms")
    private Integer durationMs;

    @Column(nullable = false, length = 20)
    private String status;

    @Column(name = "error_code", length = 40)
    private String errorCode;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private OffsetDateTime createdAt;
}
