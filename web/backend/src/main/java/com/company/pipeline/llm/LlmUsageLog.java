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

@Entity
@Table(name = "llm_usage_log")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LlmUsageLog {

    public static final String AGENT_JOB_ANALYSIS = "JOB_ANALYSIS";
    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, length = 255)
    private String userId;

    @Column(name = "agent_type", nullable = false, length = 30)
    private String agentType;

    @Enumerated(EnumType.STRING)
    @Column(name = "target_type", nullable = false, length = 30)
    private LlmTargetType targetType;

    @Column(name = "target_id", length = 100)
    private String targetId;

    @Column(name = "provider_config_id")
    private Long providerConfigId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private LlmProvider provider;

    @Column(name = "model_name", nullable = false, length = 200)
    private String modelName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private LlmDeployment deployment;

    @Column(nullable = false)
    private boolean masked;

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

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    public static LlmUsageLog create(String userId, LlmTargetType targetType, String targetId,
                                     LlmProviderConfig config, boolean masked) {
        LlmUsageLog log = new LlmUsageLog();
        log.userId = userId;
        log.agentType = AGENT_JOB_ANALYSIS;
        log.targetType = targetType;
        log.targetId = targetId;
        log.providerConfigId = config.getId();
        log.provider = config.getProvider();
        log.modelName = config.getModelName();
        log.deployment = config.getDeployment();
        log.masked = masked;
        log.status = STATUS_PENDING;
        log.createdAt = OffsetDateTime.now();
        return log;
    }
}
