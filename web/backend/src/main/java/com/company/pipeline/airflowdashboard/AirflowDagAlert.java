package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "airflow_dag_alert")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AirflowDagAlert extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_id", nullable = false, length = 250)
    private String dagId;

    @Enumerated(EnumType.STRING)
    @Column(name = "rule_type", nullable = false, length = 30)
    private RuleType ruleType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private Severity severity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @Column(name = "detected_at", nullable = false)
    private LocalDateTime detectedAt;

    @Column(name = "last_detected_at", nullable = false)
    private LocalDateTime lastDetectedAt;

    @Column(name = "acknowledged_at")
    private LocalDateTime acknowledgedAt;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    public AirflowDagAlert(
            String dagId,
            RuleType ruleType,
            Severity severity,
            String message,
            LocalDateTime detectedAt) {
        this.dagId = dagId;
        this.ruleType = ruleType;
        this.severity = severity;
        this.status = Status.OPEN;
        this.message = message;
        this.detectedAt = detectedAt;
        this.lastDetectedAt = detectedAt;
    }

    public void detect(Severity severity, String message, LocalDateTime detectedAt) {
        this.severity = severity;
        this.message = message;
        this.lastDetectedAt = detectedAt;
    }

    public void acknowledge(LocalDateTime acknowledgedAt) {
        if (status == Status.OPEN) {
            status = Status.ACKNOWLEDGED;
            this.acknowledgedAt = acknowledgedAt;
        }
    }

    public void resolve(LocalDateTime resolvedAt) {
        if (status != Status.RESOLVED) {
            status = Status.RESOLVED;
            this.resolvedAt = resolvedAt;
        }
    }

    public enum RuleType {
        CONSECUTIVE_FAILURE,
        STALE,
        DURATION_ANOMALY,
        SLA_EXCEEDED
    }

    public enum Severity {
        INFO,
        WARNING,
        DANGER
    }

    public enum Status {
        OPEN,
        ACKNOWLEDGED,
        RESOLVED
    }
}
