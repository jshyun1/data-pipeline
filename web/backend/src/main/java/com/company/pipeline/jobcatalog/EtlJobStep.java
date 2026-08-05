package com.company.pipeline.jobcatalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * 잡을 이루는 프로세서 하나. V17 마이그레이션.
 *
 * <p>설정 전체는 {@code propsJson}에 통째로 담고, 조회에 자주 쓰는 값만 컬럼으로
 * 올린다("어느 DB의 무엇을 어디로 넣는가"에 답하는 값들). 프로세서 타입마다 프로퍼티
 * 키가 달라서 컬럼을 늘리는 방식으로는 감당이 안 되기 때문이다.
 */
@Entity
@Table(name = "etl_job_step")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJobStep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "nifi_processor_id", length = 100, nullable = false)
    private String nifiProcessorId;

    @Column(name = "step_name", length = 200, nullable = false)
    private String stepName;

    @Column(name = "step_type", length = 100, nullable = false)
    private String stepType;

    @Column(name = "scheduling_strategy", length = 30)
    private String schedulingStrategy;

    @Column(name = "scheduling_period", length = 50)
    private String schedulingPeriod;

    @Column(name = "sql_text")
    private String sqlText;

    @Column(name = "target_table", length = 200)
    private String targetTable;

    @Column(name = "statement_type", length = 30)
    private String statementType;

    @Column(name = "update_keys", length = 400)
    private String updateKeys;

    @Column(name = "dbcp_service_id", length = 100)
    private String dbcpServiceId;

    @Column(name = "x_pos")
    private Double xPos;

    @Column(name = "y_pos")
    private Double yPos;

    @Column(name = "validation_status", length = 20)
    private String validationStatus;

    @Column(name = "run_status", length = 20)
    private String runStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "props_json", columnDefinition = "jsonb")
    private String propsJson;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlJobStep(Long jobId, String nifiProcessorId, String stepName, String stepType) {
        this.jobId = jobId;
        this.nifiProcessorId = nifiProcessorId;
        this.stepName = stepName;
        this.stepType = stepType;
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void applySnapshot(Long jobId, String stepName, String stepType,
                              String schedulingStrategy, String schedulingPeriod,
                              String sqlText, String targetTable, String statementType,
                              String updateKeys, String dbcpServiceId,
                              Double xPos, Double yPos,
                              String validationStatus, String runStatus, String propsJson) {
        this.jobId = jobId;
        this.stepName = stepName;
        this.stepType = stepType;
        this.schedulingStrategy = schedulingStrategy;
        this.schedulingPeriod = schedulingPeriod;
        this.sqlText = sqlText;
        this.targetTable = targetTable;
        this.statementType = statementType;
        this.updateKeys = updateKeys;
        this.dbcpServiceId = dbcpServiceId;
        this.xPos = xPos;
        this.yPos = yPos;
        this.validationStatus = validationStatus;
        this.runStatus = runStatus;
        this.propsJson = propsJson;
        this.deletedAt = null;
        this.updatedAt = LocalDateTime.now();
    }

    public void markDeleted() {
        if (this.deletedAt == null) {
            this.deletedAt = LocalDateTime.now();
            this.updatedAt = this.deletedAt;
        }
    }
}
