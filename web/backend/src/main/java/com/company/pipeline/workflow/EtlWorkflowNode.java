package com.company.pipeline.workflow;

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

/**
 * 캔버스 위의 노드 1개. V57 마이그레이션.
 *
 * <p>{@code JOB} 노드는 {@code etl_job}을 <b>참조</b>한다. NiFi는 프로세스 그룹을 참조로
 * 재사용할 수 없어서(붙여넣으면 독립 복사본이 된다) 재사용은 이 계층에서만 성립한다 -
 * 같은 job을 여러 워크플로우가 참조해도 NiFi에는 실체가 하나뿐이다.
 *
 * <p>{@code nodeKey}는 Airflow TaskGroup id가 되므로 캔버스 안에서 불변이어야 한다.
 */
@Entity
@Table(name = "etl_workflow_node")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlWorkflowNode {

    public static final String TYPE_JOB = "JOB";
    public static final String TYPE_BRANCH = "BRANCH";
    public static final String TYPE_JOIN = "JOIN";
    public static final String TYPE_START = "START";
    public static final String TYPE_END = "END";
    public static final String TYPE_SUBWF = "SUBWF";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", nullable = false)
    private Long workflowId;

    @Column(name = "node_key", length = 80, nullable = false)
    private String nodeKey;

    @Column(name = "node_type", length = 20, nullable = false)
    private String nodeType;

    @Column(name = "job_id")
    private Long jobId;

    @Column(name = "sub_workflow_id")
    private Long subWorkflowId;

    @Column(name = "trigger_rule", length = 40, nullable = false)
    private String triggerRule;

    @Column(name = "branch_expr")
    private String branchExpr;

    @Column(name = "retries", nullable = false)
    private int retries;

    @Column(name = "retry_delay_sec", nullable = false)
    private int retryDelaySec;

    @Column(name = "display_x")
    private Double displayX;

    @Column(name = "display_y")
    private Double displayY;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlWorkflowNode(Long workflowId, String nodeKey, String nodeType, Long jobId,
                           Long subWorkflowId, String triggerRule, String branchExpr,
                           Integer retries, Integer retryDelaySec, Double displayX, Double displayY) {
        LocalDateTime now = LocalDateTime.now();
        this.workflowId = workflowId;
        this.nodeKey = nodeKey;
        this.nodeType = nodeType == null ? TYPE_JOB : nodeType;
        this.jobId = jobId;
        this.subWorkflowId = subWorkflowId;
        this.triggerRule = triggerRule == null ? "ALL_SUCCESS" : triggerRule;
        this.branchExpr = branchExpr;
        this.retries = retries == null ? 0 : retries;
        this.retryDelaySec = retryDelaySec == null ? 60 : retryDelaySec;
        this.displayX = displayX;
        this.displayY = displayY;
        this.createdAt = now;
        this.updatedAt = now;
    }
}
