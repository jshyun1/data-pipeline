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
 * 노드 사이의 실행 순서 1개. V57 마이그레이션.
 *
 * <p>Informatica의 link condition에 해당한다. {@code SUCCESS}는 앞 노드가 성공해야 진행,
 * {@code FAILURE}는 실패했을 때만(보상·알림 경로), {@code ALWAYS}는 성패 무관이다.
 * 컴파일 시 Airflow 의존({@code >>})과 하류 노드의 trigger_rule로 번역된다.
 */
@Entity
@Table(name = "etl_workflow_edge")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlWorkflowEdge {

    public static final String COND_SUCCESS = "SUCCESS";
    public static final String COND_FAILURE = "FAILURE";
    public static final String COND_ALWAYS = "ALWAYS";
    public static final String COND_EXPR = "EXPR";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", nullable = false)
    private Long workflowId;

    @Column(name = "from_node_key", length = 80, nullable = false)
    private String fromNodeKey;

    @Column(name = "to_node_key", length = 80, nullable = false)
    private String toNodeKey;

    @Column(name = "condition_type", length = 20, nullable = false)
    private String conditionType;

    @Column(name = "condition_expr")
    private String conditionExpr;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    public EtlWorkflowEdge(Long workflowId, String fromNodeKey, String toNodeKey,
                           String conditionType, String conditionExpr) {
        this.workflowId = workflowId;
        this.fromNodeKey = fromNodeKey;
        this.toNodeKey = toNodeKey;
        this.conditionType = conditionType == null ? COND_SUCCESS : conditionType;
        this.conditionExpr = conditionExpr;
        this.createdAt = LocalDateTime.now();
    }
}
