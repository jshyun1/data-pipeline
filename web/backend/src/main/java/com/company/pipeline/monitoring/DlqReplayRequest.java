package com.company.pipeline.monitoring;

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
import lombok.Setter;

@Entity
@Table(name = "dlq_replay_request")
@Getter @Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DlqReplayRequest {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(name="pipeline_id", nullable=false) private Long pipelineId;
    @Column(name="dlq_topic", nullable=false) private String dlqTopic;
    @Column(name="dlq_partition", nullable=false) private Integer dlqPartition;
    @Column(name="dlq_offset", nullable=false) private Long dlqOffset;
    @Column(name="original_topic", nullable=false) private String originalTopic;
    @Column(name="risk_level", nullable=false) private String riskLevel;
    @Column(nullable=false) private String status;
    @Column(nullable=false, columnDefinition="TEXT") private String reason;
    @Column(name="requested_by", nullable=false) private String requestedBy;
    @Column(name="requested_at", nullable=false) private LocalDateTime requestedAt;
    @Column(name="approved_by") private String approvedBy;
    @Column(name="approved_at") private LocalDateTime approvedAt;
    @Column(name="executed_at") private LocalDateTime executedAt;
    @Column(name="result_message", columnDefinition="TEXT") private String resultMessage;

    public DlqReplayRequest(Long pipelineId, String dlqTopic, int partition, long offset, String originalTopic,
            String riskLevel, String status, String reason, String requestedBy) {
        this.pipelineId=pipelineId; this.dlqTopic=dlqTopic; this.dlqPartition=partition; this.dlqOffset=offset;
        this.originalTopic=originalTopic; this.riskLevel=riskLevel; this.status=status; this.reason=reason;
        this.requestedBy=requestedBy; this.requestedAt=LocalDateTime.now();
    }
}
