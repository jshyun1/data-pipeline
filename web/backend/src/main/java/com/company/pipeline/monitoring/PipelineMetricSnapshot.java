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

/** 엔티티 쉘만 존재. MetricCollector/ConnectorStatusCollector가 생기는 증분에서 채워진다. */
@Entity
@Table(name = "pipeline_metric_snapshot")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineMetricSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_id", nullable = false)
    private Long pipelineId;

    @Column(name = "collected_at")
    private LocalDateTime collectedAt;

    @Column(name = "connector_state", length = 30)
    private String connectorState;

    @Column(name = "source_connector_state", length = 30)
    private String sourceConnectorState;

    @Column(name = "sink_connector_state", length = 30)
    private String sinkConnectorState;

    @Column(name = "task_state", length = 30)
    private String taskState;

    @Column(name = "topic_name", length = 200)
    private String topicName;

    @Column(name = "partition_count")
    private Integer partitionCount;

    @Column(name = "end_offset")
    private Long endOffset;

    @Column(name = "committed_offset")
    private Long committedOffset;

    @Column(name = "consumer_lag")
    private Long consumerLag;

    @Column(name = "error_count")
    private Long errorCount = 0L;

    @Column(name = "last_error_message", columnDefinition = "TEXT")
    private String lastErrorMessage;
}
