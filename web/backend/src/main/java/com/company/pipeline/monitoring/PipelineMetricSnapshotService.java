package com.company.pipeline.monitoring;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import java.time.LocalDateTime;
import java.util.Map;
import org.springframework.stereotype.Service;

/**
 * 파이프라인의 실제 데이터 적재량을 근사하기 위한 스냅샷을 기록한다. 싱크 커넥터의
 * 컨슈머 그룹이 소스 topic에 대해 커밋한 offset 합계를 "지금까지 적재(consume)한
 * 레코드 수"로 취급한다 - Airflow 쪽에서 두 시점의 스냅샷 차이(delta)로 "이번 실행에서
 * 새로 적재된 건수"를 계산한다 (verify_target_db_landing).
 */
@Service
public class PipelineMetricSnapshotService {

    private static final String SINK_ROLE = "SINK";

    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final KafkaTopicOffsetReader kafkaTopicOffsetReader;
    private final PipelineMetricSnapshotRepository pipelineMetricSnapshotRepository;

    public PipelineMetricSnapshotService(
            PipelineConnectorRepository pipelineConnectorRepository,
            KafkaConnectClient kafkaConnectClient,
            KafkaTopicOffsetReader kafkaTopicOffsetReader,
            PipelineMetricSnapshotRepository pipelineMetricSnapshotRepository) {
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.kafkaTopicOffsetReader = kafkaTopicOffsetReader;
        this.pipelineMetricSnapshotRepository = pipelineMetricSnapshotRepository;
    }

    public PipelineMetricSnapshot recordSnapshot(Long pipelineId) {
        PipelineConnector sink = pipelineConnectorRepository
                .findByPipelineIdAndConnectorRole(pipelineId, SINK_ROLE)
                .orElseThrow(() -> new BusinessException(
                        ErrorCode.PIPELINE_NOT_FOUND, "파이프라인 " + pipelineId + "에 싱크 커넥터가 없습니다."));
        PipelineConnector source = pipelineConnectorRepository
                .findByPipelineIdAndConnectorRole(pipelineId, "SOURCE")
                .orElse(null);

        Map<String, Object> config = kafkaConnectClient.getConfig(sink.getConnectorName());
        String topicName = extractTopicName(config, sink.getConnectorName());
        // Kafka Connect 싱크 커넥터의 컨슈머 그룹 id 기본값
        String consumerGroupId = "connect-" + sink.getConnectorName();

        long committedOffset = kafkaTopicOffsetReader.getCommittedOffsetSum(consumerGroupId, topicName);
        KafkaTopicOffsetReader.TopicEndOffsetSummary endOffsetSummary =
                kafkaTopicOffsetReader.getEndOffsetSummary(topicName);
        // retention 으로 앞부분이 지워졌으면 그 구간은 컨슈머가 읽을 수 없어 "미처리"가 아니다.
        // 하한을 안 두면 committed 가 그보다 작을 때(그룹이 처음 붙어 0 인 경우 등) 삭제된
        // 구간까지 lag 으로 잡혀 실제보다 크게 나오고, 임계값을 넘겨 "지연"으로 오탐한다
        // (2026-08-25 실측: 실제 10만 건 스냅샷인데 lag 20만으로 표시).
        long earliestOffset = kafkaTopicOffsetReader.getEarliestOffsetSum(topicName);
        long readableFrom = Math.max(committedOffset, earliestOffset);

        PipelineMetricSnapshot snapshot = new PipelineMetricSnapshot();
        snapshot.setPipelineId(pipelineId);
        snapshot.setCollectedAt(LocalDateTime.now());
        snapshot.setConnectorState(sink.getStatus());
        snapshot.setSourceConnectorState(source != null ? source.getStatus() : null);
        snapshot.setSinkConnectorState(sink.getStatus());
        snapshot.setTopicName(topicName);
        snapshot.setCommittedOffset(committedOffset);
        snapshot.setEarliestOffset(earliestOffset);
        snapshot.setPartitionCount(endOffsetSummary.partitionCount());
        snapshot.setEndOffset(endOffsetSummary.endOffset());
        snapshot.setConsumerLag(Math.max(0L, endOffsetSummary.endOffset() - readableFrom));
        return pipelineMetricSnapshotRepository.save(snapshot);
    }

    private String extractTopicName(Map<String, Object> config, String connectorName) {
        Object topics = config.get("topics");
        if (topics == null || !org.springframework.util.StringUtils.hasText(topics.toString())) {
            throw new BusinessException(
                    ErrorCode.KAFKA_CONNECT_ERROR,
                    "커넥터 " + connectorName + "의 topics 설정을 찾을 수 없습니다.");
        }
        // 이 프로젝트의 싱크 커넥터는 항상 topic 1개만 구독하도록 렌더링되지만,
        // 콤마로 여러 개가 들어올 가능성까지 방어적으로 처리한다.
        return topics.toString().split(",")[0].trim();
    }
}
