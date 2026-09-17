package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.llm.context.SensitiveDataMasker;
import com.company.pipeline.monitoring.KafkaBrokerProperties;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.company.pipeline.pipeline.dto.StuckSinkResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * 멈춘 싱크에서 <b>고칠 수 없는 한 건을 건너뛴다</b>(설계: docs/2026-09-09-cdc-error-policy-dlq.md 5절 ③ · 9-3 B).
 *
 * <p>싱크는 한 건이라도 거부되면 멈춘다(2026-09-17 전환). 원인을 고칠 수 있으면 고치고 재시작하면 되지만, 원본이 깨진 메시지처럼
 * 고칠 수 없는 건은 재시작해도 같은 자리에서 또 멈춘다. 그때 <b>Kafka 의 읽은 위치를 한 칸 넘겨</b> 다음 건부터 이어 가게 한다.
 *
 * <p><b>이것은 데이터 손실이다</b> - 넘긴 건은 타깃에 들어가지 않는다. 그래서:
 * <ul>
 *   <li>넘기기 전에 무엇을 넘기는지 보여 준다(레코드 내용 · 멈춘 원인). 개인정보는 가리고 길이는 자른다.</li>
 *   <li>사유를 받아 명령 이력({@code SKIP_RECORD})에 남긴다 - 누가 · 언제 · 어느 위치를 · 왜 넘겼는지.</li>
 *   <li>싱크가 실제로 멈춰 있을 때만 된다. 돌고 있는데 위치를 옮기면 «읽는 중인 데이터»를 건너뛰게 된다(Kafka 도 거절한다).</li>
 *   <li>화면에서 본 위치와 실제 위치가 같을 때만 넘긴다 - 그 사이 상황이 바뀌었으면 엉뚱한 건을 버리게 된다.</li>
 * </ul>
 */
@Service
@EnableConfigurationProperties(KafkaBrokerProperties.class)
public class SinkStuckRecordService {

    private static final Logger log = LoggerFactory.getLogger(SinkStuckRecordService.class);
    /** Kafka Connect 싱크 커넥터의 컨슈머 그룹 id 규칙(PipelineMetricSnapshotService 와 같다). */
    private static final String GROUP_PREFIX = "connect-";
    private static final int MAX_PREVIEW_CHARS = 2_000;
    private static final int MAX_TRACE_LINES = 12;
    private static final Duration KAFKA_TIMEOUT = Duration.ofSeconds(10);

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final PipelineCommandHistoryRecorder commandHistoryRecorder;
    private final PipelineDeployService pipelineDeployService;
    private final KafkaConnectClient kafkaConnectClient;
    private final KafkaBrokerProperties kafkaBrokerProperties;

    public SinkStuckRecordService(PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineConnectorRepository pipelineConnectorRepository,
            PipelineCommandHistoryRecorder commandHistoryRecorder,
            PipelineDeployService pipelineDeployService,
            KafkaConnectClient kafkaConnectClient,
            KafkaBrokerProperties kafkaBrokerProperties) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.commandHistoryRecorder = commandHistoryRecorder;
        this.pipelineDeployService = pipelineDeployService;
        this.kafkaConnectClient = kafkaConnectClient;
        this.kafkaBrokerProperties = kafkaBrokerProperties;
    }

    /** 멈춘 위치와 그 레코드를 보여 준다(읽기만 한다). */
    public StuckSinkResponse inspect(Long pipelineId) {
        PipelineConnector sink = sinkOf(pipelineId);
        String connectorName = sink.getConnectorName();
        ConnectorStatusResponse status = kafkaConnectClient.getStatus(connectorName);
        boolean stopped = hasFailedTask(status);
        List<String> traces = failureTraces(status);
        String groupId = GROUP_PREFIX + connectorName;

        List<StuckSinkResponse.StuckPosition> positions = new ArrayList<>();
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(KAFKA_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            if (committed.isEmpty()) {
                return new StuckSinkResponse(pipelineId, connectorName, groupId, stopped, traces, List.of());
            }
            try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(committed.keySet(), KAFKA_TIMEOUT);
                committed.forEach((partition, offset) -> {
                    long end = endOffsets.getOrDefault(partition, offset.offset());
                    if (end <= offset.offset()) {
                        return;   // 다 읽은 파티션 - 여기서 멈춘 것이 아니다
                    }
                    positions.add(readPosition(consumer, partition, offset.offset(), end));
                });
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR, "Kafka 조회가 중단되었습니다.");
        } catch (Exception ex) {
            log.warn("멈춘 위치 조회 실패 pipelineId={} group={}: {}", pipelineId, groupId, ex.getMessage());
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                    "Kafka 에서 멈춘 위치를 읽지 못했습니다: " + ex.getMessage());
        }
        return new StuckSinkResponse(pipelineId, connectorName, groupId, stopped, traces, positions);
    }

    /**
     * 그 한 건을 건너뛰고 싱크를 다시 띄운다.
     *
     * @param offset 화면에서 본 위치. 지금 위치와 다르면 거절한다
     */
    public PipelineResponse skip(Long pipelineId, String topic, int partition, long offset, String reason, String actor) {
        if (!StringUtils.hasText(reason)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "건너뛰는 사유를 입력하십시오. 이력에 남습니다.");
        }
        PipelineConnector sink = sinkOf(pipelineId);
        String connectorName = sink.getConnectorName();
        String groupId = GROUP_PREFIX + connectorName;
        ConnectorStatusResponse status = kafkaConnectClient.getStatus(connectorName);
        if (!hasFailedTask(status)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "싱크가 멈춘 상태가 아닙니다(태스크 " + taskStates(status) + "). 멈춘 건을 건너뛰는 기능은 싱크가 실패로 멈췄을 때만 쓸 수 있습니다.");
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        String detail = "%s-%d 오프셋 %d 건너뜀 · 사유: %s".formatted(topic, partition, offset, reason.strip());
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(KAFKA_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            OffsetAndMetadata current = committed.get(topicPartition);
            if (current == null) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "이 싱크가 읽는 위치가 아닙니다: " + topic + "-" + partition);
            }
            if (current.offset() != offset) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "멈춘 위치가 화면에서 본 것과 다릅니다(화면 " + offset + ", 지금 " + current.offset()
                                + "). 다시 조회한 뒤 건너뛰십시오.");
            }
            admin.alterConsumerGroupOffsets(groupId, Map.of(topicPartition, new OffsetAndMetadata(offset + 1)))
                    .all().get(KAFKA_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (BusinessException ex) {
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR, "Kafka 요청이 중단되었습니다.");
        } catch (Exception ex) {
            commandHistoryRecorder.record(pipelineId, "SKIP_RECORD", "FAILED", detail + " · 실패: " + ex.getMessage(), actor);
            log.warn("멈춘 건 건너뛰기 실패 pipelineId={} {}: {}", pipelineId, detail, ex.getMessage());
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                    "읽은 위치를 옮기지 못했습니다(싱크가 아직 돌고 있으면 옮길 수 없습니다): " + ex.getMessage());
        }

        log.info("파이프라인 {} 멈춘 건 건너뜀 - {} (요청자 {})", pipelineId, detail, actor);
        commandHistoryRecorder.record(pipelineId, "SKIP_RECORD", "SUCCESS", detail, actor);
        // 넘긴 뒤 바로 다시 띄운다 - 여기서 멈추면 운영자가 또 한 번 눌러야 하고, 그 사이 상태가 헷갈린다.
        return pipelineDeployService.restart(pipelineId);
    }

    // ---------------------------------------------------------------- 내부

    private StuckSinkResponse.StuckPosition readPosition(KafkaConsumer<byte[], byte[]> consumer,
            TopicPartition partition, long offset, long endOffset) {
        try {
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(3))) {
                if (record.offset() != offset) {
                    continue;
                }
                return new StuckSinkResponse.StuckPosition(partition.topic(), partition.partition(), offset, endOffset,
                        endOffset - offset, Instant.ofEpochMilli(record.timestamp()),
                        preview(record.key()), preview(record.value()), null);
            }
            return position(partition, offset, endOffset, "그 위치의 레코드를 읽지 못했습니다(보존 기간이 지났을 수 있습니다).");
        } catch (Exception ex) {
            return position(partition, offset, endOffset, "레코드 조회 실패: " + ex.getMessage());
        }
    }

    private static StuckSinkResponse.StuckPosition position(TopicPartition partition, long offset, long endOffset,
            String readError) {
        return new StuckSinkResponse.StuckPosition(partition.topic(), partition.partition(), offset, endOffset,
                endOffset - offset, null, null, null, readError);
    }

    /** 원문에는 개인정보가 들어 있을 수 있다 - 가리고 자른다(전송·표시 공통 규칙). */
    private static String preview(byte[] value) {
        if (value == null) {
            return null;
        }
        String text = new String(value, StandardCharsets.UTF_8);
        String masked = SensitiveDataMasker.mask(text);
        return masked.length() <= MAX_PREVIEW_CHARS ? masked
                : masked.substring(0, MAX_PREVIEW_CHARS) + "\n…(이후 생략)";
    }

    private static boolean hasFailedTask(ConnectorStatusResponse status) {
        return status != null && status.tasks() != null
                && status.tasks().stream().anyMatch(task -> "FAILED".equalsIgnoreCase(task.state()));
    }

    private static List<String> taskStates(ConnectorStatusResponse status) {
        return status == null || status.tasks() == null ? List.of()
                : status.tasks().stream().map(ConnectorTaskStatus::state).toList();
    }

    private static List<String> failureTraces(ConnectorStatusResponse status) {
        if (status == null || status.tasks() == null) {
            return List.of();
        }
        return status.tasks().stream()
                .map(ConnectorTaskStatus::trace)
                .filter(StringUtils::hasText)
                .map(SinkStuckRecordService::shortenTrace)
                .toList();
    }

    /** 스택 추적 앞부분과 «Caused by» 줄만. 화면에서 원인을 읽는 데는 이 정도면 된다. */
    private static String shortenTrace(String trace) {
        List<String> lines = List.of(trace.split("\\R"));
        List<String> head = new ArrayList<>(lines.subList(0, Math.min(MAX_TRACE_LINES, lines.size())));
        if (lines.size() > MAX_TRACE_LINES) {
            head.add("\t…");
            lines.stream().skip(MAX_TRACE_LINES)
                    .filter(line -> line.stripLeading().startsWith("Caused by:"))
                    .limit(10)
                    .forEach(head::add);
        }
        return String.join("\n", head);
    }

    private PipelineConnector sinkOf(Long pipelineId) {
        pipelineDefinitionRepository.findById(pipelineId).orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        return pipelineConnectorRepository.findByPipelineIdAndConnectorRole(pipelineId, "SINK")
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "싱크 커넥터가 없습니다. 먼저 «배포»를 하십시오."));
    }

    private AdminClient adminClient() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerProperties.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) KAFKA_TIMEOUT.toMillis());
        return AdminClient.create(config);
    }

    private KafkaConsumer<byte[], byte[]> consumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerProperties.bootstrapServers());
        // 싱크의 그룹에 끼어들면 안 된다 - 읽기 전용 임시 그룹으로 붙는다(커밋하지 않는다).
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "pipeline-api-stuck-read-" + UUID.randomUUID());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(KAFKA_TIMEOUT.toMillis()));
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(KAFKA_TIMEOUT.toMillis()));
        return new KafkaConsumer<>(config);
    }
}
