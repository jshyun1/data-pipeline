package com.company.pipeline.connector;

import com.company.pipeline.monitoring.KafkaBrokerProperties;
import com.company.pipeline.pipeline.PipelineDefinition;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 파이프라인을 삭제해도 그 파이프라인이 쓰던 Kafka 토픽(CDC 데이터 토픽, Oracle 소스면
 * Debezium 스키마 이력 토픽)은 커넥터 삭제만으로는 안 없어진다 - 토픽은 Kafka Connect가
 * 아니라 브로커 자체의 리소스라서 별개로 지워야 한다. 안 지우면 아무도 안 쓰는 토픽이
 * 계속 쌓여서 브로커/컨트롤러 메타데이터 부담이 커지고, 운영 중 "이게 살아있는 토픽인지"
 * 헷갈리는 문제가 생긴다(실제로 겪음).
 *
 * PostgresReplicationCleanupService와 같은 원칙 - 실패해도 예외를 던지지 않고 조용히
 * 넘어간다(파이프라인 삭제 자체를 막을 이유가 없음).
 */
@Component
@EnableConfigurationProperties(KafkaBrokerProperties.class)
public class KafkaTopicCleanupService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicCleanupService.class);
    private static final String ORACLE_CONNECTOR_CLASS = "io.debezium.connector.oracle.OracleConnector";
    private static final int MAX_ATTEMPTS = 4;
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(3);

    private final AdminClient adminClient;

    public KafkaTopicCleanupService(KafkaBrokerProperties properties) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        this.adminClient = AdminClient.create(config);
    }

    /**
     * @param pipeline 삭제 중인 파이프라인
     * @param connectors 그 파이프라인의 커넥터들(삭제 전에 조회해둔 것)
     * @param allPipelines 삭제 시점의 전체 파이프라인 목록(자기 자신 포함) - 같은 토픽을
     *                      다른 파이프라인이 여전히 쓰고 있으면 지우지 않기 위한 안전장치.
     */
    public void cleanup(PipelineDefinition pipeline, List<PipelineConnector> connectors,
            List<PipelineDefinition> allPipelines) {
        Set<String> topics = new LinkedHashSet<>();

        String dataTopic = resolveDataTopic(pipeline);
        if (dataTopic != null) {
            boolean stillUsedElsewhere = allPipelines.stream()
                    .filter(other -> !other.getId().equals(pipeline.getId()))
                    .anyMatch(other -> dataTopic.equals(resolveDataTopic(other)));
            if (stillUsedElsewhere) {
                log.info("파이프라인 {} 삭제하지만 토픽 {}는 다른 파이프라인이 여전히 사용 중이라 남겨둠",
                        pipeline.getId(), dataTopic);
            } else {
                topics.add(dataTopic);
            }
        }

        // 스키마 이력 토픽은 커넥터 이름 기반이라 다른 파이프라인과 겹칠 일이 없다.
        connectors.stream()
                .filter(c -> "SOURCE".equals(c.getConnectorRole()))
                .filter(c -> ORACLE_CONNECTOR_CLASS.equals(c.getConnectorClass()))
                .forEach(c -> topics.add("schema-changes." + c.getConnectorName()));

        if (topics.isEmpty()) {
            return;
        }

        // Kafka Connect의 커넥터 DELETE는 응답을 반환한 뒤에도 내부 태스크의 컨슈머
        // 스레드가 완전히 멈추기까지 async 꼬리가 남는다 - 그 사이에 컨슈머가 poll을
        // 한 번 더 하면 클라이언트 기본값(allow.auto.create.topics=true) 때문에 방금
        // 지운 토픽이 그대로 재생성돼버리는 걸 실제로 확인했다(최대 5초 넘게 걸리는
        // 경우도 있었음). 재생성이 멈출 때까지 짧은 간격으로 몇 차례 재확인/재삭제한다.
        Set<String> remaining = topics;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS && !remaining.isEmpty(); attempt++) {
            deleteTopicsQuietly(remaining, pipeline.getId());
            try {
                Thread.sleep(RETRY_INTERVAL.toMillis());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
            remaining = filterExistingTopics(topics);
            if (!remaining.isEmpty()) {
                log.info("파이프라인 {} 삭제 후 {}회차 확인 - 컨슈머가 재생성한 것으로 보이는 토픽 남음: {}",
                        pipeline.getId(), attempt, remaining);
            }
        }
        if (!remaining.isEmpty()) {
            log.warn("파이프라인 {} 삭제 후에도 토픽이 계속 재생성되어 정리 못함(수동 확인 필요): {}",
                    pipeline.getId(), remaining);
        }
    }

    private Set<String> filterExistingTopics(Set<String> topics) {
        try {
            Set<String> existing = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
            Set<String> result = new LinkedHashSet<>(topics);
            result.retainAll(existing);
            return result;
        } catch (Exception ex) {
            log.warn("Kafka 토픽 목록 조회 실패, 재정리 여부 확인 못 함: {}", ex.getMessage());
            return Set.of();
        }
    }

    private void deleteTopicsQuietly(Set<String> topics, Long pipelineId) {
        try {
            adminClient.deleteTopics(topics).all().get(10, TimeUnit.SECONDS);
            log.info("파이프라인 {} 삭제에 따라 Kafka 토픽 정리: {}", pipelineId, topics);
        } catch (Exception ex) {
            // 토픽이 이미 없거나 브로커가 일시적으로 응답 안 해도 파이프라인 삭제
            // 자체는 막지 않는다.
            log.warn("Kafka 토픽 정리 실패(다음에 수동 정리 필요할 수 있음): {} - {}", topics, ex.getMessage());
        }
    }

    private String resolveDataTopic(PipelineDefinition pipeline) {
        if ("LOG_FILE".equals(pipeline.getPipelineType())) {
            return StringUtils.hasText(pipeline.getTopicName()) ? pipeline.getTopicName() : null;
        }
        if (pipeline.getSourceDbType() != null
                && StringUtils.hasText(pipeline.getSourceSchema())
                && StringUtils.hasText(pipeline.getSourceTable())
                && StringUtils.hasText(pipeline.getTopicName())) {
            return ConnectorNaming.topicName(pipeline.getTopicName(), pipeline.getSourceDbType(),
                    pipeline.getSourceSchema(), pipeline.getSourceTable());
        }
        return null;
    }

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(3));
    }
}
