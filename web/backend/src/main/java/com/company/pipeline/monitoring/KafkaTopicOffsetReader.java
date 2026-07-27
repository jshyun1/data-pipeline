package com.company.pipeline.monitoring;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Kafka Connect 싱크 커넥터의 컨슈머 그룹이 특정 topic에 대해 실제로 얼마나 커밋했는지
 * (committed offset 합계)를 조회한다. 파이프라인이 "적재한" 레코드 수를 근사하는 데 쓴다
 * (KafkaBrokerHealthChecker와 같은 AdminClient 재사용 패턴).
 */
@Component
@EnableConfigurationProperties(KafkaBrokerProperties.class)
public class KafkaTopicOffsetReader {

    public record TopicEndOffsetSummary(int partitionCount, long endOffset) {
    }

    private final AdminClient adminClient;

    public KafkaTopicOffsetReader(KafkaBrokerProperties properties) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        this.adminClient = AdminClient.create(config);
    }

    /** consumerGroupId가 topicName에 대해 커밋한 offset들의 합. 그룹/토픽이 아직 없으면 0. */
    public long getCommittedOffsetSum(String consumerGroupId, String topicName) {
        try {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                    adminClient.listConsumerGroupOffsets(consumerGroupId)
                            .partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(entry -> entry.getKey().topic().equals(topicName))
                    .mapToLong(entry -> entry.getValue().offset())
                    .sum();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaTopicOffsetReaderException("오프셋 조회 중 인터럽트: " + ex.getMessage(), ex);
        } catch (ExecutionException ex) {
            throw new KafkaTopicOffsetReaderException("오프셋 조회 실패: " + ex.getMessage(), ex);
        } catch (java.util.concurrent.TimeoutException ex) {
            throw new KafkaTopicOffsetReaderException("오프셋 조회 타임아웃: " + ex.getMessage(), ex);
        }
    }

    /**
     * 토픽 각 파티션의 최신 offset 합과 파티션 수. committed offset과 비교하면 Sink가
     * 아직 소비하지 못한 레코드 수(consumer lag)를 구할 수 있다.
     */
    public TopicEndOffsetSummary getEndOffsetSummary(String topicName) {
        try {
            var description = adminClient.describeTopics(List.of(topicName))
                    .allTopicNames()
                    .get(5, TimeUnit.SECONDS)
                    .get(topicName);
            if (description == null) {
                return new TopicEndOffsetSummary(0, 0L);
            }

            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            description.partitions().forEach(partition ->
                    request.put(new TopicPartition(topicName, partition.partition()), OffsetSpec.latest()));
            var offsets = adminClient.listOffsets(request).all().get(5, TimeUnit.SECONDS);
            long endOffset = offsets.values().stream().mapToLong(info -> info.offset()).sum();
            return new TopicEndOffsetSummary(description.partitions().size(), endOffset);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaTopicOffsetReaderException("토픽 end offset 조회 중 인터럽트: " + ex.getMessage(), ex);
        } catch (ExecutionException ex) {
            throw new KafkaTopicOffsetReaderException("토픽 end offset 조회 실패: " + ex.getMessage(), ex);
        } catch (java.util.concurrent.TimeoutException ex) {
            throw new KafkaTopicOffsetReaderException("토픽 end offset 조회 타임아웃: " + ex.getMessage(), ex);
        }
    }

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(3));
    }
}
