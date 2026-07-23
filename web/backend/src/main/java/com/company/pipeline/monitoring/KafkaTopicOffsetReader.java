package com.company.pipeline.monitoring;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(3));
    }
}
