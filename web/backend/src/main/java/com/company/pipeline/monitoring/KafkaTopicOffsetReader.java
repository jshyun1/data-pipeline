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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicOffsetReader.class);

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
     * 토픽 각 파티션의 <b>가장 오래된</b> offset 합. retention 으로 앞부분이 삭제되면 0 이 아니다.
     *
     * <p>lag 계산의 하한으로 쓴다. 삭제된 구간은 컨슈머가 읽을 수 없으므로 "미처리"가 아닌데,
     * committed offset 이 그보다 작으면(예: 그룹이 처음 붙어 0 인 상태) 그 차이까지 lag 으로
     * 잡혀 실제보다 크게 나온다(2026-08-25 실측: 실제 10만 건인데 20만으로 표시).
     *
     * @return 조회 실패 시 0 - 하한이 0 이면 보정 전과 같은 값이 되어 안전하다
     */
    public long getEarliestOffsetSum(String topicName) {
        try {
            var description = adminClient.describeTopics(List.of(topicName))
                    .allTopicNames()
                    .get(5, TimeUnit.SECONDS)
                    .get(topicName);
            if (description == null) {
                return 0L;
            }
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            description.partitions().forEach(partition ->
                    request.put(new TopicPartition(topicName, partition.partition()), OffsetSpec.earliest()));
            var offsets = adminClient.listOffsets(request).all().get(5, TimeUnit.SECONDS);
            return offsets.values().stream().mapToLong(info -> info.offset()).sum();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaTopicOffsetReaderException("토픽 earliest offset 조회 중 인터럽트: " + ex.getMessage(), ex);
        } catch (ExecutionException | java.util.concurrent.TimeoutException ex) {
            // 하한을 못 구해도 지표 수집 자체를 멈추지는 않는다 - 보정 전과 같은 값으로 떨어진다.
            log.warn("토픽 earliest offset 조회 실패 - {} : {}", topicName, ex.getMessage());
            return 0L;
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
