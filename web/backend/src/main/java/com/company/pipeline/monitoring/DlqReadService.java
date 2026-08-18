package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.DlqRecordDetailResponse;
import com.company.pipeline.monitoring.dto.DlqRecordResponse;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

@Service
@EnableConfigurationProperties(KafkaBrokerProperties.class)
public class DlqReadService {
    private static final int MAX_RECORDS = 200;
    private static final int MAX_PAYLOAD_BYTES = 1_048_576;
    private final KafkaBrokerProperties properties;
    private final PipelineDefinitionRepository pipelineRepository;

    public DlqReadService(KafkaBrokerProperties properties, PipelineDefinitionRepository pipelineRepository) {
        this.properties = properties;
        this.pipelineRepository = pipelineRepository;
    }

    public List<DlqRecordResponse> list(Long pipelineId, Instant from, Instant to) {
        PipelineDefinition pipeline = findPipeline(pipelineId);
        String topic = topic(pipelineId);
        List<DlqRecordResponse> result = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
            if (infos == null || infos.isEmpty()) return result;
            List<TopicPartition> partitions = infos.stream().map(info -> new TopicPartition(topic, info.partition())).toList();
            consumer.assign(partitions);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> starts = consumer.offsetsForTimes(
                    partitions.stream().collect(java.util.stream.Collectors.toMap(value -> value, value -> from.toEpochMilli())));
            for (TopicPartition partition : partitions) {
                var start = starts.get(partition);
                if (start == null) consumer.seekToEnd(List.of(partition)); else consumer.seek(partition, start.offset());
            }
            int emptyPolls = 0;
            while (result.size() < MAX_RECORDS && emptyPolls < 2) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) { emptyPolls++; continue; }
                emptyPolls = 0;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.timestamp() > to.toEpochMilli()) continue;
                    result.add(summary(pipeline, record));
                    if (result.size() >= MAX_RECORDS) break;
                }
            }
        } catch (Exception ignored) {
            return List.of();
        }
        return result.stream().sorted(Comparator.comparing(DlqRecordResponse::occurredAt).reversed()).toList();
    }

    public DlqRecordDetailResponse detail(Long pipelineId, int partition, long offset) {
        findPipeline(pipelineId);
        String topic = topic(pipelineId);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(2))) {
                if (record.offset() == offset) return detail(pipelineId, record);
            }
        }
        throw new IllegalArgumentException("DLQ 레코드를 찾을 수 없습니다.");
    }

    ConsumerRecord<byte[], byte[]> readRecord(Long pipelineId, int partition, long offset) {
        findPipeline(pipelineId);
        String topic = topic(pipelineId);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(2))) {
                if (record.offset() == offset) return record;
            }
        }
        throw new IllegalArgumentException("DLQ 레코드를 찾을 수 없습니다.");
    }

    private KafkaConsumer<byte[], byte[]> consumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "pipeline-api-dlq-read-" + UUID.randomUUID());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return new KafkaConsumer<>(config);
    }

    private DlqRecordResponse summary(PipelineDefinition pipeline, ConsumerRecord<byte[], byte[]> record) {
        return new DlqRecordResponse(pipeline.getId(), pipeline.getName(), record.topic(), record.partition(), record.offset(),
                Instant.ofEpochMilli(record.timestamp()), header(record, "__connect.errors.connector.name"),
                header(record, "__connect.errors.exception.class.name"), header(record, "__connect.errors.exception.message"));
    }

    private DlqRecordDetailResponse detail(Long pipelineId, ConsumerRecord<byte[], byte[]> record) {
        return new DlqRecordDetailResponse(pipelineId, record.topic(), record.partition(), record.offset(),
                Instant.ofEpochMilli(record.timestamp()), header(record, "__connect.errors.connector.name"),
                header(record, "__connect.errors.exception.class.name"), header(record, "__connect.errors.exception.message"),
                text(record.key()), text(record.value()));
    }

    private String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : text(header.value());
    }

    private String text(byte[] value) {
        if (value == null) return null;
        int length = Math.min(value.length, MAX_PAYLOAD_BYTES);
        String text = new String(value, 0, length, StandardCharsets.UTF_8);
        return value.length > length ? text + "\n…(1MB 이후 생략)" : text;
    }

    private PipelineDefinition findPipeline(Long id) {
        return pipelineRepository.findById(id).orElseThrow(() -> new PipelineNotFoundException(id));
    }

    private String topic(Long pipelineId) { return "dlq.pipeline-" + pipelineId; }
}
