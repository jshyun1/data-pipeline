package com.company.pipeline.monitoring;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.monitoring.dto.DlqReplayCreateRequest;
import com.company.pipeline.monitoring.dto.DlqReplayResponse;
import com.company.pipeline.user.AppUser;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Service;

@Service
public class DlqReplayService {
    private final DlqReplayRequestRepository repository;
    private final DlqReadService readService;
    private final KafkaBrokerProperties kafkaProperties;

    public DlqReplayService(DlqReplayRequestRepository repository, DlqReadService readService,
            KafkaBrokerProperties kafkaProperties) {
        this.repository = repository; this.readService = readService; this.kafkaProperties = kafkaProperties;
    }

    public DlqReplayResponse request(DlqReplayCreateRequest input, AppUser user) {
        requireUser(user);
        if (input.pipelineId() == null || input.partition() == null || input.offset() == null) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "재처리 대상을 선택하세요.");
        }
        String dlqTopic = "dlq.pipeline-" + input.pipelineId();
        if (repository.existsByDlqTopicAndDlqPartitionAndDlqOffset(dlqTopic, input.partition(), input.offset())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이미 재처리 요청된 DLQ 데이터입니다.");
        }
        ConsumerRecord<byte[], byte[]> record = readService.readRecord(input.pipelineId(), input.partition(), input.offset());
        String originalTopic = requiredHeader(record, "__connect.errors.topic");
        boolean highRisk = isDelete(record.value());
        String status = highRisk ? "PENDING_SECOND_ADMIN" : user.isAdmin() ? "APPROVED_SELF" : "PENDING_ADMIN";
        DlqReplayRequest saved = repository.save(new DlqReplayRequest(input.pipelineId(), dlqTopic,
                input.partition(), input.offset(), originalTopic, highRisk ? "HIGH" : "NORMAL", status,
                input.reason().trim(), user.getUserId()));
        if ("APPROVED_SELF".equals(status)) execute(saved, record, user.getUserId());
        return DlqReplayResponse.from(repository.save(saved));
    }

    public DlqReplayResponse approve(Long requestId, AppUser approver) {
        requireUser(approver);
        if (!approver.isAdmin()) throw new BusinessException(ErrorCode.FORBIDDEN, "관리자만 재처리를 승인할 수 있습니다.");
        DlqReplayRequest request = repository.findById(requestId)
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR, "재처리 요청을 찾을 수 없습니다."));
        if (!request.getStatus().startsWith("PENDING")) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "승인 대기 상태가 아닙니다.");
        }
        if (request.getRequestedBy().equals(approver.getUserId())) {
            throw new BusinessException(ErrorCode.FORBIDDEN, "고위험 요청은 요청자 본인이 승인할 수 없습니다.");
        }
        request.setApprovedBy(approver.getUserId());
        request.setApprovedAt(LocalDateTime.now());
        request.setStatus("APPROVED");
        ConsumerRecord<byte[], byte[]> record = readService.readRecord(
                request.getPipelineId(), request.getDlqPartition(), request.getDlqOffset());
        execute(request, record, approver.getUserId());
        return DlqReplayResponse.from(repository.save(request));
    }

    public List<DlqReplayResponse> history(AppUser user) {
        requireUser(user);
        return repository.findTop100ByOrderByRequestedAtDesc().stream().map(DlqReplayResponse::from).toList();
    }

    private void execute(DlqReplayRequest request, ConsumerRecord<byte[], byte[]> source, String executor) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(request.getOriginalTopic(), source.key(), source.value())).get();
            producer.flush();
            request.setStatus("SUCCEEDED");
            request.setExecutedAt(LocalDateTime.now());
            request.setResultMessage("재처리 완료 (실행자: " + executor + ")");
        } catch (Exception exception) {
            request.setStatus("FAILED");
            request.setExecutedAt(LocalDateTime.now());
            request.setResultMessage("재처리 실패: " + exception.getMessage());
        }
    }

    private boolean isDelete(byte[] value) {
        if (value == null) return false;
        String payload = new String(value, StandardCharsets.UTF_8);
        return payload.matches("(?s).*\\\"op\\\"\\s*:\\s*\\\"d\\\".*")
                || payload.matches("(?s).*\\\"__deleted\\\"\\s*:\\s*\\\"?true\\\"?.*");
    }

    private String requiredHeader(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) throw new BusinessException(ErrorCode.VALIDATION_ERROR, "원본 Topic 정보가 없어 재처리할 수 없습니다.");
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private void requireUser(AppUser user) {
        if (user == null) throw new BusinessException(ErrorCode.UNAUTHORIZED);
    }
}
