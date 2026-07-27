package com.company.pipeline.monitoring;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.monitoring.dto.CdcEventLogResponse;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 계속 실행되는 CDC를 배치 실행 이력이 아닌 처리량/Lag 관점의 운영 로그로 변환한다.
 * 20초 스냅샷은 화면에서 읽기 쉽도록 파이프라인별 1분 단위로 묶는다.
 */
@Service
@Transactional(readOnly = true)
public class CdcLogService {

    private static final long MAX_RANGE_DAYS = 31L;
    private static final long DELAYED_LAG_THRESHOLD = 1_000L;

    private final PipelineDefinitionRepository pipelineRepository;
    private final PipelineMetricSnapshotRepository snapshotRepository;
    private final PipelineCommandHistoryRepository commandHistoryRepository;

    public CdcLogService(
            PipelineDefinitionRepository pipelineRepository,
            PipelineMetricSnapshotRepository snapshotRepository,
            PipelineCommandHistoryRepository commandHistoryRepository) {
        this.pipelineRepository = pipelineRepository;
        this.snapshotRepository = snapshotRepository;
        this.commandHistoryRepository = commandHistoryRepository;
    }

    public List<CdcProcessingLogResponse> processingLogs(LocalDate from, LocalDate to) {
        DateRange range = validateRange(from, to);
        List<PipelineDefinition> pipelines = cdcPipelines();
        if (pipelines.isEmpty()) {
            return List.of();
        }

        Map<Long, PipelineDefinition> pipelineById = new HashMap<>();
        pipelines.forEach(pipeline -> pipelineById.put(pipeline.getId(), pipeline));
        List<Long> pipelineIds = pipelines.stream().map(PipelineDefinition::getId).toList();
        List<PipelineMetricSnapshot> snapshots =
                snapshotRepository.findByPipelineIdInAndCollectedAtBetweenOrderByCollectedAtAsc(
                        pipelineIds, range.from(), range.to());

        Map<Long, LinkedHashMap<LocalDateTime, PipelineMetricSnapshot>> minuteBuckets = new LinkedHashMap<>();
        for (PipelineMetricSnapshot snapshot : snapshots) {
            LocalDateTime minute = snapshot.getCollectedAt().truncatedTo(ChronoUnit.MINUTES);
            minuteBuckets
                    .computeIfAbsent(snapshot.getPipelineId(), ignored -> new LinkedHashMap<>())
                    .put(minute, snapshot);
        }

        List<CdcProcessingLogResponse> result = new ArrayList<>();
        for (Map.Entry<Long, LinkedHashMap<LocalDateTime, PipelineMetricSnapshot>> pipelineEntry
                : minuteBuckets.entrySet()) {
            Long pipelineId = pipelineEntry.getKey();
            PipelineDefinition pipeline = pipelineById.get(pipelineId);
            PipelineMetricSnapshot baseline = snapshotRepository
                    .findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(pipelineId, range.from())
                    .orElse(null);
            Long previousCommitted = baseline != null ? baseline.getCommittedOffset() : null;
            String previousSourceState = baseline != null ? sourceState(baseline) : null;
            String previousSinkState = baseline != null ? sinkState(baseline) : null;

            List<Map.Entry<LocalDateTime, PipelineMetricSnapshot>> buckets =
                    new ArrayList<>(pipelineEntry.getValue().entrySet());
            for (int index = 0; index < buckets.size(); index++) {
                Map.Entry<LocalDateTime, PipelineMetricSnapshot> bucket = buckets.get(index);
                PipelineMetricSnapshot snapshot = bucket.getValue();
                long committed = valueOrZero(snapshot.getCommittedOffset());
                long processed = previousCommitted == null
                        ? 0L : Math.max(0L, committed - previousCommitted);
                long lag = valueOrZero(snapshot.getConsumerLag());
                String sourceState = sourceState(snapshot);
                String sinkState = sinkState(snapshot);
                boolean stateChanged = !Objects.equals(previousSourceState, sourceState)
                        || !Objects.equals(previousSinkState, sinkState);
                String status = status(sourceState, sinkState, lag, snapshot.getErrorCount());
                boolean latestBucket = index == buckets.size() - 1;

                // 변화 없는 정상 heartbeat를 전부 노출하면 로그가 지나치게 커진다.
                if (processed > 0L || lag > 0L || stateChanged || !"SUCCESS".equals(status) || latestBucket) {
                    result.add(toProcessingResponse(
                            pipeline, bucket.getKey(), snapshot, processed, lag, sourceState, sinkState, status));
                }
                previousCommitted = committed;
                previousSourceState = sourceState;
                previousSinkState = sinkState;
            }
        }
        result.sort(Comparator.comparing(CdcProcessingLogResponse::occurredAt).reversed());
        return result;
    }

    public List<CdcEventLogResponse> eventLogs(LocalDate from, LocalDate to) {
        DateRange range = validateRange(from, to);
        List<PipelineDefinition> pipelines = cdcPipelines();
        if (pipelines.isEmpty()) {
            return List.of();
        }
        Map<Long, String> names = new HashMap<>();
        pipelines.forEach(pipeline -> names.put(pipeline.getId(), pipeline.getName()));
        return commandHistoryRepository
                .findByPipelineIdInAndRequestedAtBetweenOrderByRequestedAtDesc(
                        pipelines.stream().map(PipelineDefinition::getId).toList(), range.from(), range.to())
                .stream()
                .map(history -> new CdcEventLogResponse(
                        history.getId(),
                        history.getPipelineId(),
                        names.getOrDefault(history.getPipelineId(), "(삭제된 파이프라인)"),
                        history.getCommand(),
                        history.getResult(),
                        history.getMessage(),
                        history.getRequestedBy(),
                        history.getRequestedAt(),
                        history.getCompletedAt()))
                .toList();
    }

    private List<PipelineDefinition> cdcPipelines() {
        return pipelineRepository.findAll().stream()
                .filter(pipeline -> "TABLE_CDC".equalsIgnoreCase(pipeline.getPipelineType()))
                .toList();
    }

    private CdcProcessingLogResponse toProcessingResponse(
            PipelineDefinition pipeline,
            LocalDateTime occurredAt,
            PipelineMetricSnapshot snapshot,
            long processed,
            long lag,
            String sourceState,
            String sinkState,
            String status) {
        return new CdcProcessingLogResponse(
                pipeline.getId(),
                pipeline.getName(),
                path(pipeline.getSourceDbType(), pipeline.getSourceSchema(), pipeline.getSourceTable()),
                path(pipeline.getTargetDbType(), pipeline.getTargetSchema(), pipeline.getTargetTable()),
                snapshot.getTopicName(),
                occurredAt,
                processed,
                valueOrZero(snapshot.getCommittedOffset()),
                lag,
                sourceState,
                sinkState,
                status,
                message(status, lag, snapshot.getLastErrorMessage()));
    }

    private String path(Object dbType, String schema, String table) {
        return dbType + " · " + schema + "." + table;
    }

    private String sourceState(PipelineMetricSnapshot snapshot) {
        return snapshot.getSourceConnectorState() != null
                ? snapshot.getSourceConnectorState() : "UNKNOWN";
    }

    private String sinkState(PipelineMetricSnapshot snapshot) {
        if (snapshot.getSinkConnectorState() != null) {
            return snapshot.getSinkConnectorState();
        }
        return snapshot.getConnectorState() != null ? snapshot.getConnectorState() : "UNKNOWN";
    }

    private String status(String sourceState, String sinkState, long lag, Long errorCount) {
        if (isFailed(sourceState) || isFailed(sinkState) || valueOrZero(errorCount) > 0L) {
            return "FAILED";
        }
        if ("STOPPED".equalsIgnoreCase(sinkState) || "PAUSED".equalsIgnoreCase(sinkState)) {
            return "STOPPED";
        }
        if (lag >= DELAYED_LAG_THRESHOLD) {
            return "DELAYED";
        }
        return "SUCCESS";
    }

    private boolean isFailed(String state) {
        return "FAILED".equalsIgnoreCase(state) || "ERROR".equalsIgnoreCase(state);
    }

    private String message(String status, long lag, String errorMessage) {
        if (errorMessage != null && !errorMessage.isBlank()) {
            return errorMessage;
        }
        return switch (status) {
            case "FAILED" -> "Source 또는 Sink 커넥터 상태를 확인하세요.";
            case "STOPPED" -> "Sink가 중지되어 변경 데이터가 Kafka에 대기할 수 있습니다.";
            case "DELAYED" -> "미처리 데이터 " + lag + "건이 대기 중입니다.";
            default -> null;
        };
    }

    private DateRange validateRange(LocalDate from, LocalDate to) {
        if (from == null || to == null || from.isAfter(to)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "조회 시작일과 종료일을 확인하세요.");
        }
        if (ChronoUnit.DAYS.between(from, to) > MAX_RANGE_DAYS) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "CDC 로그는 최대 31일까지 조회할 수 있습니다.");
        }
        return new DateRange(from.atStartOfDay(), to.plusDays(1).atStartOfDay().minusNanos(1));
    }

    private long valueOrZero(Long value) {
        return value != null ? value : 0L;
    }

    private record DateRange(LocalDateTime from, LocalDateTime to) {
    }
}
