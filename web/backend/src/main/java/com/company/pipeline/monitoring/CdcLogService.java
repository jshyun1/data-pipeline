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
        List<PipelineDefinition> pipelines = monitoredPipelines();
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
            String previousSourceState = baseline != null ? sourceState(pipeline, baseline) : null;
            String previousSinkState = baseline != null ? sinkState(baseline) : null;
            LocalDate cumulativeDate = null;
            long dailyProcessedCount = 0L;

            List<Map.Entry<LocalDateTime, PipelineMetricSnapshot>> buckets =
                    new ArrayList<>(pipelineEntry.getValue().entrySet());
            for (int index = 0; index < buckets.size(); index++) {
                Map.Entry<LocalDateTime, PipelineMetricSnapshot> bucket = buckets.get(index);
                PipelineMetricSnapshot snapshot = bucket.getValue();
                LocalDate bucketDate = bucket.getKey().toLocalDate();
                if (!bucketDate.equals(cumulativeDate)) {
                    cumulativeDate = bucketDate;
                    dailyProcessedCount = 0L;
                }
                long committed = valueOrZero(snapshot.getCommittedOffset());
                long processed = previousCommitted == null
                        ? 0L : Math.max(0L, committed - previousCommitted);
                dailyProcessedCount += processed;
                long lag = valueOrZero(snapshot.getConsumerLag());
                String sourceState = sourceState(pipeline, snapshot);
                String sinkState = sinkState(snapshot);
                boolean stateChanged = !Objects.equals(previousSourceState, sourceState)
                        || !Objects.equals(previousSinkState, sinkState);
                String status = status(sourceState, sinkState, lag, snapshot.getErrorCount());
                boolean latestBucket = index == buckets.size() - 1;

                // 변화 없는 정상 heartbeat를 전부 노출하면 로그가 지나치게 커진다.
                if (processed > 0L || lag > 0L || stateChanged || !"SUCCESS".equals(status) || latestBucket) {
                    result.add(toProcessingResponse(
                            pipeline, bucket.getKey(), snapshot, processed, dailyProcessedCount,
                            lag, sourceState, sinkState, status));
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
        List<PipelineDefinition> pipelines = monitoredPipelines();
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

    /**
     * 이 화면이 다루는 파이프라인 전체.
     *
     * <p>예전에는 TABLE_CDC만 골랐다 - 로그 파이프라인이 나중에 같은 테이블/같은 화면을
     * 쓰게 됐는데 필터가 남아서, 처리 이력과 오류·상태 이력 양쪽에서 통째로 빠져 있었다.
     * 두 탭 모두 이 메서드를 쓰므로 여기 한 곳만 고치면 된다.
     */
    private List<PipelineDefinition> monitoredPipelines() {
        return pipelineRepository.findAll();
    }

    private boolean isLogFile(PipelineDefinition pipeline) {
        return "LOG_FILE".equalsIgnoreCase(pipeline.getPipelineType());
    }

    private CdcProcessingLogResponse toProcessingResponse(
            PipelineDefinition pipeline,
            LocalDateTime occurredAt,
            PipelineMetricSnapshot snapshot,
            long processed,
            long dailyProcessedCount,
            long lag,
            String sourceState,
            String sinkState,
            String status) {
        return new CdcProcessingLogResponse(
                pipeline.getId(),
                pipeline.getName(),
                // 로그 파이프라인의 소스는 filebeat라 DB 경로가 없다 - "null · null.null"이
                // 찍히지 않도록 에이전트 이름으로 표시한다(파이프라인 목록 화면과 동일).
                isLogFile(pipeline) ? "Filebeat"
                        : path(pipeline.getSourceDbType(), pipeline.getSourceSchema(), pipeline.getSourceTable()),
                path(pipeline.getTargetDbType(), pipeline.getTargetSchema(), pipeline.getTargetTable()),
                snapshot.getTopicName(),
                occurredAt,
                processed,
                valueOrZero(snapshot.getCommittedOffset()),
                dailyProcessedCount,
                lag,
                sourceState,
                sinkState,
                status,
                message(status, lag, snapshot.getLastErrorMessage()));
    }

    private String path(Object dbType, String schema, String table) {
        return dbType + " · " + schema + "." + table;
    }

    /**
     * 로그 파이프라인은 소스가 filebeat여서 Kafka Connect 소스 커넥터가 없다 - 없는
     * 것을 UNKNOWN으로 찍으면 "상태를 못 읽었다"처럼 보이므로 null로 두고 화면에서
     * "-"로 표시한다. status() 판정도 null은 실패로 보지 않는다.
     */
    private String sourceState(PipelineDefinition pipeline, PipelineMetricSnapshot snapshot) {
        if (isLogFile(pipeline)) {
            return null;
        }
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
