package com.company.pipeline.monitoring;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.monitoring.dto.CdcEventLogResponse;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineMetadataArchive;
import com.company.pipeline.pipeline.PipelineMetadataArchiveRepository;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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
    private final PipelineMetadataArchiveRepository metadataArchiveRepository;

    public CdcLogService(
            PipelineDefinitionRepository pipelineRepository,
            PipelineMetricSnapshotRepository snapshotRepository,
            PipelineCommandHistoryRepository commandHistoryRepository,
            PipelineMetadataArchiveRepository metadataArchiveRepository) {
        this.pipelineRepository = pipelineRepository;
        this.snapshotRepository = snapshotRepository;
        this.commandHistoryRepository = commandHistoryRepository;
        this.metadataArchiveRepository = metadataArchiveRepository;
    }

    /** 화면 렌더에 필요한 파이프라인 표시값(현존/삭제 공통). 삭제분은 아카이브에서 온다. */
    private record PipelineLogMeta(long id, String name, boolean logFile, String sourcePath, String targetPath) {
    }

    public List<CdcProcessingLogResponse> processingLogs(LocalDate from, LocalDate to) {
        DateRange range = validateRange(from, to);
        Map<Long, PipelineLogMeta> metaById = logMetaByPipelineId();
        if (metaById.isEmpty()) {
            return List.of();
        }
        List<Long> pipelineIds = new ArrayList<>(metaById.keySet());
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
            PipelineLogMeta meta = metaById.get(pipelineId);
            if (meta == null) {
                continue;   // 메타 없는 고아 스냅샷(보존창 밖 삭제분)은 건너뛴다
            }
            PipelineMetricSnapshot baseline = snapshotRepository
                    .findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(pipelineId, range.from())
                    .orElse(null);
            Long previousCommitted = baseline != null ? baseline.getCommittedOffset() : null;
            String previousSourceState = baseline != null ? sourceState(meta.logFile(), baseline) : null;
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
                String sourceState = sourceState(meta.logFile(), snapshot);
                String sinkState = sinkState(snapshot);
                boolean stateChanged = !Objects.equals(previousSourceState, sourceState)
                        || !Objects.equals(previousSinkState, sinkState);
                String status = status(sourceState, sinkState, lag, snapshot.getErrorCount());
                boolean latestBucket = index == buckets.size() - 1;

                // 변화 없는 정상 heartbeat를 전부 노출하면 로그가 지나치게 커진다.
                if (processed > 0L || lag > 0L || stateChanged || !"SUCCESS".equals(status) || latestBucket) {
                    result.add(toProcessingResponse(
                            meta, bucket.getKey(), snapshot, processed, dailyProcessedCount,
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
        Map<Long, String> names = new HashMap<>();
        pipelineRepository.findAll().forEach(p -> names.put(p.getId(), p.getName()));
        OffsetDateTime cutoff = OffsetDateTime.now().minusDays(MAX_RANGE_DAYS + 1);
        metadataArchiveRepository.findByArchivedAtAfter(cutoff)
                .forEach(a -> names.putIfAbsent(a.getPipelineId(), a.getName() + " (삭제됨)"));
        if (names.isEmpty()) {
            return List.of();
        }
        return commandHistoryRepository
                .findByPipelineIdInAndRequestedAtBetweenOrderByRequestedAtDesc(
                        new ArrayList<>(names.keySet()), range.from(), range.to())
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
     * 이 화면이 다루는 파이프라인 표시 메타. 현존 파이프라인 + 최근 삭제(스냅샷 보존창 내)를 합친다.
     * 삭제분은 pipeline_metadata_archive 에서 오므로, 삭제해도 보존정리 전까지 로그가 보인다.
     */
    private Map<Long, PipelineLogMeta> logMetaByPipelineId() {
        Map<Long, PipelineLogMeta> metaById = new LinkedHashMap<>();
        for (PipelineDefinition p : pipelineRepository.findAll()) {
            metaById.put(p.getId(), toMeta(p));
        }
        OffsetDateTime cutoff = OffsetDateTime.now().minusDays(MAX_RANGE_DAYS + 1);
        for (PipelineMetadataArchive a : metadataArchiveRepository.findByArchivedAtAfter(cutoff)) {
            metaById.putIfAbsent(a.getPipelineId(), toMeta(a));   // 현존 정의가 우선
        }
        return metaById;
    }

    private PipelineLogMeta toMeta(PipelineDefinition p) {
        boolean logFile = isLogFile(p.getPipelineType());
        String source = logFile ? "Filebeat"
                : path(p.getSourceDbType(), p.getSourceSchema(), p.getSourceTable());
        String target = path(p.getTargetDbType(), p.getTargetSchema(), p.getTargetTable());
        return new PipelineLogMeta(p.getId(), p.getName(), logFile, source, target);
    }

    private PipelineLogMeta toMeta(PipelineMetadataArchive a) {
        return new PipelineLogMeta(a.getPipelineId(), a.getName() + " (삭제됨)",
                isLogFile(a.getPipelineType()), a.getSourcePath(), a.getTargetPath());
    }

    private boolean isLogFile(String pipelineType) {
        return "LOG_FILE".equalsIgnoreCase(pipelineType);
    }

    private CdcProcessingLogResponse toProcessingResponse(
            PipelineLogMeta meta,
            LocalDateTime occurredAt,
            PipelineMetricSnapshot snapshot,
            long processed,
            long dailyProcessedCount,
            long lag,
            String sourceState,
            String sinkState,
            String status) {
        return new CdcProcessingLogResponse(
                meta.id(),
                meta.name(),
                // 소스/타겟 경로는 메타에 이미 계산돼 있다(로그 파이프라인은 "Filebeat", 삭제분은 아카이브 값).
                meta.sourcePath(),
                meta.targetPath(),
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
    private String sourceState(boolean logFile, PipelineMetricSnapshot snapshot) {
        if (logFile) {
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
