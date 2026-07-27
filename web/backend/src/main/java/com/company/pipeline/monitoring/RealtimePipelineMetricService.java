package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.RealtimePipelineMetricResponse;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import java.time.Duration;
import java.util.List;
import org.springframework.stereotype.Service;

/** 대시보드 상단의 파이프라인별 실시간 처리율·미처리량 조회 서비스. */
@Service
public class RealtimePipelineMetricService {

    private static final String SOURCE = "KAFKA";

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineMetricSnapshotRepository snapshotRepository;
    private final PipelineDailyLoadMetricRepository dailyLoadMetricRepository;

    public RealtimePipelineMetricService(
            PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineMetricSnapshotRepository snapshotRepository,
            PipelineDailyLoadMetricRepository dailyLoadMetricRepository) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.snapshotRepository = snapshotRepository;
        this.dailyLoadMetricRepository = dailyLoadMetricRepository;
    }

    public List<RealtimePipelineMetricResponse> getMetrics() {
        return pipelineDefinitionRepository.findAll().stream()
                .map(pipeline -> toResponse(pipeline.getId()))
                .toList();
    }

    private RealtimePipelineMetricResponse toResponse(Long pipelineId) {
        List<PipelineMetricSnapshot> snapshots =
                snapshotRepository.findTop2ByPipelineIdOrderByCollectedAtDesc(pipelineId);
        if (snapshots.isEmpty()) {
            return new RealtimePipelineMetricResponse(
                    pipelineId, null, null, null, null, null,
                    0D, null, lastProgressAt(pipelineId), "NO_DATA");
        }

        PipelineMetricSnapshot latest = snapshots.get(0);
        double throughput = calculateThroughput(snapshots);
        long lag = valueOrZero(latest.getConsumerLag());
        Long recoverySeconds = throughput > 0D && lag > 0L
                ? (long) Math.ceil(lag / throughput)
                : null;

        return new RealtimePipelineMetricResponse(
                pipelineId,
                latest.getCollectedAt(),
                latest.getPartitionCount(),
                latest.getEndOffset(),
                latest.getCommittedOffset(),
                latest.getConsumerLag(),
                throughput,
                recoverySeconds,
                lastProgressAt(pipelineId),
                "COLLECTED");
    }

    private double calculateThroughput(List<PipelineMetricSnapshot> snapshots) {
        if (snapshots.size() < 2) {
            return 0D;
        }
        PipelineMetricSnapshot latest = snapshots.get(0);
        PipelineMetricSnapshot previous = snapshots.get(1);
        if (latest.getCollectedAt() == null || previous.getCollectedAt() == null
                || latest.getCommittedOffset() == null || previous.getCommittedOffset() == null) {
            return 0D;
        }
        long elapsedMillis = Duration.between(previous.getCollectedAt(), latest.getCollectedAt()).toMillis();
        long delta = latest.getCommittedOffset() - previous.getCommittedOffset();
        if (elapsedMillis <= 0L || delta <= 0L) {
            return 0D;
        }
        return Math.round((delta * 1000D / elapsedMillis) * 10D) / 10D;
    }

    private java.time.LocalDateTime lastProgressAt(Long pipelineId) {
        return dailyLoadMetricRepository
                .findTopByPipelineSourceAndPipelineKeyOrderByUpdatedAtDesc(SOURCE, pipelineId.toString())
                .map(PipelineDailyLoadMetric::getUpdatedAt)
                .orElse(null);
    }

    private long valueOrZero(Long value) {
        return value == null ? 0L : value;
    }
}
