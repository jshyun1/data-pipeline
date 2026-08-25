package com.company.pipeline.monitoring;

import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.rollup.RollupService;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 대시보드 "적재 건수"를 위해 배포된 Kafka 파이프라인들의 싱크 커넥터 committed offset을
 * 주기적으로 스냅샷하고, 직전 스냅샷 대비 늘어난 만큼을 오늘 날짜 롤업에 더한다.
 * Airflow DAG를 수동으로 트리거해야만 잡히던 이전 방식과 달리, 파이프라인이 배포돼서
 * 돌아가는 동안 자동으로 계속 반영된다.
 */
@Component
public class KafkaPipelineMetricScheduler {

    private static final Logger log = LoggerFactory.getLogger(KafkaPipelineMetricScheduler.class);
    private static final String SOURCE = "KAFKA";
    private static final String TASK_KEY = "verify_target_db_landing";

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineMetricSnapshotService pipelineMetricSnapshotService;
    private final PipelineMetricSnapshotRepository pipelineMetricSnapshotRepository;
    private final PipelineDailyLoadMetricService dailyLoadMetricService;
    private final HeartbeatService heartbeat;
    private final RollupService rollupService;

    public KafkaPipelineMetricScheduler(
            PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineMetricSnapshotService pipelineMetricSnapshotService,
            PipelineMetricSnapshotRepository pipelineMetricSnapshotRepository,
            PipelineDailyLoadMetricService dailyLoadMetricService,
            HeartbeatService heartbeat,
            RollupService rollupService) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineMetricSnapshotService = pipelineMetricSnapshotService;
        this.pipelineMetricSnapshotRepository = pipelineMetricSnapshotRepository;
        this.dailyLoadMetricService = dailyLoadMetricService;
        this.heartbeat = heartbeat;
        this.rollupService = rollupService;
    }

    @Scheduled(fixedRate = 20_000, initialDelay = 20_000)
    public void checkDeployedPipelines() {
        // STOPPED는 Sink만 멈추고 Source는 계속 Kafka에 쌓는 상태이므로 lag가 증가하는지
        // 계속 관측해야 한다. READY는 Source/Sink 모두 정지 상태라 수집 대상이 아니다.
        List<PipelineDefinition> deployed = pipelineDefinitionRepository.findByStatusIn(
                List.of(PipelineStatus.DEPLOYED, PipelineStatus.STOPPED));
        for (PipelineDefinition pipeline : deployed) {
            try {
                checkOne(pipeline);
            } catch (Exception ex) {
                // 파이프라인 하나가 일시적으로 응답 안 해도(예: 커넥터 재시작 중) 다른 파이프라인
                // 체크까지 막히면 안 되므로 로그만 남기고 계속 진행한다.
                log.warn("파이프라인 {} 적재 건수 체크 실패: {}", pipeline.getId(), ex.getMessage());
            }
        }
        // 주기 완주(대상 0건이어도) - 생존 신호. 수집이 멈추면 이 beat 가 끊긴다(U5).
        heartbeat.beat("kafka-metrics");
    }

    private void checkOne(PipelineDefinition pipeline) {
        Optional<PipelineMetricSnapshot> previous =
                pipelineMetricSnapshotRepository.findTopByPipelineIdOrderByCollectedAtDesc(pipeline.getId());
        PipelineMetricSnapshot current = pipelineMetricSnapshotService.recordSnapshot(pipeline.getId());

        long delta = 0;
        if (previous.isPresent() && current.getCommittedOffset() != null
                && previous.get().getCommittedOffset() != null) {
            // 직전 스냅샷 시점에 컨슈머가 실제로 읽기 시작할 수 있었던 지점부터 센다.
            // retention 으로 그 아래가 이미 삭제됐다면 컨슈머는 earliest 로 건너뛰므로,
            // committed 차이를 그대로 쓰면 읽지도 않은 삭제 구간까지 처리 건수에 들어간다
            // (2026-08-25 실측: 실제 10만 건인데 20만으로 집계).
            long previousEarliest = previous.get().getEarliestOffset() == null
                    ? 0L : previous.get().getEarliestOffset();
            long countFrom = Math.max(previous.get().getCommittedOffset(), previousEarliest);
            delta = Math.max(0, current.getCommittedOffset() - countFrom);
        }
        // delta==0(적재 없음/기준점) 에도 기록해 observation_count 를 올린다 - "0건 관측"과 "미관측"을
        // 구분하는 U7 의 핵심(가드 제거).
        rollupService.recordObservation(SOURCE, pipeline.getId().toString(), TASK_KEY, pipeline.getName(),
                pipeline.getId(), delta);
        if (delta > 0) {
            dailyLoadMetricService.incrementLoadedCount(
                    SOURCE, pipeline.getId().toString(), TASK_KEY, pipeline.getName(), delta);
        }
    }
}
