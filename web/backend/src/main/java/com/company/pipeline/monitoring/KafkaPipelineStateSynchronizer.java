package com.company.pipeline.monitoring;

import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.pipeline.PipelineCommandHistoryRecorder;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Airflow 시작 명령이 끝난 뒤에도 장시간 실행되는 파이프라인의 실제 Kafka Connect
 * 상태를 metadata-db와 동기화한다. TABLE_CDC와 LOG_FILE 둘 다 대상이다.
 *
 * <p>로그 파이프라인의 "소스"는 filebeat라서 Kafka Connect 커넥터가 아니다 - SINK만
 * 있는 것이 정상 구성이므로 그쪽은 Sink만 검사한다. 예전에는 TABLE_CDC만 보고
 * 넘어갔는데, 그 탓에 로그 파이프라인은 커넥터가 죽어도 배포 시점에 찍힌 상태가
 * 그대로 남아 화면에 계속 "실행중"으로 보였다.
 *
 * Kafka Connect 분산 워커는 재조정 중 connector/task가 잠깐 비정상처럼 보일 수
 * 있으므로 한 번의 불일치로 FAILED를 만들지 않는다. 동일 파이프라인에서 연속 3회
 * 불일치가 확인될 때만 확정 장애로 기록한다.
 *
 * <p>두 가지를 구분하는 것이 중요하다.
 * <ul>
 *   <li><b>상태 불일치</b> - Kafka Connect에 물어봤고, 커넥터가 기대와 다른 상태였다.
 *       이건 진짜 장애 신호이므로 임계치를 넘으면 FAILED로 확정한다.</li>
 *   <li><b>조회 불가</b> - Kafka Connect에 물어보지도 못했다(I/O 오류 등).
 *       커넥터가 멀쩡한데 우리가 못 본 것일 수도 있으므로 <b>판정을 보류</b>한다.
 *       실제로 호스트 메모리 고갈로 몇 분간 API 호출이 막혔을 때, 정상 동작 중이던
 *       파이프라인이 FAILED로 찍힌 사고가 있었다(2026-07-27).</li>
 * </ul>
 *
 * <p>또한 FAILED 파이프라인도 계속 점검 대상에 포함한다. 예전에는 DEPLOYED/STOPPED만
 * 조회해서, 한 번 FAILED가 되면 커넥터가 이후 아무리 정상이어도 영원히 FAILED로
 * 남았다(조회 대상에서 빠지므로 되돌릴 주체가 없었다). 이제 커넥터 실제 상태가
 * 기대와 맞으면 자동으로 원래 상태로 되돌린다.
 */
@Component
public class KafkaPipelineStateSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(KafkaPipelineStateSynchronizer.class);
    private static final int FAILURE_THRESHOLD = 3;

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final PipelineCommandHistoryRecorder commandHistoryRecorder;
    private final KafkaConnectClient kafkaConnectClient;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<Long, Integer> consecutiveFailures = new ConcurrentHashMap<>();

    public KafkaPipelineStateSynchronizer(
            PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineConnectorRepository pipelineConnectorRepository,
            PipelineCommandHistoryRecorder commandHistoryRecorder,
            KafkaConnectClient kafkaConnectClient,
            ObjectMapper objectMapper) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.commandHistoryRecorder = commandHistoryRecorder;
        this.kafkaConnectClient = kafkaConnectClient;
        this.objectMapper = objectMapper;
    }

    @Scheduled(
            fixedRateString = "${pipeline.cdc.runtime-monitor.interval-millis:10000}",
            initialDelayString = "${pipeline.cdc.runtime-monitor.initial-delay-millis:10000}")
    public void synchronizeRuntimeStates() {
        // FAILED도 포함한다 - 빼면 한 번 실패한 파이프라인을 되돌릴 주체가 없어진다.
        List<PipelineDefinition> pipelines = pipelineDefinitionRepository.findByStatusIn(
                List.of(PipelineStatus.DEPLOYED, PipelineStatus.STOPPED, PipelineStatus.FAILED));
        for (PipelineDefinition pipeline : pipelines) {
            synchronizeOne(pipeline);
        }
    }

    void synchronizeOne(PipelineDefinition pipeline) {
        List<PipelineConnector> connectors =
                pipelineConnectorRepository.findByPipelineId(pipeline.getId());
        // 로그 파이프라인은 소스가 filebeat라 SINK 하나뿐인 것이 정상이다.
        boolean sourceRequired = "TABLE_CDC".equalsIgnoreCase(pipeline.getPipelineType());
        PipelineConnector source = connectorByRole(connectors, "SOURCE");
        PipelineConnector sink = connectorByRole(connectors, "SINK");

        if (sink == null || (sourceRequired && source == null)) {
            registerProblem(pipeline, "Connector 메타데이터가 불완전합니다."
                    + " source=" + (source != null) + ", sink=" + (sink != null));
            return;
        }

        ConnectorStatusResponse sourceStatus;
        ConnectorStatusResponse sinkStatus;
        try {
            sourceStatus = source == null ? null : kafkaConnectClient.getStatus(source.getConnectorName());
            sinkStatus = kafkaConnectClient.getStatus(sink.getConnectorName());
        } catch (Exception ex) {
            // 조회 자체가 실패했으면 커넥터 상태를 알 수 없다. 여기서 FAILED를 찍으면
            // Kafka Connect가 잠시 느려진 것만으로 정상 파이프라인이 장애로 둔갑한다.
            // 판정을 보류하고 실패 카운터도 건드리지 않는다(다음 주기에 다시 시도).
            log.warn("파이프라인 {} 상태 조회 불가 - 판정 보류: {}",
                    pipeline.getId(), safeMessage(ex));
            return;
        }
        if (source != null) {
            saveLiveStatus(source, sourceStatus);
        }
        saveLiveStatus(sink, sinkStatus);

        if (pipeline.getStatus() == PipelineStatus.FAILED) {
            recoverIfHealthy(pipeline, sourceStatus, sinkStatus);
            return;
        }

        String problem = expectedStateProblem(pipeline.getStatus(), sourceStatus, sinkStatus);
        if (problem == null) {
            consecutiveFailures.remove(pipeline.getId());
            return;
        }
        registerProblem(pipeline, problem);
    }

    /**
     * FAILED로 기록된 파이프라인의 커넥터가 실제로는 정상이면 원래 상태로 되돌린다.
     *
     * <p>되돌릴 상태는 저장해두지 않고 커넥터 실제 상태에서 역으로 판단한다 - Source/Sink가
     * 모두 RUNNING이면 DEPLOYED, Sink만 STOPPED면 STOPPED. 어느 쪽에도 맞지 않으면
     * 진짜 문제가 있는 것이므로 FAILED를 유지한다.
     */
    private void recoverIfHealthy(PipelineDefinition pipeline,
            ConnectorStatusResponse sourceStatus, ConnectorStatusResponse sinkStatus) {
        PipelineStatus recovered = null;
        if (expectedStateProblem(PipelineStatus.DEPLOYED, sourceStatus, sinkStatus) == null) {
            recovered = PipelineStatus.DEPLOYED;
        } else if (expectedStateProblem(PipelineStatus.STOPPED, sourceStatus, sinkStatus) == null) {
            recovered = PipelineStatus.STOPPED;
        }
        if (recovered == null) {
            return;
        }

        PipelineStatus target = recovered;
        pipelineDefinitionRepository.findById(pipeline.getId()).ifPresent(current -> {
            if (current.getStatus() != PipelineStatus.FAILED) {
                return;  // 그 사이 사람이 배포/중지해서 이미 벗어났으면 건드리지 않는다
            }
            current.setStatus(target);
            pipelineDefinitionRepository.save(current);
            consecutiveFailures.remove(current.getId());
            String detail = "Kafka Connect 커넥터가 정상 상태로 확인되어 " + target + "으로 자동 복구";
            log.info("CDC 파이프라인 {} {}", current.getId(), detail);
            commandHistoryRecorder.record(current.getId(), "RUNTIME_MONITOR", "SUCCESS", detail);
        });
    }

    private void registerProblem(PipelineDefinition pipeline, String problem) {
        int failureCount = consecutiveFailures.merge(pipeline.getId(), 1, Integer::sum);
        log.warn("CDC 파이프라인 {} 런타임 상태 불일치 ({}/{}): {}",
                pipeline.getId(), failureCount, FAILURE_THRESHOLD, problem);
        if (failureCount < FAILURE_THRESHOLD) {
            return;
        }

        String failureDetail = problem;
        // 점검 도중 Airflow stop/start가 완료되어 상태가 바뀌었으면 이전 기대 상태를
        // 기준으로 FAILED를 덮어쓰지 않는다. 다음 스케줄 주기에서 새 상태로 다시 확인한다.
        pipelineDefinitionRepository.findById(pipeline.getId()).ifPresent(current -> {
            if (current.getStatus() != pipeline.getStatus()) {
                consecutiveFailures.remove(pipeline.getId());
                return;
            }
            current.setStatus(PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(current);
            commandHistoryRecorder.record(
                    current.getId(), "RUNTIME_MONITOR", "FAILED", failureDetail);
            consecutiveFailures.remove(current.getId());
        });
    }

    private PipelineConnector connectorByRole(List<PipelineConnector> connectors, String role) {
        return connectors.stream()
                .filter(connector -> role.equalsIgnoreCase(connector.getConnectorRole()))
                .findFirst()
                .orElse(null);
    }

    /**
     * {@code sourceStatus}가 null이면 소스 커넥터가 없는 구성(LOG_FILE)이므로 Sink만 본다.
     *
     * <p>CDC에서 중지(STOPPED)일 때 Source를 RUNNING으로 기대하는 것은 의도된 설계다 -
     * 소스를 멈추면 원천 변경분(Oracle redo 등)을 영구히 놓치므로 Sink만 멈춘다.
     */
    private String expectedStateProblem(PipelineStatus pipelineStatus,
            ConnectorStatusResponse sourceStatus, ConnectorStatusResponse sinkStatus) {
        if (pipelineStatus == PipelineStatus.DEPLOYED) {
            String sourceProblem = sourceStatus == null ? null
                    : connectorProblem("Source", sourceStatus, "RUNNING", true);
            String sinkProblem = connectorProblem("Sink", sinkStatus, "RUNNING", true);
            return joinProblems(sourceProblem, sinkProblem);
        }
        if (pipelineStatus == PipelineStatus.STOPPED) {
            String sourceProblem = sourceStatus == null ? null
                    : connectorProblem("Source", sourceStatus, "RUNNING", true);
            String sinkProblem = connectorProblem("Sink", sinkStatus, "STOPPED", false);
            return joinProblems(sourceProblem, sinkProblem);
        }
        return null;
    }

    private String connectorProblem(String role, ConnectorStatusResponse status,
            String expectedConnectorState, boolean requireRunningTasks) {
        String connectorState = status != null && status.connector() != null
                ? status.connector().state() : "UNKNOWN";
        List<String> taskStates = status != null && status.tasks() != null
                ? status.tasks().stream().map(ConnectorTaskStatus::state).toList()
                : List.of();
        boolean connectorMatches = expectedConnectorState.equalsIgnoreCase(connectorState);
        boolean tasksMatch = !requireRunningTasks
                || (!taskStates.isEmpty()
                && taskStates.stream().allMatch("RUNNING"::equalsIgnoreCase));
        if (connectorMatches && tasksMatch) {
            return null;
        }
        return role + " 상태 불일치: expected=" + expectedConnectorState
                + (requireRunningTasks ? "/tasks RUNNING" : "")
                + ", connector=" + connectorState + ", tasks=" + taskStates;
    }

    private String joinProblems(String first, String second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first + "; " + second;
    }

    private void saveLiveStatus(PipelineConnector connector, ConnectorStatusResponse status) {
        connector.setStatus(status != null && status.connector() != null
                ? status.connector().state() : "UNKNOWN");
        connector.setLastStatusJson(toJson(status));
        pipelineConnectorRepository.save(connector);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            return "{\"error\":\"상태 직렬화 실패\"}";
        }
    }

    private String safeMessage(Exception ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }
}
