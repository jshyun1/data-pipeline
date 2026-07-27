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
 * Airflow 시작 명령이 끝난 뒤에도 장시간 실행되는 TABLE_CDC 파이프라인의 실제
 * Kafka Connect 상태를 metadata-db와 동기화한다.
 *
 * Kafka Connect 분산 워커는 재조정 중 connector/task가 잠깐 비정상처럼 보일 수
 * 있으므로 한 번의 불일치로 FAILED를 만들지 않는다. 동일 파이프라인에서 연속 3회
 * 불일치가 확인될 때만 확정 장애로 기록한다.
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
        List<PipelineDefinition> pipelines = pipelineDefinitionRepository.findByStatusIn(
                List.of(PipelineStatus.DEPLOYED, PipelineStatus.STOPPED));
        for (PipelineDefinition pipeline : pipelines) {
            if (!"TABLE_CDC".equalsIgnoreCase(pipeline.getPipelineType())) {
                continue;
            }
            synchronizeOne(pipeline);
        }
    }

    void synchronizeOne(PipelineDefinition pipeline) {
        List<PipelineConnector> connectors =
                pipelineConnectorRepository.findByPipelineId(pipeline.getId());
        PipelineConnector source = connectorByRole(connectors, "SOURCE");
        PipelineConnector sink = connectorByRole(connectors, "SINK");

        String problem = null;
        if (source == null || sink == null) {
            problem = "Source/Sink Connector 메타데이터가 불완전합니다."
                    + " source=" + (source != null) + ", sink=" + (sink != null);
        } else {
            try {
                ConnectorStatusResponse sourceStatus = kafkaConnectClient.getStatus(source.getConnectorName());
                ConnectorStatusResponse sinkStatus = kafkaConnectClient.getStatus(sink.getConnectorName());
                saveLiveStatus(source, sourceStatus);
                saveLiveStatus(sink, sinkStatus);
                problem = expectedStateProblem(pipeline.getStatus(), sourceStatus, sinkStatus);
            } catch (Exception ex) {
                problem = "Kafka Connect 상태 조회 실패: " + safeMessage(ex);
            }
        }

        if (problem == null) {
            consecutiveFailures.remove(pipeline.getId());
            return;
        }

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

    private String expectedStateProblem(PipelineStatus pipelineStatus,
            ConnectorStatusResponse sourceStatus, ConnectorStatusResponse sinkStatus) {
        if (pipelineStatus == PipelineStatus.DEPLOYED) {
            String sourceProblem = connectorProblem("Source", sourceStatus, "RUNNING", true);
            String sinkProblem = connectorProblem("Sink", sinkStatus, "RUNNING", true);
            return joinProblems(sourceProblem, sinkProblem);
        }
        if (pipelineStatus == PipelineStatus.STOPPED) {
            String sourceProblem = connectorProblem("Source", sourceStatus, "RUNNING", true);
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
