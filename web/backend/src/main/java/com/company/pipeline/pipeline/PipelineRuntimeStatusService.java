package com.company.pipeline.pipeline;

import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.pipeline.dto.PipelineRuntimeStatusResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 저장된 Kafka Connect 관측값을 화면 표시용 상태로 판정한다. 조회 중 Connect REST를 직접 호출하지 않는다. */
@Service
@Transactional(readOnly = true)
public class PipelineRuntimeStatusService {

    private static final Set<String> CONTROL_COMMANDS =
            Set.of("PREPARE", "DEPLOY", "START", "PAUSE", "STOP", "RESTART");

    private final PipelineDefinitionRepository pipelineRepository;
    private final PipelineConnectorRepository connectorRepository;
    private final PipelineCommandHistoryRepository commandHistoryRepository;
    private final ObjectMapper objectMapper;

    public PipelineRuntimeStatusService(PipelineDefinitionRepository pipelineRepository,
            PipelineConnectorRepository connectorRepository,
            PipelineCommandHistoryRepository commandHistoryRepository,
            ObjectMapper objectMapper) {
        this.pipelineRepository = pipelineRepository;
        this.connectorRepository = connectorRepository;
        this.commandHistoryRepository = commandHistoryRepository;
        this.objectMapper = objectMapper;
    }

    public List<PipelineRuntimeStatusResponse> list() {
        return pipelineRepository.findAll().stream().map(this::toResponse).toList();
    }

    public PipelineRuntimeStatusResponse get(Long pipelineId) {
        PipelineDefinition pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        return toResponse(pipeline);
    }

    PipelineRuntimeStatusResponse toResponse(PipelineDefinition pipeline) {
        List<PipelineConnector> connectors = connectorRepository.findByPipelineId(pipeline.getId());
        PipelineConnector source = connector(connectors, "SOURCE");
        PipelineConnector sink = connector(connectors, "SINK");
        ConnectorObservation sourceObservation = observe(source);
        ConnectorObservation sinkObservation = observe(sink);
        boolean sourceRequired = "TABLE_CDC".equalsIgnoreCase(pipeline.getPipelineType());

        RuntimeDecision decision = decide(
                pipeline.getStatus(), sourceRequired, sourceObservation.state(), sinkObservation.state());
        LocalDateTime checkedAt = latest(sourceObservation.checkedAt(), sinkObservation.checkedAt());
        PipelineCommandHistory lastCommand = commandHistoryRepository
                .findByPipelineIdOrderByRequestedAtDesc(pipeline.getId()).stream()
                .filter(history -> CONTROL_COMMANDS.contains(history.getCommand()))
                .findFirst().orElse(null);

        return new PipelineRuntimeStatusResponse(
                pipeline.getId(), pipeline.getStatus().name(), decision.runtimeStatus(),
                sourceObservation.state(), sourceObservation.taskStates(),
                sinkObservation.state(), sinkObservation.taskStates(), checkedAt,
                decision.mismatch(), decision.reason(),
                lastCommand == null ? null : lastCommand.getCommand(),
                lastCommand == null ? null : lastCommand.getResult(),
                lastCommand == null ? null : lastCommand.getMessage(),
                lastCommand == null ? null : lastCommand.getRequestedAt());
    }

    private RuntimeDecision decide(PipelineStatus stored, boolean sourceRequired,
            String sourceState, String sinkState) {
        if (stored == PipelineStatus.CREATED && sourceState == null && sinkState == null) {
            return new RuntimeDecision("NOT_DEPLOYED", false, "아직 Kafka Connect에 배포되지 않았습니다.");
        }
        if (sinkState == null || (sourceRequired && sourceState == null)) {
            return new RuntimeDecision("MISSING", true, "필수 커넥터 메타데이터가 없습니다.");
        }
        if (isUnknown(sinkState) || (sourceRequired && isUnknown(sourceState))) {
            return new RuntimeDecision("UNKNOWN", false, "Kafka Connect의 최근 상태를 확인할 수 없습니다.");
        }
        if (isFailed(sinkState) || (sourceRequired && isFailed(sourceState))) {
            return new RuntimeDecision("FAILED", stored != PipelineStatus.FAILED, "커넥터 또는 태스크가 실패 상태입니다.");
        }

        String actual = actualStatus(sourceRequired, sourceState, sinkState);
        String expected = expectedStatus(stored);
        boolean mismatch = expected != null && !expected.equals(actual);
        String reason = mismatch
                ? "저장 상태 " + stored + "의 예상 실측값은 " + expected + "이지만 현재 " + actual + "입니다."
                : null;
        return new RuntimeDecision(actual, mismatch, reason);
    }

    private String actualStatus(boolean sourceRequired, String sourceState, String sinkState) {
        if ("PAUSED".equalsIgnoreCase(sinkState)
                && (!sourceRequired || "PAUSED".equalsIgnoreCase(sourceState))) {
            return "PAUSED";
        }
        if ("STOPPED".equalsIgnoreCase(sinkState)) {
            if (!sourceRequired || "STOPPED".equalsIgnoreCase(sourceState)) return "READY";
            if ("RUNNING".equalsIgnoreCase(sourceState)) return "STOPPED";
        }
        if ("RUNNING".equalsIgnoreCase(sinkState)
                && (!sourceRequired || "RUNNING".equalsIgnoreCase(sourceState))) {
            return "RUNNING";
        }
        return "DEGRADED";
    }

    private String expectedStatus(PipelineStatus stored) {
        return switch (stored) {
            case READY -> "READY";
            case DEPLOYED -> "RUNNING";
            case PAUSED -> "PAUSED";
            case STOPPED -> "STOPPED";
            case FAILED -> "FAILED";
            default -> null;
        };
    }

    private ConnectorObservation observe(PipelineConnector connector) {
        if (connector == null) return new ConnectorObservation(null, List.of(), null);
        String state = connector.getStatus();
        List<String> taskStates = List.of();
        if (connector.getLastStatusJson() != null && !connector.getLastStatusJson().isBlank()) {
            try {
                ConnectorStatusResponse status = objectMapper.readValue(
                        connector.getLastStatusJson(), ConnectorStatusResponse.class);
                if (status.connector() != null && status.connector().state() != null) state = status.connector().state();
                if (status.tasks() != null) {
                    taskStates = status.tasks().stream().map(task -> task.state()).filter(Objects::nonNull).toList();
                    if (taskStates.stream().anyMatch(this::isFailed)) state = "FAILED";
                }
            } catch (JsonProcessingException ignored) {
                // 이전 버전이 저장한 JSON을 읽지 못해도 connector.status는 계속 사용할 수 있다.
            }
        }
        return new ConnectorObservation(state, taskStates, connector.getUpdatedAt());
    }

    private PipelineConnector connector(List<PipelineConnector> connectors, String role) {
        return connectors.stream().filter(c -> role.equalsIgnoreCase(c.getConnectorRole())).findFirst().orElse(null);
    }

    private boolean isFailed(String state) {
        return "FAILED".equalsIgnoreCase(state);
    }

    private boolean isUnknown(String state) {
        return state == null || "UNKNOWN".equalsIgnoreCase(state) || "UNASSIGNED".equalsIgnoreCase(state);
    }

    private LocalDateTime latest(LocalDateTime first, LocalDateTime second) {
        if (first == null) return second;
        if (second == null) return first;
        return first.isAfter(second) ? first : second;
    }

    private record ConnectorObservation(String state, List<String> taskStates, LocalDateTime checkedAt) {}
    private record RuntimeDecision(String runtimeStatus, boolean mismatch, String reason) {}
}
