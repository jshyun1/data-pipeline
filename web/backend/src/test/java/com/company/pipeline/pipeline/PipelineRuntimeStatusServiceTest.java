package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineRuntimeStatusServiceTest {

    @Mock PipelineDefinitionRepository pipelineRepository;
    @Mock PipelineConnectorRepository connectorRepository;
    @Mock PipelineCommandHistoryRepository commandHistoryRepository;

    private PipelineRuntimeStatusService service;

    @BeforeEach
    void setUp() {
        service = new PipelineRuntimeStatusService(
                pipelineRepository, connectorRepository, commandHistoryRepository, new ObjectMapper());
    }

    @Test
    void deployedPipeline_reportsRunningWithoutMismatch() throws Exception {
        PipelineDefinition pipeline = pipeline(1L, PipelineStatus.DEPLOYED);
        PipelineConnector source = connector("SOURCE", "RUNNING", "RUNNING");
        PipelineConnector sink = connector("SINK", "RUNNING", "RUNNING");
        when(connectorRepository.findByPipelineId(1L)).thenReturn(List.of(source, sink));
        when(commandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(1L)).thenReturn(List.of());

        var result = service.toResponse(pipeline);

        assertThat(result.runtimeStatus()).isEqualTo("RUNNING");
        assertThat(result.statusMismatch()).isFalse();
        assertThat(result.sourceTaskStates()).containsExactly("RUNNING");
        assertThat(result.sinkTaskStates()).containsExactly("RUNNING");
    }

    @Test
    void deployedPipeline_reportsStoppedMismatchAndLatestCommand() throws Exception {
        PipelineDefinition pipeline = pipeline(1L, PipelineStatus.DEPLOYED);
        PipelineConnector source = connector("SOURCE", "RUNNING", "RUNNING");
        PipelineConnector sink = connector("SINK", "STOPPED", "STOPPED");
        PipelineCommandHistory command = new PipelineCommandHistory();
        command.setPipelineId(1L);
        command.setCommand("STOP");
        command.setResult("SUCCESS");
        command.setRequestedAt(LocalDateTime.of(2026, 8, 13, 10, 0));
        when(connectorRepository.findByPipelineId(1L)).thenReturn(List.of(source, sink));
        when(commandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(1L)).thenReturn(List.of(command));

        var result = service.toResponse(pipeline);

        assertThat(result.runtimeStatus()).isEqualTo("STOPPED");
        assertThat(result.statusMismatch()).isTrue();
        assertThat(result.runtimeStatusReason()).contains("DEPLOYED").contains("STOPPED");
        assertThat(result.lastCommand()).isEqualTo("STOP");
        assertThat(result.lastCommandResult()).isEqualTo("SUCCESS");
    }

    @Test
    void taskFailure_overridesConnectorRunningState() throws Exception {
        PipelineDefinition pipeline = pipeline(1L, PipelineStatus.DEPLOYED);
        PipelineConnector source = connector("SOURCE", "RUNNING", "RUNNING");
        PipelineConnector sink = connector("SINK", "RUNNING", "FAILED");
        when(connectorRepository.findByPipelineId(1L)).thenReturn(List.of(source, sink));
        when(commandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(1L)).thenReturn(List.of());

        var result = service.toResponse(pipeline);

        assertThat(result.runtimeStatus()).isEqualTo("FAILED");
        assertThat(result.statusMismatch()).isTrue();
        assertThat(result.sinkTaskStates()).containsExactly("FAILED");
    }

    @Test
    void createdPipeline_withoutConnectorsIsNotDeployed() throws Exception {
        PipelineDefinition pipeline = pipeline(1L, PipelineStatus.CREATED);
        when(connectorRepository.findByPipelineId(1L)).thenReturn(List.of());
        when(commandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(1L)).thenReturn(List.of());

        var result = service.toResponse(pipeline);

        assertThat(result.runtimeStatus()).isEqualTo("NOT_DEPLOYED");
        assertThat(result.statusMismatch()).isFalse();
    }

    private PipelineDefinition pipeline(Long id, PipelineStatus status) throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "test", "TABLE_CDC", 10L, 20L, DbType.ORACLE, DbType.POSTGRESQL,
                "APP", "SOURCE_TABLE", "public", "target_table", "cdc", true, null, null);
        Field idField = PipelineDefinition.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipeline, id);
        pipeline.setStatus(status);
        return pipeline;
    }

    private PipelineConnector connector(String role, String connectorState, String taskState) {
        PipelineConnector connector = new PipelineConnector(
                1L, role, role.toLowerCase() + "-connector", "connector.Class", "{}");
        connector.setStatus(connectorState);
        connector.setLastStatusJson("""
                {"name":"connector","connector":{"state":"%s","worker_id":"worker:8083"},
                 "tasks":[{"id":0,"state":"%s","worker_id":"worker:8083"}],"type":"source"}
                """.formatted(connectorState, taskState));
        return connector;
    }
}
