package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorState;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.pipeline.PipelineCommandHistoryRecorder;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaPipelineStateSynchronizerTest {

    @Mock
    private PipelineDefinitionRepository pipelineDefinitionRepository;
    @Mock
    private PipelineConnectorRepository pipelineConnectorRepository;
    @Mock
    private PipelineCommandHistoryRecorder commandHistoryRecorder;
    @Mock
    private KafkaConnectClient kafkaConnectClient;

    private KafkaPipelineStateSynchronizer synchronizer;

    @BeforeEach
    void setUp() {
        synchronizer = new KafkaPipelineStateSynchronizer(
                pipelineDefinitionRepository,
                pipelineConnectorRepository,
                commandHistoryRecorder,
                kafkaConnectClient,
                new ObjectMapper());
    }

    @Test
    void deployedPipeline_threeConsecutiveTaskFailures_marksFailedAndRecordsHistory() throws Exception {
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.DEPLOYED);
        PipelineConnector source = connector("SOURCE", "source-50");
        PipelineConnector sink = connector("SINK", "sink-50");
        when(pipelineConnectorRepository.findByPipelineId(50L)).thenReturn(List.of(source, sink));
        when(pipelineDefinitionRepository.findById(50L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(kafkaConnectClient.getStatus("source-50")).thenReturn(failedTaskStatus());
        when(kafkaConnectClient.getStatus("sink-50")).thenReturn(runningStatus());

        synchronizer.synchronizeOne(pipeline);
        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(commandHistoryRecorder, never()).record(any(), anyString(), anyString(), anyString());

        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.FAILED);
        verify(commandHistoryRecorder).record(
                eq(50L), eq("RUNTIME_MONITOR"), eq("FAILED"),
                org.mockito.ArgumentMatchers.contains("tasks=[FAILED]"));
    }

    @Test
    void successfulCheck_resetsConsecutiveFailureCounter() throws Exception {
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.DEPLOYED);
        PipelineConnector source = connector("SOURCE", "source-50");
        PipelineConnector sink = connector("SINK", "sink-50");
        when(pipelineConnectorRepository.findByPipelineId(50L)).thenReturn(List.of(source, sink));
        when(kafkaConnectClient.getStatus("source-50"))
                .thenReturn(failedTaskStatus(), failedTaskStatus(), runningStatus(),
                        failedTaskStatus(), failedTaskStatus());
        when(kafkaConnectClient.getStatus("sink-50")).thenReturn(runningStatus());

        synchronizer.synchronizeOne(pipeline);
        synchronizer.synchronizeOne(pipeline);
        synchronizer.synchronizeOne(pipeline);
        synchronizer.synchronizeOne(pipeline);
        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(pipelineDefinitionRepository, never()).findById(50L);
        verify(commandHistoryRecorder, never()).record(any(), anyString(), anyString(), anyString());
    }

    @Test
    void stoppedPipeline_sourceRunningAndSinkStopped_isHealthy() throws Exception {
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.STOPPED);
        PipelineConnector source = connector("SOURCE", "source-50");
        PipelineConnector sink = connector("SINK", "sink-50");
        when(pipelineConnectorRepository.findByPipelineId(50L)).thenReturn(List.of(source, sink));
        when(kafkaConnectClient.getStatus("source-50")).thenReturn(runningStatus());
        when(kafkaConnectClient.getStatus("sink-50")).thenReturn(stoppedStatus());

        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.STOPPED);
        verify(commandHistoryRecorder, never()).record(any(), anyString(), anyString(), anyString());
        assertThat(source.getStatus()).isEqualTo("RUNNING");
        assertThat(sink.getStatus()).isEqualTo("STOPPED");
    }

    @Test
    void statusLookupFails_doesNotMarkFailed_evenAfterManyAttempts() throws Exception {
        // Kafka Connect에 물어보지도 못한 상황. 커넥터가 멀쩡한데 우리가 못 본 것일 수
        // 있으므로 판정을 보류해야 한다. 호스트 메모리 고갈로 API 호출이 몇 분간 막혔을 때
        // 정상 파이프라인이 FAILED로 찍혔던 사고(2026-07-27)를 막는다.
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.DEPLOYED);
        when(pipelineConnectorRepository.findByPipelineId(50L))
                .thenReturn(List.of(connector("SOURCE", "source-50"), connector("SINK", "sink-50")));
        when(kafkaConnectClient.getStatus(anyString()))
                .thenThrow(new RuntimeException("I/O error on GET request"));

        for (int i = 0; i < 5; i++) {
            synchronizer.synchronizeOne(pipeline);
        }

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(pipelineDefinitionRepository, never()).save(any());
        verify(commandHistoryRecorder, never()).record(any(), anyString(), anyString(), anyString());
    }

    @Test
    void failedPipeline_connectorsHealthyAgain_recoversToDeployed() throws Exception {
        // 예전에는 FAILED가 조회 대상에서 빠져 영원히 FAILED로 남았다.
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.FAILED);
        PipelineConnector source = connector("SOURCE", "source-50");
        PipelineConnector sink = connector("SINK", "sink-50");
        when(pipelineConnectorRepository.findByPipelineId(50L)).thenReturn(List.of(source, sink));
        when(pipelineDefinitionRepository.findById(50L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(kafkaConnectClient.getStatus("source-50")).thenReturn(runningStatus());
        when(kafkaConnectClient.getStatus("sink-50")).thenReturn(runningStatus());

        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(commandHistoryRecorder).record(
                eq(50L), eq("RUNTIME_MONITOR"), eq("SUCCESS"),
                org.mockito.ArgumentMatchers.contains("자동 복구"));
    }

    @Test
    void failedPipeline_connectorsStillBroken_staysFailed() throws Exception {
        PipelineDefinition pipeline = pipeline(50L, PipelineStatus.FAILED);
        when(pipelineConnectorRepository.findByPipelineId(50L))
                .thenReturn(List.of(connector("SOURCE", "source-50"), connector("SINK", "sink-50")));
        when(kafkaConnectClient.getStatus("source-50")).thenReturn(failedTaskStatus());
        when(kafkaConnectClient.getStatus("sink-50")).thenReturn(runningStatus());

        synchronizer.synchronizeOne(pipeline);

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.FAILED);
        verify(commandHistoryRecorder, never()).record(any(), anyString(), anyString(), anyString());
    }

    private PipelineDefinition pipeline(Long id, PipelineStatus status) throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "LW_IMP018M", "TABLE_CDC", 12L, 11L,
                DbType.ORACLE, DbType.POSTGRESQL,
                "CSB", "TB_IMP018M", "public", "LW_IMP018M",
                "oracle-cdc", true, null, null);
        Field idField = PipelineDefinition.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipeline, id);
        pipeline.setStatus(status);
        return pipeline;
    }

    private PipelineConnector connector(String role, String name) {
        return new PipelineConnector(50L, role, name, "connector.class", "{}");
    }

    private ConnectorStatusResponse runningStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "worker"),
                List.of(new ConnectorTaskStatus(0, "RUNNING", "worker", null)), "source");
    }

    private ConnectorStatusResponse failedTaskStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "worker"),
                List.of(new ConnectorTaskStatus(0, "FAILED", "worker", "boom")), "source");
    }

    private ConnectorStatusResponse stoppedStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("STOPPED", "worker"),
                List.of(), "sink");
    }
}
