package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.ConnectorConfigRenderer;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.KafkaConnectClientException;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorState;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.logpipeline.FilebeatConfigRenderer;
import com.company.pipeline.logpipeline.FilebeatInputFileService;
import com.company.pipeline.logpipeline.LogPipelineSource;
import com.company.pipeline.logpipeline.LogPipelineSourceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class PipelineDeployServiceTest {

    @Mock
    private PipelineDefinitionRepository pipelineDefinitionRepository;
    @Mock
    private PipelineConnectorRepository pipelineConnectorRepository;
    @Mock
    private PipelineCommandHistoryRecorder commandHistoryRecorder;
    @Mock
    private ConnectionRepository connectionRepository;
    @Mock
    private PasswordCryptoService passwordCryptoService;
    @Mock
    private ConnectorConfigRenderer connectorConfigRenderer;
    @Mock
    private KafkaConnectClient kafkaConnectClient;
    @Mock
    private LogPipelineSourceRepository logPipelineSourceRepository;
    @Mock
    private FilebeatConfigRenderer filebeatConfigRenderer;
    @Mock
    private FilebeatInputFileService filebeatInputFileService;
    @Mock
    private DeltaTargetTableService deltaTargetTableService;

    private PipelineDeployService deployService;

    @BeforeEach
    void setUp() {
        deployService = new PipelineDeployService(pipelineDefinitionRepository, pipelineConnectorRepository,
                commandHistoryRecorder, connectionRepository, passwordCryptoService,
                connectorConfigRenderer, kafkaConnectClient, new ObjectMapper(),
                logPipelineSourceRepository, filebeatConfigRenderer, filebeatInputFileService,
                deltaTargetTableService);
        // 단위 테스트에서는 실제 대기 없이 안정화 상태를 한 번 더 검증한다.
        ReflectionTestUtils.setField(deployService, "startStabilityMillis", 0L);
    }

    @Test
    void deploy_tableCdc_createsBothConnectorsStoppedAndMarksReady() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineConnection source = newConnection(10L, DbType.ORACLE, "cipher-src");
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL, "cipher-tgt");
        when(connectionRepository.findById(10L)).thenReturn(Optional.of(source));
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));
        when(passwordCryptoService.decrypt(anyString())).thenReturn("plain-pw");

        RenderedConnectorConfig sourceRendered = new RenderedConnectorConfig(
                "source-1-oracle-appuser-customers", "SOURCE", "io.debezium.connector.oracle.OracleConnector",
                Map.of("connector.class", "io.debezium.connector.oracle.OracleConnector"));
        RenderedConnectorConfig sinkRendered = new RenderedConnectorConfig(
                "sink-1-postgresql-cdc_landing-customers", "SINK", "io.debezium.connector.jdbc.JdbcSinkConnector",
                Map.of("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector"));
        when(connectorConfigRenderer.renderSource(any())).thenReturn(sourceRendered);
        when(connectorConfigRenderer.renderSink(any())).thenReturn(sinkRendered);

        when(pipelineConnectorRepository.findByPipelineIdAndConnectorRole(eq(1L), anyString()))
                .thenReturn(Optional.empty());
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of());
        when(kafkaConnectClient.getStatus("source-1-oracle-appuser-customers"))
                .thenThrow(new KafkaConnectClientException("status not found"))
                .thenReturn(stoppedStatus());
        when(kafkaConnectClient.getStatus("sink-1-postgresql-cdc_landing-customers"))
                .thenReturn(stoppedStatus());

        deployService.deploy(1L);

        verify(kafkaConnectClient).createStopped(eq("source-1-oracle-appuser-customers"), any());
        verify(kafkaConnectClient).createStopped(eq("sink-1-postgresql-cdc_landing-customers"), any());
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.READY);
        verify(commandHistoryRecorder).record(1L, "PREPARE", "SUCCESS", null);
        verify(pipelineConnectorRepository, times(2)).save(any());
    }

    @Test
    void deploy_kafkaConnectFailure_marksFailedAndRecordsHistory() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineConnection source = newConnection(10L, DbType.ORACLE, "cipher-src");
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL, "cipher-tgt");
        when(connectionRepository.findById(10L)).thenReturn(Optional.of(source));
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));
        when(passwordCryptoService.decrypt(anyString())).thenReturn("plain-pw");

        RenderedConnectorConfig sourceRendered = new RenderedConnectorConfig(
                "source-1-oracle-appuser-customers", "SOURCE", "io.debezium.connector.oracle.OracleConnector",
                Map.of("k", "v"));
        when(connectorConfigRenderer.renderSource(any())).thenReturn(sourceRendered);
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of());
        doThrow(new KafkaConnectClientException("connection refused"))
                .when(kafkaConnectClient).createStopped(anyString(), any());

        org.junit.jupiter.api.Assertions.assertThrows(KafkaConnectClientException.class,
                () -> deployService.deploy(1L));

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.FAILED);
        verify(commandHistoryRecorder).record(eq(1L), eq("PREPARE"), eq("FAILED"), anyString());
        // 소스 등록 시도 전에 실패했으니 싱크 렌더링까지는 안 가야 함
        verify(connectorConfigRenderer, never()).renderSink(any());
    }

    @Test
    void deploy_logFilePipeline_createsSinkStoppedAndLandsInReady() throws Exception {
        // 배포는 "만들되 실행하지 않는" 단계로 CDC와 통일했다 - 예전에는 LOG_FILE만
        // 배포 즉시 적재가 시작돼서, 아무도 시작을 지시하지 않았는데 데이터가 흐르는
        // 경로가 있었다. 실행 지시는 오직 Airflow 제어 DAG가 한다.
        PipelineDefinition pipeline = newLogFilePipeline(1L, 20L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL, "cipher-tgt");
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));
        when(passwordCryptoService.decrypt(anyString())).thenReturn("plain-pw");

        LogPipelineSource source = new LogPipelineSource(1L, "filebeat-1", "/var/log/app/app.log",
                null, "END", "PLAIN", "UTF-8", false, "log-1");
        when(logPipelineSourceRepository.findByPipelineId(1L)).thenReturn(Optional.of(source));

        RenderedConnectorConfig sinkRendered = new RenderedConnectorConfig(
                "sink-1-postgresql-log_landing-app_log", "SINK", "io.debezium.connector.jdbc.JdbcSinkConnector",
                Map.of("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector"));
        when(connectorConfigRenderer.renderLogSink(any())).thenReturn(sinkRendered);
        when(filebeatConfigRenderer.render(eq(1L), any())).thenReturn("- type: filestream");

        when(pipelineConnectorRepository.findByPipelineIdAndConnectorRole(eq(1L), anyString()))
                .thenReturn(Optional.empty());
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of());
        when(kafkaConnectClient.getStatus(anyString())).thenReturn(
                new ConnectorStatusResponse("x", new ConnectorState("STOPPED", "w"), List.of(), "sink"));

        deployService.deploy(1L);

        // 즉시 실행되는 upsertConfig가 아니라 initial_state=STOPPED로 만들어야 한다.
        verify(kafkaConnectClient).createStopped(eq("sink-1-postgresql-log_landing-app_log"), any());
        verify(kafkaConnectClient, never()).upsertConfig(anyString(), any());
        verify(connectorConfigRenderer, never()).renderSource(any());
        verify(filebeatInputFileService).write(1L, "- type: filestream");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.READY);
    }

    @Test
    void pause_pausesEveryDeployedConnectorAndMarksPipelinePaused() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        var sourceConnector = new com.company.pipeline.connector.PipelineConnector(
                1L, "SOURCE", "source-1-oracle-appuser-customers", "x", "{}");
        var sinkConnector = new com.company.pipeline.connector.PipelineConnector(
                1L, "SINK", "sink-1-postgresql-cdc_landing-customers", "y", "{}");
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of(sourceConnector, sinkConnector));
        when(kafkaConnectClient.getStatus(anyString())).thenReturn(
                new ConnectorStatusResponse("x", new ConnectorState("PAUSED", "w"), List.of(), "source"));

        deployService.pause(1L);

        verify(kafkaConnectClient).pause("source-1-oracle-appuser-customers");
        verify(kafkaConnectClient).pause("sink-1-postgresql-cdc_landing-customers");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.PAUSED);
        verify(commandHistoryRecorder).record(1L, "PAUSE", "SUCCESS", null);
    }

    @Test
    void pause_withNoDeployedConnectors_throwsValidationError() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class, () -> deployService.pause(1L));
        verify(kafkaConnectClient, never()).pause(anyString());
    }

    @Test
    void restart_callsRestartTaskForEachConnector() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        var connector = new com.company.pipeline.connector.PipelineConnector(
                1L, "SOURCE", "source-1-oracle-appuser-customers", "x", "{}");
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of(connector));
        when(kafkaConnectClient.getStatus(anyString())).thenReturn(
                new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"), List.of(), "source"));

        deployService.restart(1L);

        verify(kafkaConnectClient).restartTask("source-1-oracle-appuser-customers", 0);
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
    }

    @Test
    void start_fromReady_resumesSourceThenSinkAndMarksDeployed() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.READY);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1"))
                .thenReturn(stoppedStatus(), startingStatus(), runningStatus());
        when(kafkaConnectClient.getStatus("sink-1"))
                .thenReturn(stoppedStatus(), runningStatus());

        deployService.resume(1L);

        var order = org.mockito.Mockito.inOrder(kafkaConnectClient);
        order.verify(kafkaConnectClient).resume("source-1");
        order.verify(kafkaConnectClient).resume("sink-1");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(commandHistoryRecorder).record(1L, "START", "SUCCESS", null);
    }

    @Test
    void start_fromStopped_resumesOnlySinkAndKeepsSourceRunning() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.STOPPED);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1")).thenReturn(runningStatus());
        when(kafkaConnectClient.getStatus("sink-1"))
                .thenReturn(stoppedStatus(), runningStatus());

        deployService.resume(1L);

        verify(kafkaConnectClient, never()).resume("source-1");
        verify(kafkaConnectClient).resume("sink-1");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
    }

    @Test
    void start_fromDeployed_rejectsDuplicateMonitorRunWithoutChangingConnectors() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.DEPLOYED);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1")).thenReturn(runningStatus());
        when(kafkaConnectClient.getStatus("sink-1")).thenReturn(runningStatus());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> deployService.resume(1L));

        verify(kafkaConnectClient, never()).resume(anyString());
        verify(kafkaConnectClient, never()).stop(anyString());
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(commandHistoryRecorder).record(eq(1L), eq("START"), eq("FAILED"),
                org.mockito.ArgumentMatchers.contains("이미 실행 중"));
    }

    @Test
    void start_sinkFailure_stopsBothAndRestoresReady() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.READY);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1"))
                .thenReturn(stoppedStatus(), runningStatus(), stoppedStatus());
        when(kafkaConnectClient.getStatus("sink-1"))
                .thenReturn(stoppedStatus())
                .thenReturn(failedTaskStatus())
                .thenReturn(stoppedStatus());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> deployService.resume(1L));

        verify(kafkaConnectClient).stop("sink-1");
        verify(kafkaConnectClient).stop("source-1");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.READY);
        verify(commandHistoryRecorder).record(eq(1L), eq("START"), eq("FAILED"), anyString());
    }

    @Test
    void start_sourceFailsImmediatelyAfterRunning_stopsBothAndRestoresReady() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.READY);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1"))
                .thenReturn(stoppedStatus(), runningStatus(), failedTaskStatus(), stoppedStatus());
        when(kafkaConnectClient.getStatus("sink-1"))
                .thenReturn(stoppedStatus(), runningStatus(), stoppedStatus());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> deployService.resume(1L));

        verify(kafkaConnectClient).stop("sink-1");
        verify(kafkaConnectClient).stop("source-1");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.READY);
        verify(commandHistoryRecorder).record(eq(1L), eq("START"), eq("FAILED"),
                org.mockito.ArgumentMatchers.contains("taskStates=[FAILED]"));
    }

    @Test
    void stop_stopsOnlySinkAndVerifiesSourceStillRunning() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        pipeline.setStatus(PipelineStatus.DEPLOYED);
        var source = connector("SOURCE", "source-1");
        var sink = connector("SINK", "sink-1");
        stubPipelineAndConnectors(pipeline, source, sink);
        when(kafkaConnectClient.getStatus("source-1")).thenReturn(runningStatus(), runningStatus());
        when(kafkaConnectClient.getStatus("sink-1")).thenReturn(runningStatus(), stoppedStatus());

        deployService.stop(1L);

        verify(kafkaConnectClient, never()).stop("source-1");
        verify(kafkaConnectClient).stop("sink-1");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.STOPPED);
        verify(commandHistoryRecorder).record(1L, "STOP", "SUCCESS", null);
    }

    private PipelineDefinition newPipeline(Long id) throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "test-pipeline", "TABLE_CDC", 10L, 20L, DbType.ORACLE, DbType.POSTGRESQL,
                "APPUSER", "CUSTOMERS", "cdc_landing", "customers", "test-topic", true, null, null);
        setId(pipeline, PipelineDefinition.class, id);
        return pipeline;
    }

    private PipelineDefinition newLogFilePipeline(Long id, Long targetConnectionId) throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "app-log", "LOG_FILE", null, targetConnectionId, null, DbType.POSTGRESQL,
                null, null, "log_landing", "app_log", "log-1", false, null, null);
        setId(pipeline, PipelineDefinition.class, id);
        return pipeline;
    }

    private PipelineConnection newConnection(Long id, DbType dbType, String encryptedPassword) throws Exception {
        PipelineConnection connection = new PipelineConnection(
                "conn", dbType, "host", 1234, "db", "svc", null, "user", encryptedPassword, null);
        setId(connection, PipelineConnection.class, id);
        return connection;
    }

    private com.company.pipeline.connector.PipelineConnector connector(String role, String name) {
        return new com.company.pipeline.connector.PipelineConnector(1L, role, name, "x", "{}");
    }

    private void stubPipelineAndConnectors(PipelineDefinition pipeline,
            com.company.pipeline.connector.PipelineConnector source,
            com.company.pipeline.connector.PipelineConnector sink) {
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(pipelineConnectorRepository.findByPipelineIdAndConnectorRole(1L, "SOURCE"))
                .thenReturn(Optional.of(source));
        when(pipelineConnectorRepository.findByPipelineIdAndConnectorRole(1L, "SINK"))
                .thenReturn(Optional.of(sink));
        org.mockito.Mockito.lenient()
                .when(pipelineConnectorRepository.findByPipelineId(1L))
                .thenReturn(List.of(source, sink));
    }

    private ConnectorStatusResponse stoppedStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("STOPPED", "w"), List.of(), "source");
    }

    private ConnectorStatusResponse runningStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"),
                List.of(new ConnectorTaskStatus(0, "RUNNING", "w", null)), "source");
    }

    private ConnectorStatusResponse startingStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"),
                List.of(), "source");
    }

    private ConnectorStatusResponse failedTaskStatus() {
        return new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"),
                List.of(new ConnectorTaskStatus(0, "FAILED", "w", "boom")), "source");
    }

    private void setId(Object entity, Class<?> type, Long id) throws Exception {
        Field idField = type.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
    }
}
