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

    private PipelineDeployService deployService;

    @BeforeEach
    void setUp() {
        deployService = new PipelineDeployService(pipelineDefinitionRepository, pipelineConnectorRepository,
                commandHistoryRecorder, connectionRepository, passwordCryptoService,
                connectorConfigRenderer, kafkaConnectClient, new ObjectMapper(),
                logPipelineSourceRepository, filebeatConfigRenderer, filebeatInputFileService);
    }

    @Test
    void deploy_success_registersBothConnectorsAndMarksDeployed() throws Exception {
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
        when(kafkaConnectClient.getStatus(anyString())).thenReturn(
                new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"), List.of(), "source"));

        deployService.deploy(1L);

        verify(kafkaConnectClient).upsertConfig(eq("source-1-oracle-appuser-customers"), any());
        verify(kafkaConnectClient).upsertConfig(eq("sink-1-postgresql-cdc_landing-customers"), any());
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        verify(commandHistoryRecorder).record(1L, "DEPLOY", "SUCCESS", null);
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
        doThrow(new KafkaConnectClientException("connection refused"))
                .when(kafkaConnectClient).upsertConfig(anyString(), any());

        org.junit.jupiter.api.Assertions.assertThrows(KafkaConnectClientException.class,
                () -> deployService.deploy(1L));

        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.FAILED);
        verify(commandHistoryRecorder).record(eq(1L), eq("DEPLOY"), eq("FAILED"), anyString());
        // 소스 등록 시도 전에 실패했으니 싱크 렌더링까지는 안 가야 함
        verify(connectorConfigRenderer, never()).renderSink(any());
    }

    @Test
    void deploy_logFilePipeline_registersOnlySinkConnectorAndWritesFilebeatConfig() throws Exception {
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
        when(kafkaConnectClient.getStatus(anyString())).thenReturn(
                new ConnectorStatusResponse("x", new ConnectorState("RUNNING", "w"), List.of(), "sink"));

        deployService.deploy(1L);

        verify(kafkaConnectClient).upsertConfig(eq("sink-1-postgresql-log_landing-app_log"), any());
        verify(connectorConfigRenderer, never()).renderSource(any());
        verify(filebeatInputFileService).write(1L, "- type: filestream");
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
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

    private void setId(Object entity, Class<?> type, Long id) throws Exception {
        Field idField = type.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
    }
}
