package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.KafkaTopicCleanupService;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.PostgresReplicationCleanupService;
import com.company.pipeline.logpipeline.FilebeatInputFileService;
import com.company.pipeline.logpipeline.LogPipelineSourceRepository;
import com.company.pipeline.logpipeline.dto.LogPipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineCreateRequest;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineServiceTest {

    @Mock
    private PipelineDefinitionRepository pipelineDefinitionRepository;
    @Mock
    private PipelineConnectorRepository pipelineConnectorRepository;
    @Mock
    private ConnectionRepository connectionRepository;
    @Mock
    private KafkaConnectClient kafkaConnectClient;
    @Mock
    private LogPipelineSourceRepository logPipelineSourceRepository;
    @Mock
    private FilebeatInputFileService filebeatInputFileService;
    @Mock
    private PostgresReplicationCleanupService postgresReplicationCleanupService;
    @Mock
    private KafkaTopicCleanupService kafkaTopicCleanupService;

    private PipelineService pipelineService;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        pipelineService = new PipelineService(pipelineDefinitionRepository, pipelineConnectorRepository,
                connectionRepository, kafkaConnectClient, logPipelineSourceRepository, filebeatInputFileService,
                postgresReplicationCleanupService, kafkaTopicCleanupService);
    }

    @Test
    void create_resolvesConnectionsAndPersistsDefinition() throws Exception {
        PipelineConnection source = newConnection(10L, DbType.ORACLE);
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL);
        when(connectionRepository.findById(10L)).thenReturn(Optional.of(source));
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));
        when(pipelineDefinitionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var request = new PipelineCreateRequest("test-pipeline", 10L, 20L,
                "APPUSER", "CUSTOMERS", "cdc_landing", "customers", "test-topic", true, null);

        var response = pipelineService.create(request);

        assertThat(response.name()).isEqualTo("test-pipeline");
        assertThat(response.sourceDbType()).isEqualTo(DbType.ORACLE);
        assertThat(response.targetDbType()).isEqualTo(DbType.POSTGRESQL);
        assertThat(response.status()).isEqualTo(PipelineStatus.CREATED);
    }

    @Test
    void create_rejectsOracleSourceWithNonCommonUser() throws Exception {
        PipelineConnection source = newConnectionWithUsername(10L, DbType.ORACLE, "appuser");
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL);
        when(connectionRepository.findById(10L)).thenReturn(Optional.of(source));
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));

        var request = new PipelineCreateRequest("test-pipeline", 10L, 20L,
                "APPUSER", "CUSTOMERS", "cdc_landing", "customers", "test-topic", true, null);

        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> pipelineService.create(request)).getMessage()).contains("공통 사용자");
    }

    @Test
    void createLogFilePipeline_persistsDefinitionAndLogSource() throws Exception {
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL);
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));
        when(pipelineDefinitionRepository.saveAndFlush(any())).thenAnswer(inv -> {
            PipelineDefinition def = inv.getArgument(0);
            Field idField = PipelineDefinition.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(def, 42L);
            return def;
        });

        var request = new LogPipelineCreateRequest("app-log", "filebeat-1", "/var/log/app/app.log",
                null, "END", "PLAIN", "UTF-8", false, 20L, "log_landing", "app_log", null, null);

        var response = pipelineService.createLogFilePipeline(request);

        assertThat(response.name()).isEqualTo("app-log");
        assertThat(response.pipelineType()).isEqualTo("LOG_FILE");
        assertThat(response.sourceConnectionId()).isNull();
        assertThat(response.topicName()).isEqualTo("log-42");
        assertThat(response.status()).isEqualTo(PipelineStatus.CREATED);
        verify(logPipelineSourceRepository).save(any());
    }

    @Test
    void createLogFilePipeline_rejectsNonPlainParseType() throws Exception {
        PipelineConnection target = newConnection(20L, DbType.POSTGRESQL);
        when(connectionRepository.findById(20L)).thenReturn(Optional.of(target));

        var request = new LogPipelineCreateRequest("app-log", "filebeat-1", "/var/log/app/app.log",
                null, "END", "JSON", "UTF-8", false, 20L, "log_landing", "app_log", null, null);

        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> pipelineService.createLogFilePipeline(request)).getMessage()).contains("PLAIN");
    }

    @Test
    void delete_deletesKafkaConnectConnectorsThenMetadata() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        PipelineConnector connector = new PipelineConnector(1L, "SOURCE", "source-1-oracle-appuser-customers",
                "io.debezium.connector.oracle.OracleConnector", "{}");
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of(connector));

        pipelineService.delete(1L);

        verify(kafkaConnectClient).delete("source-1-oracle-appuser-customers");
        verify(pipelineConnectorRepository).deleteAll(List.of(connector));
        verify(pipelineDefinitionRepository).delete(pipeline);
    }

    @Test
    void delete_tableCdcPipeline_invokesPostgresReplicationCleanupForSourceConnector() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        PipelineConnector sourceConnector = new PipelineConnector(1L, "SOURCE", "source-1-postgres-appuser-customers",
                "io.debezium.connector.postgresql.PostgresConnector", "{}");
        PipelineConnector sinkConnector = new PipelineConnector(1L, "SINK", "sink-1-postgresql-cdc_landing-customers",
                "io.debezium.connector.jdbc.JdbcSinkConnector", "{}");
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of(sourceConnector, sinkConnector));
        PipelineConnection source = newConnection(10L, DbType.ORACLE);
        when(connectionRepository.findById(10L)).thenReturn(Optional.of(source));

        pipelineService.delete(1L);

        verify(postgresReplicationCleanupService).cleanup(sourceConnector, source);
        verify(postgresReplicationCleanupService, never()).cleanup(eq(sinkConnector), any());
    }

    @Test
    void delete_tableCdcPipeline_invokesKafkaTopicCleanupWithCurrentPipelineList() throws Exception {
        PipelineDefinition pipeline = newPipeline(1L);
        PipelineDefinition otherPipeline = newPipeline(2L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineDefinitionRepository.findAll()).thenReturn(List.of(pipeline, otherPipeline));
        PipelineConnector connector = new PipelineConnector(1L, "SOURCE", "source-1-oracle-appuser-customers",
                "io.debezium.connector.oracle.OracleConnector", "{}");
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of(connector));

        pipelineService.delete(1L);

        verify(kafkaTopicCleanupService).cleanup(pipeline, List.of(connector), List.of(pipeline, otherPipeline));
    }

    @Test
    void delete_logFilePipeline_alsoCleansUpFilebeatConfigAndLogSource() throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "app-log", "LOG_FILE", null, 20L, null, DbType.POSTGRESQL,
                null, null, "log_landing", "app_log", "log-1", false, null, null);
        Field idField = PipelineDefinition.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipeline, 1L);
        when(pipelineDefinitionRepository.findById(1L)).thenReturn(Optional.of(pipeline));
        when(pipelineConnectorRepository.findByPipelineId(1L)).thenReturn(List.of());
        when(logPipelineSourceRepository.findByPipelineId(1L)).thenReturn(Optional.empty());

        pipelineService.delete(1L);

        verify(filebeatInputFileService).delete(1L);
        verify(pipelineDefinitionRepository).delete(pipeline);
    }

    private PipelineDefinition newPipeline(Long id) throws Exception {
        PipelineDefinition pipeline = new PipelineDefinition(
                "test-pipeline", "TABLE_CDC", 10L, 20L, DbType.ORACLE, DbType.POSTGRESQL,
                "APPUSER", "CUSTOMERS", "cdc_landing", "customers", "test-topic", true, null, null);
        Field idField = PipelineDefinition.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipeline, id);
        return pipeline;
    }

    private PipelineConnection newConnection(Long id, DbType dbType) throws Exception {
        return newConnectionWithUsername(id, dbType, "c##user");
    }

    private PipelineConnection newConnectionWithUsername(Long id, DbType dbType, String username) throws Exception {
        PipelineConnection connection = new PipelineConnection(
                "conn", dbType, "host", 1234, "db", "svc", null, username, "cipher", null);
        Field idField = PipelineConnection.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(connection, id);
        return connection;
    }
}
