package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.CdcPrerequisiteService;
import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.ConnectorConfigRenderer;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.logpipeline.FilebeatConfigRenderer;
import com.company.pipeline.logpipeline.FilebeatInputFileService;
import com.company.pipeline.logpipeline.LogPipelineSourceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** «설정 변경 미반영» - 지금 정의로 만들 설정과 등록 때 저장한 설정을 고칠 수 있는 키만 비교한다(2026-10-12). */
class PipelinePendingConfigTest {

    private PipelineConnectorRepository connectorRepository;
    private ConnectorConfigRenderer renderer;
    private PipelineDeployService deployService;
    private PipelineDefinition pipeline;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        connectorRepository = mock(PipelineConnectorRepository.class);
        ConnectionRepository connectionRepository = mock(ConnectionRepository.class);
        PasswordCryptoService crypto = mock(PasswordCryptoService.class);
        renderer = mock(ConnectorConfigRenderer.class);
        when(connectionRepository.findById(any())).thenReturn(Optional.of(mock(PipelineConnection.class)));
        when(crypto.decrypt(any())).thenReturn("secret");
        deployService = new PipelineDeployService(mock(PipelineDefinitionRepository.class), connectorRepository,
                mock(PipelineCommandHistoryRecorder.class), connectionRepository, crypto, renderer,
                mock(KafkaConnectClient.class), objectMapper, mock(LogPipelineSourceRepository.class),
                mock(FilebeatConfigRenderer.class), mock(FilebeatInputFileService.class),
                mock(DeltaTargetTableService.class), mock(CdcPrerequisiteService.class));
        pipeline = new PipelineDefinition("orders-cdc", "TABLE_CDC", 1L, 2L, DbType.POSTGRESQL, DbType.POSTGRESQL,
                "public", "orders", "dw", "orders", "postgresql-cdc.public.orders", false, null, null);
        ReflectionTestUtils.setField(pipeline, "id", 7L);
    }

    private PipelineConnector connector(String role, Map<String, Object> config) throws Exception {
        return new PipelineConnector(7L, role, role.toLowerCase() + "-7", "x", objectMapper.writeValueAsString(config));
    }

    private static Map<String, Object> config(Object... pairs) {
        Map<String, Object> out = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((String) pairs[i], pairs[i + 1]);
        }
        out.put("database.password", "secret");   // 비교 대상이 아닌 키는 달라도 상관없다
        return out;
    }

    @Test
    void 고칠_수_있는_키가_다르면_항목으로_잡는다() throws Exception {
        when(connectorRepository.findByPipelineIdAndConnectorRole(7L, "SOURCE")).thenReturn(Optional.of(
                connector("SOURCE", config("column.exclude.list", "public.orders.memo", "topic.prefix", "postgresql-cdc"))));
        when(connectorRepository.findByPipelineIdAndConnectorRole(7L, "SINK")).thenReturn(Optional.of(
                connector("SINK", config("delete.enabled", "false"))));
        when(renderer.renderSource(any())).thenReturn(new RenderedConnectorConfig("source-7", "SOURCE", "x",
                config("column.exclude.list", "public.orders.memo", "column.mask.with.8.chars", "public.orders.phone",
                        "topic.prefix", "changed-but-not-editable")));
        when(renderer.renderSink(any())).thenReturn(new RenderedConnectorConfig("sink-7", "SINK", "x",
                config("delete.enabled", "true")));

        // 마스킹 · DELETE 반영만 다르다. 제외 컬럼은 같고, 고칠 수 없는 키(topic.prefix)는 보지 않는다.
        assertThat(deployService.pendingConfigItems(pipeline)).containsExactly("마스킹 컬럼", "DELETE 반영");
    }

    @Test
    void 템플릿이_쓰지_않는_값은_차이로_잡지_않는다() throws Exception {
        // MySQL 소스 템플릿처럼 제외 · 마스킹 키를 아예 만들지 않으면, 등록본에도 없으니 같다.
        when(connectorRepository.findByPipelineIdAndConnectorRole(7L, "SOURCE")).thenReturn(Optional.of(
                connector("SOURCE", config("topic.prefix", "mysql-cdc"))));
        when(connectorRepository.findByPipelineIdAndConnectorRole(7L, "SINK")).thenReturn(Optional.empty());
        when(renderer.renderSource(any())).thenReturn(new RenderedConnectorConfig("source-7", "SOURCE", "x",
                config("topic.prefix", "mysql-cdc")));

        assertThat(deployService.pendingConfigItems(pipeline)).isEmpty();
    }

    @Test
    void 커넥터를_아직_등록하지_않았거나_로그_파이프라인이면_비어_있다() {
        when(connectorRepository.findByPipelineIdAndConnectorRole(any(), any())).thenReturn(Optional.empty());
        assertThat(deployService.pendingConfigItems(pipeline)).isEmpty();

        PipelineDefinition log = new PipelineDefinition("LOG_PIPE", "LOG_FILE", null, 2L, null, DbType.POSTGRESQL,
                null, null, "public", "demo_web_access", "log-9", false, null, null);
        assertThat(deployService.pendingConfigItems(log)).isEmpty();
    }
}
