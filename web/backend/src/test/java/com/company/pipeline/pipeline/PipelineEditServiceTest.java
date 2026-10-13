package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.SchemaDiscoveryService;
import com.company.pipeline.connection.dto.ColumnMetadataResponse;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.company.pipeline.pipeline.dto.PipelineUpdateRequest;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class PipelineEditServiceTest {

    private PipelineDefinitionRepository definitionRepository;
    private PipelineDeployService deployService;
    private PipelineGroupService groupService;
    private SchemaDiscoveryService schemaDiscoveryService;
    private PipelineCommandHistoryRecorder historyRecorder;
    private PipelineEditService service;
    private PipelineDefinition pipeline;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        definitionRepository = mock(PipelineDefinitionRepository.class);
        PipelineConnectorRepository connectorRepository = mock(PipelineConnectorRepository.class);
        deployService = mock(PipelineDeployService.class);
        groupService = mock(PipelineGroupService.class);
        schemaDiscoveryService = mock(SchemaDiscoveryService.class);
        historyRecorder = mock(PipelineCommandHistoryRecorder.class);
        // 잠금은 그대로 통과시켜 안의 동작을 본다.
        when(deployService.runLocked(eq(7L), any())).thenAnswer(invocation -> ((Supplier<Object>) invocation.getArgument(1)).get());
        when(connectorRepository.findByPipelineId(7L)).thenReturn(List.of());
        service = new PipelineEditService(definitionRepository, connectorRepository, deployService, groupService,
                schemaDiscoveryService, historyRecorder);

        pipeline = new PipelineDefinition("orders-cdc", "TABLE_CDC", 1L, 2L, DbType.POSTGRESQL, DbType.POSTGRESQL,
                "public", "orders", "dw", "orders", "postgresql-cdc.public.orders", false, "주문", null);
        ReflectionTestUtils.setField(pipeline, "id", 7L);
        pipeline.setStatus(PipelineStatus.DEPLOYED);
        pipeline.setLoadMode("UPSERT");
        pipeline.setSnapshotMode("INITIAL");
        pipeline.setExcludedColumns("memo");
        when(definitionRepository.findById(7L)).thenReturn(Optional.of(pipeline));
        when(schemaDiscoveryService.listColumns(1L, "public", "orders")).thenReturn(List.of(
                new ColumnMetadataResponse("id", "int4", true, false, false),
                new ColumnMetadataResponse("memo", "text", false, true, false),
                new ColumnMetadataResponse("phone", "varchar", false, true, false),
                new ColumnMetadataResponse("amount", "numeric", false, false, false)));
    }

    /** 화면이 보내는 모양: 잠긴 값은 지금 값 그대로, 고칠 수 있는 값만 바꾼다. */
    private PipelineUpdateRequest request(String name, String description, Long groupId, List<String> excluded,
                                          List<String> masked, Boolean deleteEnabled) {
        return new PipelineUpdateRequest(name, description, groupId, excluded, masked, deleteEnabled,
                1L, 2L, "public", "orders", "dw", "orders", "postgresql-cdc", "UPSERT", null, "INITIAL");
    }

    @Test
    void 고칠_수_있는_값만_바꾸고_커넥터는_건드리지_않고_이력을_남긴다() {
        when(definitionRepository.existsByName("orders-cdc-v2")).thenReturn(false);

        PipelineResponse response = service.update(7L,
                request("orders-cdc-v2", "주문 동기화", null, List.of("memo"), List.of("phone"), true), "kim");

        assertThat(response.name()).isEqualTo("orders-cdc-v2");
        assertThat(pipeline.getDescription()).isEqualTo("주문 동기화");
        assertThat(pipeline.getMaskedColumns()).isEqualTo("phone");
        assertThat(pipeline.getDeleteEnabled()).isTrue();
        // 잠긴 값 · 상태는 그대로 - 저장은 정의만 바꾼다.
        assertThat(pipeline.getStatus()).isEqualTo(PipelineStatus.DEPLOYED);
        assertThat(pipeline.getTopicName()).isEqualTo("postgresql-cdc.public.orders");
        verify(definitionRepository).save(pipeline);
        verify(deployService, never()).deploy(any());
        verify(historyRecorder).record(eq(7L), eq("DEFINITION_UPDATE"), eq("SUCCESS"),
                eq("이름 orders-cdc → orders-cdc-v2 · 설명 변경 · 마스킹 컬럼 없음 → phone · DELETE 반영 사용 안 함 → 사용"), eq("kim"));
    }

    @Test
    void 바뀐_것이_없으면_저장도_이력도_없다() {
        service.update(7L, request("orders-cdc", "주문", null, List.of("memo"), List.of(), false), "kim");

        verify(definitionRepository, never()).save(any());
        verify(historyRecorder, never()).record(any(), anyString(), anyString(), anyString(), anyString());
    }

    @Test
    void 잠긴_값을_바꾸려_하면_거절한다() {
        PipelineUpdateRequest changedTarget = new PipelineUpdateRequest("orders-cdc", "주문", null, null, null, null,
                1L, 2L, "public", "orders", "dw", "orders_v2", "postgresql-cdc", "UPSERT", null, "INITIAL");
        PipelineUpdateRequest changedPrefix = new PipelineUpdateRequest("orders-cdc", "주문", null, null, null, null,
                1L, 2L, "public", "orders", "dw", "orders", "other-cdc", "UPSERT", null, "INITIAL");
        PipelineUpdateRequest changedMode = new PipelineUpdateRequest("orders-cdc", "주문", null, null, null, null,
                1L, 2L, "public", "orders", "dw", "orders", "postgresql-cdc", "DELTA_APPEND", null, "INITIAL");

        assertThatThrownBy(() -> service.update(7L, changedTarget, "kim")).isInstanceOf(BusinessException.class)
                .hasMessageContaining("타깃 테이블");
        assertThatThrownBy(() -> service.update(7L, changedPrefix, "kim")).isInstanceOf(BusinessException.class)
                .hasMessageContaining("Topic Prefix");
        assertThatThrownBy(() -> service.update(7L, changedMode, "kim")).isInstanceOf(BusinessException.class)
                .hasMessageContaining("적재 방식");
        verify(definitionRepository, never()).save(any());
    }

    @Test
    void PK_마스킹_불가_겹침_없는_컬럼은_서버에서도_막는다() {
        assertThatThrownBy(() -> service.update(7L, request("orders-cdc", "주문", null, List.of("id"), List.of(), false), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("PK 컬럼은 제외할 수 없습니다");
        assertThatThrownBy(() -> service.update(7L, request("orders-cdc", "주문", null, List.of("memo"), List.of("amount"), false), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("마스킹할 수 없는 형식");
        assertThatThrownBy(() -> service.update(7L, request("orders-cdc", "주문", null, List.of("memo", "phone"), List.of("phone"), false), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("동시에");
        assertThatThrownBy(() -> service.update(7L, request("orders-cdc", "주문", null, List.of("ghost"), List.of(), false), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("없는 컬럼");
        verify(definitionRepository, never()).save(any());
    }

    @Test
    void 준비_중이면_막고_이름이_겹치면_막는다() {
        pipeline.setStatus(PipelineStatus.DEPLOYING);
        assertThatThrownBy(() -> service.update(7L, request("orders-cdc", "주문", null, null, null, null), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("준비(배포) 중");

        pipeline.setStatus(PipelineStatus.READY);
        when(definitionRepository.existsByName("taken")).thenReturn(true);
        assertThatThrownBy(() -> service.update(7L, request("taken", "주문", null, null, null, null), "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("이미 사용 중");
    }

    @Test
    void 델타_적재는_DELETE_반영을_켤_수_없다() {
        pipeline.setLoadMode("DELTA_APPEND");
        pipeline.setDeltaOpColumn("cdc_op");
        PipelineUpdateRequest turnOnDelete = new PipelineUpdateRequest("orders-cdc", "주문", null, null, null, true,
                1L, 2L, "public", "orders", "dw", "orders", "postgresql-cdc", "DELTA_APPEND", "cdc_op", "INITIAL");

        assertThatThrownBy(() -> service.update(7L, turnOnDelete, "kim"))
                .isInstanceOf(BusinessException.class).hasMessageContaining("델타");
    }

    @Test
    void 로그_파이프라인은_이름_설명_그룹만_바뀐다() {
        PipelineDefinition log = new PipelineDefinition("LOG_PIPE", "LOG_FILE", null, 2L, null, DbType.POSTGRESQL,
                null, null, "public", "demo_web_access", "log-9", false, null, null);
        ReflectionTestUtils.setField(log, "id", 7L);
        when(definitionRepository.findById(7L)).thenReturn(Optional.of(log));

        service.update(7L, new PipelineUpdateRequest("LOG_PIPE", "웹 접근 로그", null, List.of("x"), List.of("y"), true,
                null, 2L, null, null, "public", "demo_web_access", null, null, null, null), "kim");

        assertThat(log.getDescription()).isEqualTo("웹 접근 로그");
        assertThat(log.getExcludedColumns()).isNull();
        assertThat(log.getMaskedColumns()).isNull();
        assertThat(log.getDeleteEnabled()).isFalse();
    }

    @Test
    void 토픽_prefix_는_저장된_토픽_이름에서_되찾는다() {
        assertThat(PipelineEditService.topicPrefixOf(pipeline)).isEqualTo("postgresql-cdc");
        PipelineDefinition oracle = new PipelineDefinition("o", "TABLE_CDC", 1L, 2L, DbType.ORACLE, DbType.POSTGRESQL,
                "csb", "tb_imp018m", "dw", "tb_imp018m", "oracle-cdc.CSB.TB_IMP018M", false, null, null);
        assertThat(PipelineEditService.topicPrefixOf(oracle)).isEqualTo("oracle-cdc");
        assertThat(PipelineEditService.joinColumns(List.of(" b", "a", "b", ""))).isEqualTo("a,b");
        assertThat(PipelineEditService.joinColumns(List.of())).isNull();
    }
}
