package com.company.pipeline.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.dto.WorkflowRequests.CreateWorkflow;
import com.company.pipeline.workflow.dto.WorkflowResponses.WorkflowSummary;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * 워크플로우 키는 지워도 다시 쓰지 않는다(2026-09-17). 키가 dag_id(etl_wf_{키})·실행 원장·알림 판정을 묶어서,
 * 재사용하면 지운 워크플로우의 실행 이력이 새 워크플로우에 이어 붙었다.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WorkflowServiceKeyTest {

    @Mock
    private EtlWorkflowRepository workflowRepository;
    @Mock
    private EtlWorkflowNodeRepository nodeRepository;
    @Mock
    private EtlWorkflowEdgeRepository edgeRepository;
    @Mock
    private EtlJobRepository jobRepository;
    @Mock
    private NifiProcessGroupMetadataRepository pgRepository;
    @Mock
    private PipelineDefinitionRepository pipelineRepository;

    private WorkflowService service;

    @BeforeEach
    void setUp() {
        service = new WorkflowService(workflowRepository, nodeRepository, edgeRepository, jobRepository,
                pgRepository, pipelineRepository);
        when(workflowRepository.findByWorkflowKeyAndDeletedAtIsNull(any())).thenReturn(Optional.empty());
        when(workflowRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void generatedKey_skipsKeyOfDeletedWorkflow() {
        // «cdc» 는 지운 워크플로우가 썼던 키 - 살아 있는 워크플로우는 없어도 건너뛰어야 한다.
        when(workflowRepository.existsByWorkflowKey("cdc")).thenReturn(true);
        when(workflowRepository.existsByWorkflowKey("cdc_2")).thenReturn(false);

        WorkflowSummary created = service.create(request(null, "CDC"), "kim");

        assertThat(created.workflowKey()).isEqualTo("cdc_2");
        assertThat(created.dagId()).isEqualTo("etl_wf_cdc_2");
    }

    @Test
    void explicitKeyOfDeletedWorkflow_isRejected() {
        when(workflowRepository.existsByWorkflowKey("cdc")).thenReturn(true);

        assertThatThrownBy(() -> service.create(request("cdc", "CDC"), "kim"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("삭제된 워크플로우가 쓰던");
        verify(workflowRepository, never()).save(any());
    }

    @Test
    void freshKey_isUsedAsIs() {
        when(workflowRepository.existsByWorkflowKey("orders")).thenReturn(false);

        assertThat(service.create(request(null, "orders"), "kim").workflowKey()).isEqualTo("orders");
    }

    @Test
    void create_storesRegistrant() {
        // 등록자를 남긴다(2026-10-13, V81) - 예전에는 칸이 없어 AI 가 누가 만들었는지 말할 수 없었다.
        when(workflowRepository.existsByWorkflowKey("orders")).thenReturn(false);

        service.create(request(null, "orders"), "kim");

        ArgumentCaptor<EtlWorkflow> saved = ArgumentCaptor.forClass(EtlWorkflow.class);
        verify(workflowRepository).save(saved.capture());
        assertThat(saved.getValue().getCreatedBy()).isEqualTo("kim");
    }

    private static CreateWorkflow request(String key, String name) {
        return new CreateWorkflow(key, name, null, null, null, null, null, null);
    }
}
