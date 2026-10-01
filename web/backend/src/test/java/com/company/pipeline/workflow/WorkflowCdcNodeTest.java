package com.company.pipeline.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import com.company.pipeline.workflow.dto.WorkflowValidationResult.Issue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * 워크플로우 CDC 노드(2026-09-10 설계 «방안 B» - 부르고 끝낸다)의 게시 검증 V12 와 컴파일 결과.
 *
 * <p>CDC 노드는 제어 DAG 를 부르고 기다리지 않는다. 그래서 순서(연결선)도 스케줄도 뜻이 없고,
 * 순서가 있는 job·하위 워크플로우와 섞이면 그림과 실제 실행이 어긋난다 - 그걸 게시 전에 막는지 본다.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WorkflowCdcNodeTest {

    @Mock
    private EtlJobRepository jobRepository;
    @Mock
    private EtlJobStepRepository stepRepository;
    @Mock
    private NifiProcessGroupMetadataRepository pgRepository;
    @Mock
    private EtlWorkflowRepository workflowRepository;
    @Mock
    private EtlWorkflowNodeRepository nodeRepository;
    @Mock
    private PipelineDefinitionRepository pipelineRepository;

    private WorkflowValidator validator;
    private WorkflowCompiler compiler;

    @BeforeEach
    void setUp() {
        validator = new WorkflowValidator(jobRepository, workflowRepository, nodeRepository, pipelineRepository);
        compiler = new WorkflowCompiler(jobRepository, stepRepository, pgRepository, workflowRepository,
                pipelineRepository);
        when(pipelineRepository.findById(anyLong())).thenReturn(Optional.empty());
        stubPipeline(3L, "orders-cdc");
        stubPipeline(4L, "customers-cdc");
    }

    @Test
    void cdcOnlyWithoutEdgesOrSchedule_isValid() {
        WorkflowValidationResult result = validator.validate(workflow(null),
                List.of(cdcNode("cdc_orders", 3L), cdcNode("cdc_customers", 4L)), List.of());

        assertThat(result.errors()).isEmpty();
        assertThat(result.valid()).isTrue();
    }

    @Test
    void edgeTouchingCdcNode_isRejected() {
        WorkflowValidationResult result = validator.validate(workflow(null),
                List.of(cdcNode("cdc_orders", 3L), cdcNode("cdc_customers", 4L)),
                List.of(new EtlWorkflowEdge(1L, "cdc_orders", "cdc_customers", "SUCCESS", null)));

        assertThat(result.valid()).isFalse();
        assertThat(messagesOf(result, "V12")).anyMatch(m -> m.contains("연결할 수 없습니다"));
    }

    @Test
    void mixingJobNodeIntoCdcWorkflow_isRejectedOnTheMinority() {
        // CDC 2 · job 1 → 적은 쪽인 job 을 짚는다.
        EtlWorkflowNode job = jobNode("dw_orders");
        WorkflowValidationResult result = validator.validate(workflow(null),
                List.of(cdcNode("cdc_orders", 3L), cdcNode("cdc_customers", 4L), job), List.of());

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).anyMatch(issue -> "V12".equals(issue.code())
                && "dw_orders".equals(issue.nodeKey())
                && issue.message().contains("섞을 수 없습니다"));
        assertThat(result.errors()).noneMatch(issue -> "V12".equals(issue.code()) && "cdc_orders".equals(issue.nodeKey()));
    }

    @Test
    void cdcNodeDroppedIntoEtlWorkflow_marksTheCdcNode() {
        // 기존 ETL 워크플로우(job 2)에 CDC 하나를 잘못 놓은 경우 - 그 CDC 를 짚어야 한다(job 을 지우라고 하면 안 된다).
        WorkflowValidationResult result = validator.validate(workflow(null),
                List.of(jobNode("dw_a"), jobNode("dw_b"), cdcNode("cdc_orders", 3L)), List.of());

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).anyMatch(issue -> "V12".equals(issue.code()) && "cdc_orders".equals(issue.nodeKey()));
        assertThat(result.errors()).noneMatch(issue -> "V12".equals(issue.code()) && "dw_a".equals(issue.nodeKey()));

        // 스케줄 있는 ETL 워크플로우에 CDC 하나를 놓은 경우 - «스케줄을 지우라»는 안내가 섞이면 안 된다.
        WorkflowValidationResult scheduled = validator.validate(workflow("0 3 * * *"),
                List.of(jobNode("dw_a"), cdcNode("cdc_orders", 3L)), List.of());
        assertThat(messagesOf(scheduled, "V12")).noneMatch(m -> m.contains("스케줄"));
    }

    @Test
    void cdcWorkflowAsSubWorkflowNode_isRejected() {
        EtlWorkflow child = workflowWithId(41L, null, null);
        when(workflowRepository.findByIdAndDeletedAtIsNull(41L)).thenReturn(Optional.of(child));
        when(nodeRepository.findByWorkflowIdAndDeletedAtIsNull(41L)).thenReturn(List.of(cdcNode("cdc_s", 3L)));
        EtlWorkflowNode sub = new EtlWorkflowNode(42L, "wf_sub", EtlWorkflowNode.TYPE_SUBWF, null, 41L, null,
                null, null, null, null, null, null);

        WorkflowValidationResult result = validator.validate(workflowWithId(42L, "30 10 * * *", null), List.of(sub), List.of());

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).anyMatch(issue -> "V8".equals(issue.code()) && "wf_sub".equals(issue.nodeKey())
                && issue.message().contains("하위 워크플로우로 넣을 수 없습니다"));
    }

    @Test
    void cdcWorkflowAsUpstream_isRejected() {
        EtlWorkflow cdc = workflowWithId(41L, null, null);
        when(workflowRepository.findByIdAndDeletedAtIsNull(41L)).thenReturn(Optional.of(cdc));
        when(nodeRepository.findByWorkflowIdAndDeletedAtIsNull(41L)).thenReturn(List.of(cdcNode("cdc_s", 3L)));

        WorkflowValidationResult result = validator.validate(workflowWithId(43L, null, "[41]"),
                List.of(jobNode("dw_a")), List.of());

        assertThat(messagesOf(result, "V10")).anyMatch(m -> m.contains("선행으로 지정할 수 없습니다"));
    }

    @Test
    void cdcWorkflowWithUpstream_isRejected() {
        WorkflowValidationResult result = validator.validate(workflowWithId(41L, null, "[42]"),
                List.of(cdcNode("cdc_s", 3L)), List.of());

        assertThat(messagesOf(result, "V12")).anyMatch(m -> m.contains("선행 워크플로우를 둘 수 없습니다"));
    }

    @Test
    void workflowUsedAsSubOrUpstreamElsewhere_cannotBecomeCdc() {
        EtlWorkflow parent = workflowWithId(42L, "30 10 * * *", null);
        EtlWorkflow downstream = workflowWithId(44L, null, "[41]");
        EtlWorkflowNode parentNode = new EtlWorkflowNode(42L, "wf_sub", EtlWorkflowNode.TYPE_SUBWF, null, 41L, null,
                null, null, null, null, null, null);
        when(nodeRepository.findBySubWorkflowIdAndDeletedAtIsNull(41L)).thenReturn(List.of(parentNode));
        when(workflowRepository.findById(42L)).thenReturn(Optional.of(parent));
        when(workflowRepository.findByDeletedAtIsNullOrderByNameAsc()).thenReturn(List.of(parent, downstream));

        WorkflowValidationResult result = validator.validate(workflowWithId(41L, null, null),
                List.of(cdcNode("cdc_s", 3L)), List.of());

        assertThat(messagesOf(result, "V12"))
                .anyMatch(m -> m.contains("하위 노드로 쓰고 있어"))
                .anyMatch(m -> m.contains("선행으로 지정해"));
    }

    @Test
    void scheduleOnCdcWorkflow_isRejected() {
        WorkflowValidationResult result = validator.validate(workflow("0 2 * * *"),
                List.of(cdcNode("cdc_orders", 3L)), List.of());

        assertThat(result.valid()).isFalse();
        assertThat(messagesOf(result, "V12")).anyMatch(m -> m.contains("스케줄"));
    }

    @Test
    void deletedPipeline_isRejectedOnThatNode() {
        WorkflowValidationResult result = validator.validate(workflow(null),
                List.of(cdcNode("cdc_gone", 404L)), List.of());

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).anyMatch(issue -> "V12".equals(issue.code())
                && "cdc_gone".equals(issue.nodeKey()));
    }

    @Test
    void etlWorkflowWithoutCdcNodes_isNotAffectedByV12() {
        EtlWorkflowNode job = new EtlWorkflowNode(1L, "dw_orders", EtlWorkflowNode.TYPE_JOB, 99L, null, null,
                null, null, null, null, null, null);
        WorkflowValidationResult result = validator.validate(workflow("0 2 * * *"), List.of(job), List.of());

        assertThat(messagesOf(result, "V12")).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void compile_cdcNodePointsAtPipelineControlDag() {
        Map<String, Object> spec = compiler.compile(workflow(null),
                List.of(cdcNode("cdc_orders", 3L)), List.of());

        assertThat(spec.get("kind")).isEqualTo("CDC");
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) spec.get("nodes");
        assertThat(nodes).singleElement().satisfies(node -> {
            assertThat(node.get("type")).isEqualTo("CDC");
            assertThat(node.get("pipeline_id")).isEqualTo(3L);
            // 파이프라인별 제어 DAG 는 폐지됐다(2026-10-01) - 노드가 pipeline_id 로 직접 지시한다.
            assertThat(node).doesNotContainKey("cdc_dag_id");
            assertThat(node.get("name")).isEqualTo("orders-cdc");
        });
    }

    @Test
    void compile_etlWorkflowStaysEtlKind() {
        EtlWorkflowNode job = new EtlWorkflowNode(1L, "dw_orders", EtlWorkflowNode.TYPE_JOB, 99L, null, null,
                null, null, null, null, null, null);
        Map<String, Object> spec = compiler.compile(workflow(null), List.of(job), List.of());

        assertThat(spec.get("kind")).isEqualTo("ETL");
    }

    private void stubPipeline(Long id, String name) {
        PipelineDefinition pipeline = mock(PipelineDefinition.class);
        when(pipeline.getId()).thenReturn(id);
        when(pipeline.getName()).thenReturn(name);
        when(pipelineRepository.findById(id)).thenReturn(Optional.of(pipeline));
    }

    private static EtlWorkflow workflow(String cron) {
        EtlWorkflow workflow = new EtlWorkflow("cdc_bulk", "CDC 일괄");
        workflow.updateSettings("CDC 일괄", null, cron, "Asia/Seoul", false, 1, false);
        return workflow;
    }

    private static EtlWorkflow workflowWithId(Long id, String cron, String upstreamJson) {
        EtlWorkflow workflow = new EtlWorkflow("wf_" + id, "워크플로우 " + id);
        workflow.updateSettings("워크플로우 " + id, null, cron, "Asia/Seoul", false, 1, false,
                upstreamJson, upstreamJson == null ? null : "ALL");
        org.springframework.test.util.ReflectionTestUtils.setField(workflow, "id", id);
        return workflow;
    }

    private static EtlWorkflowNode jobNode(String key) {
        return new EtlWorkflowNode(1L, key, EtlWorkflowNode.TYPE_JOB, 99L, null, null,
                null, null, null, null, null, null);
    }

    private static EtlWorkflowNode cdcNode(String key, Long pipelineId) {
        return new EtlWorkflowNode(1L, key, EtlWorkflowNode.TYPE_CDC, null, null, pipelineId,
                null, null, null, null, null, null);
    }

    private static List<String> messagesOf(WorkflowValidationResult result, String code) {
        return result.errors().stream().filter(issue -> code.equals(issue.code())).map(Issue::message).toList();
    }
}
