package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class WorkflowTaskContextCollectorTest {

    private static final String ERROR = "AirflowException: NiFi job dw_pop_pop003l did not finish: FAILED";

    private EtlWorkflowRepository workflowRepository;
    private EtlWorkflowNodeRepository nodeRepository;
    private EtlJobRepository jobRepository;
    private WorkflowTaskContextCollector collector;

    @BeforeEach
    void setUp() {
        workflowRepository = mock(EtlWorkflowRepository.class);
        nodeRepository = mock(EtlWorkflowNodeRepository.class);
        jobRepository = mock(EtlJobRepository.class);
        EtlJobContextText jobText = mock(EtlJobContextText.class);
        when(jobText.steps(any())).thenReturn("단계 없음");
        when(jobText.recentRuns(any(), isNull())).thenReturn("실행 없음");
        WorkflowGraphText graphText = mock(WorkflowGraphText.class);
        when(graphText.parentPath(anyLong(), any())).thenReturn(List.of());
        when(graphText.neighbors(anyLong(), any(), any(), any())).thenReturn(new WorkflowGraphText.Neighbors(List.of(), List.of()));
        collector = new WorkflowTaskContextCollector(workflowRepository, nodeRepository, jobRepository, jobText, graphText);
    }

    private static EtlWorkflow workflow(long id, String key, String name, boolean deleted) {
        EtlWorkflow workflow = new EtlWorkflow(key, name);
        ReflectionTestUtils.setField(workflow, "id", id);
        if (deleted) {
            workflow.softDelete();
        }
        return workflow;
    }

    private static Map<String, String> target(String dagId) {
        return Map.of("dagId", dagId, "runId", "manual__2026-08-31T03:57:12+00:00", "taskId", "dw_pop_pop003l.await");
    }

    private static String sections(LlmContextCollector.CollectedTarget collected) {
        return collected.context().sections().stream()
                .map(section -> section.title() + ": " + section.body())
                .collect(Collectors.joining("\n"));
    }

    @Test
    void 지운_워크플로우의_실패도_해석한다_삭제_표시를_붙인다() {
        // 2026-10-06: 워크플로우 로그에 남은 지운 워크플로우의 실패를 해석하면 «워크플로우를 찾을 수 없습니다»로 막혔다.
        when(workflowRepository.findAll()).thenReturn(List.of(workflow(25L, "dw_pop_daily", "dw_pop_daily", true)));
        when(nodeRepository.findByWorkflowIdAndDeletedAtIsNull(25L)).thenReturn(List.of(new EtlWorkflowNode(
                25L, "dw_pop_pop003l", "JOB", 7L, null, null, "all_success", null, 0, 60, 0.0, 0.0)));
        EtlJob job = new EtlJob("pg-7", "pop003l");
        job.markDeleted();
        when(jobRepository.findById(7L)).thenReturn(Optional.of(job));

        LlmContextCollector.CollectedTarget collected = collector.collect(target("etl_wf_dw_pop_daily"), ERROR);

        assertThat(collected.label()).isEqualTo("dw_pop_daily · pop003l");
        assertThat(sections(collected)).contains("삭제된 워크플로우").contains("삭제된 job").contains("did not finish");
    }

    @Test
    void 같은_dag_id_가_여럿이면_살아_있는_것을_고른다() {
        // 예전에는 지운 워크플로우의 키를 다시 썼다(etl_wf_cdc: 37 · 39).
        when(workflowRepository.findAll()).thenReturn(List.of(
                workflow(37L, "cdc", "옛 CDC", true), workflow(39L, "cdc", "새 CDC", false), workflow(40L, "cdc", "더 옛 CDC", true)));

        LlmContextCollector.CollectedTarget collected = collector.collect(target("etl_wf_cdc"), ERROR);

        assertThat(collected.label()).startsWith("새 CDC");
        assertThat(sections(collected)).doesNotContain("삭제된 워크플로우");
    }

    @Test
    void 워크플로우_기록이_없으면_실패_로그만으로_해석한다() {
        when(workflowRepository.findAll()).thenReturn(List.of());

        LlmContextCollector.CollectedTarget collected = collector.collect(target("kafka_pipeline_6_control"), ERROR);

        assertThat(collected.targetId()).isEqualTo("kafka_pipeline_6_control/dw_pop_pop003l.await");
        assertThat(sections(collected)).contains("워크플로우 정보가 남아 있지 않습니다").contains("ETL 처리 완료 대기")
                .contains("did not finish");
    }

    @Test
    void 워크플로우_기록도_실패_로그도_없으면_찾을_수_없음() {
        when(workflowRepository.findAll()).thenReturn(List.of());

        assertThatThrownBy(() -> collector.collect(target("etl_wf_no_such"), " "))
                .isInstanceOf(BusinessException.class)
                .satisfies(ex -> assertThat(((BusinessException) ex).getErrorCode()).isEqualTo(ErrorCode.LLM_TARGET_NOT_FOUND));
    }
}
