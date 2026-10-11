package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.RunDetail;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.TaskInstance;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class WorkflowRunContextCollectorTest {

    private static final OffsetDateTime T0 = OffsetDateTime.of(2026, 10, 11, 9, 58, 56, 0, ZoneOffset.UTC);   // KST 18:58:56
    private static final String RUN = "manual__2026-10-11T09:58:56+00:00";
    private static final String CHILD_RUN = "manual__" + RUN + "__sub03";
    private static final String PREVIOUS = "manual__2026-10-11T09:19:59+00:00";

    private AirflowDagRunClient dagRunClient;
    private EtlWorkflowRepository workflowRepository;
    private EtlWorkflowNodeRepository nodeRepository;
    private EtlJobRepository jobRepository;
    private EtlJobRunRepository runRepository;
    private WorkflowRunContextCollector collector;

    @BeforeEach
    void setUp() {
        dagRunClient = mock(AirflowDagRunClient.class);
        workflowRepository = mock(EtlWorkflowRepository.class);
        nodeRepository = mock(EtlWorkflowNodeRepository.class);
        jobRepository = mock(EtlJobRepository.class);
        runRepository = mock(EtlJobRunRepository.class);
        WorkflowGraphText graphText = mock(WorkflowGraphText.class);
        when(graphText.neighbors(anyLong(), any(), any(), any())).thenReturn(new WorkflowGraphText.Neighbors(List.of(), List.of()));
        collector = new WorkflowRunContextCollector(dagRunClient, workflowRepository, nodeRepository, jobRepository,
                runRepository, mock(PipelineDefinitionRepository.class), graphText);
        when(runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(anyString())).thenReturn(List.of());
    }

    private static EtlWorkflow workflow(long id, String key, String name) {
        EtlWorkflow workflow = new EtlWorkflow(key, name);
        ReflectionTestUtils.setField(workflow, "id", id);
        return workflow;
    }

    private static EtlWorkflowNode node(long workflowId, String key, String type, Long jobId, Long subWorkflowId) {
        return new EtlWorkflowNode(workflowId, key, type, jobId, subWorkflowId, null, "all_success", null, 0, 60, 0.0, 0.0);
    }

    private static EtlJob job(long id, String name) {
        EtlJob job = new EtlJob("pg-" + id, name);
        ReflectionTestUtils.setField(job, "id", id);
        return job;
    }

    private static EtlJobRun ledger(String runId, String nodeKey, Long rows, String error) {
        EtlJobRun entry = new EtlJobRun(1L, LocalDateTime.of(2026, 10, 11, 18, 59));
        entry.adoptByWorkflow("token", "demo", nodeKey, runId, nodeKey + ".await");
        ReflectionTestUtils.setField(entry, "rowsProcessed", rows);
        ReflectionTestUtils.setField(entry, "errorMessage", error);
        return entry;
    }

    /** job 노드 하나의 다섯 단계. await 상태만 바꾼다. */
    private static List<TaskInstance> jobTasks(String key, int startSec, String awaitState) {
        List<TaskInstance> out = new ArrayList<>();
        String[] steps = {"open_run", "start_pg", "await", "stop_pg", "verify"};
        for (int i = 0; i < steps.length; i++) {
            String state = steps[i].equals("await") ? awaitState
                    : steps[i].equals("verify") && !"success".equals(awaitState) ? "upstream_failed" : "success";
            out.add(new TaskInstance(key + "." + steps[i], state, 1, T0.plusSeconds(startSec + i), T0.plusSeconds(startSec + i + 1)));
        }
        return out;
    }

    private static String sections(LlmContextCollector.CollectedTarget collected) {
        return collected.context().sections().stream()
                .map(section -> "## " + section.title() + "\n" + section.body())
                .collect(Collectors.joining("\n"));
    }

    @Test
    void 고른_차수의_노드별_결과_하위_워크플로우_적재_합계_실패_원인_직전_실행을_모은다() {
        when(workflowRepository.findAll()).thenReturn(List.of(workflow(1L, "demo", "ETL시연"), workflow(2L, "demo_dz_order_daily", "DEMO_DZ_ORDER_DAILY")));
        when(nodeRepository.findByWorkflowIdAndDeletedAtIsNull(1L)).thenReturn(List.of(
                node(1L, "job01", "JOB", 11L, null), node(1L, "job02", "JOB", 12L, null), node(1L, "sub03", "SUBWF", null, 2L)));
        when(nodeRepository.findByWorkflowIdAndDeletedAtIsNull(2L)).thenReturn(List.of(node(2L, "o1", "JOB", 21L, null)));
        when(jobRepository.findById(11L)).thenReturn(Optional.of(job(11L, "test_unstructed")));
        when(jobRepository.findById(12L)).thenReturn(Optional.of(job(12L, "test_Ontology_CL")));
        when(jobRepository.findById(21L)).thenReturn(Optional.of(job(21L, "DZ_ORDER_LOAD")));

        when(dagRunClient.getDagRun("etl_wf_demo", RUN)).thenReturn(
                new RunDetail(RUN, "etl_wf_demo", "failed", "manual", T0.minusSeconds(2), T0, T0.plusSeconds(85)));
        when(dagRunClient.getDagRun("etl_wf_demo_dz_order_daily", CHILD_RUN)).thenReturn(
                new RunDetail(CHILD_RUN, "etl_wf_demo_dz_order_daily", "success", "manual", null, T0.plusSeconds(15), T0.plusSeconds(80)));
        List<TaskInstance> parentTasks = new ArrayList<>(jobTasks("job01", 0, "success"));
        parentTasks.addAll(jobTasks("job02", 2, "failed"));
        parentTasks.add(new TaskInstance("sub03.run", "success", 1, T0.plusSeconds(14), T0.plusSeconds(81)));
        parentTasks.add(new TaskInstance("workflow_done", "upstream_failed", 0, null, null));
        when(dagRunClient.getTaskInstances("etl_wf_demo", RUN)).thenReturn(parentTasks);
        when(dagRunClient.getTaskInstances("etl_wf_demo_dz_order_daily", CHILD_RUN)).thenReturn(jobTasks("o1", 16, "success"));
        when(runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(RUN)).thenReturn(List.of(
                ledger(RUN, "job01", 18_047L, null), ledger(RUN, "job02", null, "ORA-00942: table or view does not exist")));
        when(runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(CHILD_RUN)).thenReturn(List.of(ledger(CHILD_RUN, "o1", 117_655L, null)));
        when(runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(PREVIOUS)).thenReturn(List.of(ledger(PREVIOUS, "job01", 18_048L, null)));
        when(dagRunClient.getDagRuns("etl_wf_demo")).thenReturn(List.of(
                new DagRun(RUN, "failed", T0, T0.plusSeconds(85), Map.of()),
                new DagRun(PREVIOUS, "success", T0.minusMinutes(39), T0.minusMinutes(37), Map.of())));
        when(dagRunClient.getTaskLogTail(eq("etl_wf_demo"), eq(RUN), eq("job02.await"), anyInt(), anyInt()))
                .thenReturn("ERROR 적재 실패\nORA-00942: table or view does not exist");

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("dagId", "etl_wf_demo", "runId", RUN), null);
        String text = sections(collected);

        assertThat(collected.label()).isEqualTo("ETL시연 · 10-11 18:58:56 · 2차");
        assertThat(collected.targetId()).isEqualTo("etl_wf_demo/" + RUN);
        assertThat(text)
                .contains("차수: 그날 2번째 실행 (2차)")
                .contains("실행 유형: 수동 실행")
                .contains("상태: 실패")
                .contains("소요: 1분 25초")
                .contains("적재 합계: 135,702행 (자체 job 18,047행 + 하위 워크플로우 117,655행)")
                .contains("01 test_unstructed (ETL job) | 성공")
                .contains("02 test_Ontology_CL (ETL job) | 실패")
                .contains("실패 단계 await")
                .contains("원장 오류: ORA-00942")
                .contains("03 DEMO_DZ_ORDER_DAILY (하위 워크플로우 호출) | 성공")
                .contains("    01 DZ_ORDER_LOAD (ETL job) | 성공")
                .contains("적재 117,655행")
                .contains("04 workflow_done (워크플로우 끝 표시) | 선행 실패로 미실행")
                .contains("[test_Ontology_CL · await]\nERROR 적재 실패")
                .contains("2026-10-11 18:19:56 | 성공 | 소요 2분 0초 | 자체 job 적재 18,048행");
    }

    @Test
    void 지운_워크플로우의_차수도_노드_키로_설명한다() {
        when(workflowRepository.findAll()).thenReturn(List.of());
        when(dagRunClient.getDagRun("etl_wf_gone", RUN)).thenReturn(
                new RunDetail(RUN, "etl_wf_gone", "success", "scheduled", null, T0, T0.plusSeconds(10)));
        when(dagRunClient.getTaskInstances("etl_wf_gone", RUN)).thenReturn(jobTasks("old_job", 0, "success"));
        when(dagRunClient.getDagRuns("etl_wf_gone")).thenReturn(List.of());

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("dagId", "etl_wf_gone", "runId", RUN), null);

        assertThat(collected.label()).isEqualTo("etl_wf_gone · 10-11 18:58:56");
        assertThat(sections(collected)).contains("01 old_job (ETL job) | 성공").contains("실행 유형: 스케줄")
                .contains("(이 차수 앞의 실행 기록 없음)");
    }

    @Test
    void 실행이_없으면_찾을_수_없음() {
        when(dagRunClient.getDagRun("etl_wf_demo", RUN)).thenReturn(null);

        assertThatThrownBy(() -> collector.collect(Map.of("dagId", "etl_wf_demo", "runId", RUN), null))
                .isInstanceOf(BusinessException.class);
    }
}
