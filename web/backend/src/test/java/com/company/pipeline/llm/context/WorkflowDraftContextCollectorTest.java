package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import com.company.pipeline.workflow.WorkflowValidator;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import com.company.pipeline.workflow.dto.WorkflowValidationResult.Issue;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WorkflowDraftContextCollectorTest {

    private final EtlWorkflowRepository workflowRepository = mock(EtlWorkflowRepository.class);
    private final EtlJobRepository jobRepository = mock(EtlJobRepository.class);
    private final PipelineDefinitionRepository pipelineRepository = mock(PipelineDefinitionRepository.class);
    private final WorkflowValidator validator = mock(WorkflowValidator.class);
    private final EtlJobContextText jobText = mock(EtlJobContextText.class);
    private final WorkflowGraphText graphText = mock(WorkflowGraphText.class);
    private final WorkflowDraftContextCollector collector = new WorkflowDraftContextCollector(new ObjectMapper(),
            workflowRepository, jobRepository, pipelineRepository, validator, jobText, graphText);

    @Test
    void draft_describesSettingsGraphValidationAndJobSteps() {
        EtlJob jobA = mock(EtlJob.class);
        when(jobA.getJobName()).thenReturn("load_a");
        EtlJob jobB = mock(EtlJob.class);
        when(jobB.getJobName()).thenReturn("load_b");
        when(jobRepository.findById(1L)).thenReturn(Optional.of(jobA));
        when(jobRepository.findById(2L)).thenReturn(Optional.of(jobB));
        when(jobText.steps(any())).thenReturn("1. SELECT ...");
        when(workflowRepository.findByDeletedAtIsNullOrderByNameAsc()).thenReturn(List.of());
        when(validator.validate(any(), anyList(), anyList())).thenReturn(WorkflowValidationResult.of(
                List.of(), List.of(Issue.at("V2", "연결되지 않은 노드입니다.", "n3"))));

        String draft = """
                {"name":"daily_new","scheduleCron":"30 10 * * *","timezone":"Asia/Seoul","suspendOnError":true,"maxActiveRuns":1,
                 "nodes":[{"nodeKey":"n1","nodeType":"JOB","jobId":1,"triggerRule":"ALL_SUCCESS","retries":0,"label":"x"},
                          {"nodeKey":"n2","nodeType":"JOB","jobId":2,"triggerRule":"ALL_DONE","retries":2,"retryDelaySec":60},
                          {"nodeKey":"n3","nodeType":"JOB","label":"missing","triggerRule":"ALL_SUCCESS"}],
                 "edges":[{"fromNodeKey":"n1","toNodeKey":"n2","conditionType":"SUCCESS"}],
                 "unknownField":1}
                """;
        LlmContextCollector.CollectedTarget target = collector.collect(Map.of("mode", "draft"), null, draft);

        String text = target.context().assemble(60_000).text();
        assertThat(target.label()).isEqualTo("daily_new");
        assertThat(target.targetId()).isEqualTo("draft/*");
        assertThat(text)
                .contains("## 작성 중인 새 워크플로우 (저장 전)", "이름: daily_new", "스케줄(cron): 30 10 * * * · 시간대 Asia/Seoul")
                .contains("- load_b (job) ← 선행: load_a · 실행 조건 ALL_DONE · 재시도 2회 (간격 60초)")
                .contains("먼저 시작하는 노드(선행 없음): load_a, missing", "마지막 노드(후행 없음): load_b, missing",
                        "어디에도 연결되지 않은 노드: missing")
                .contains("오류 없음 (게시 가능)", "- 경고: 연결되지 않은 노드입니다. [노드 missing]")
                .contains("## job «load_a» 의 단계", "## job «load_b» 의 단계");
    }

    @Test
    void cdcDraftNodes_areNamedByPipeline_notRejected() {
        PipelineDefinition pipeline = mock(PipelineDefinition.class);
        when(pipeline.getName()).thenReturn("AAA-tb_imp018m");
        when(pipelineRepository.findById(2L)).thenReturn(Optional.of(pipeline));
        when(pipelineRepository.findById(3L)).thenReturn(Optional.empty());
        when(workflowRepository.findByDeletedAtIsNullOrderByNameAsc()).thenReturn(List.of());
        when(validator.validate(any(), anyList(), anyList())).thenReturn(WorkflowValidationResult.of(List.of(), List.of()));

        String draft = """
                {"name":"cdc_draft","nodes":[
                  {"nodeKey":"cdc_a","nodeType":"CDC","jobId":null,"subWorkflowId":null,"pipelineId":2,"label":"cdc_a"},
                  {"nodeKey":"cdc_b","nodeType":"CDC","jobId":null,"subWorkflowId":null,"pipelineId":3,"label":"cdc_b"}],
                 "edges":[]}
                """;
        String text = collector.collect(Map.of("mode", "draft"), null, draft).context().assemble(60_000).text();

        assertThat(text).contains("실행 방식: CDC 워크플로우", "- AAA-tb_imp018m (CDC 파이프라인)", "- cdc_b (CDC 파이프라인)", "노드 2개 · 연결 0개")
                .doesNotContain("먼저 시작하는 노드", "## job");
    }

    @Test
    void emptyName_andEmptyCanvas_areSpelledOut() {
        when(workflowRepository.findByDeletedAtIsNullOrderByNameAsc()).thenReturn(List.of());
        when(validator.validate(any(), anyList(), anyList())).thenReturn(WorkflowValidationResult.of(
                List.of(Issue.of("V0", "노드가 하나도 없습니다.")), List.of()));

        String text = collector.collect(Map.of("mode", "draft"), null, "{\"name\":\"  \",\"nodes\":[],\"edges\":[]}")
                .context().assemble(60_000).text();

        assertThat(text).contains("이름: (비어 있음", "(노드 없음", "오류 1건", "- 오류: 노드가 하나도 없습니다.");
    }

    @Test
    void missingOrBrokenDraft_isRejected() {
        assertThatThrownBy(() -> collector.collect(Map.of("mode", "draft"), null, null)).isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> collector.collect(Map.of("mode", "draft"), null, "{not json")).isInstanceOf(BusinessException.class);
    }
}
