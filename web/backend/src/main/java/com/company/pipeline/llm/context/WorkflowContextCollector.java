package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 워크플로우 «전체» 수집기 - 워크플로우 설계 화면에서 아무 노드도 고르지 않았을 때의 속성창 AI 어시스턴트(2026-09-18 요청).
 *
 * <p>노드 수집기(WORKFLOW_NODE)는 «노드 하나»를 설명해서 워크플로우 전체를 물으면 엉뚱한 틀(하위 노드 · 저장 안 된 노드)로 보냈다.
 * 여기서는 이름 · 상위 · 스케줄의 실제 동작 · 게시 · 메모와 노드 구성(job · 하위 워크플로우 · CDC)을 보낸다.
 *
 * <p>화면이 보내는 것: {@code workflowId}. 저장 전 초안은 대상이 없어 화면이 탭을 그리지 않는다.
 * CDC 노드(node_type=CDC, pipeline_id)는 실패하지 않고 파이프라인 이름으로 적는다 - CDC 워크플로우는 스케줄 · 선행 · 연결선 없이
 * 실행 때 고른 지시를 모든 CDC 노드에 보낸다(docs/2026-09-10-cdc-workflow-node-design.md 7절).
 */
@Component
public class WorkflowContextCollector implements LlmContextCollector {

    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlJobRepository jobRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final EtlJobContextText jobText;
    private final WorkflowGraphText graphText;

    public WorkflowContextCollector(EtlWorkflowRepository workflowRepository, EtlWorkflowNodeRepository nodeRepository,
                                    EtlJobRepository jobRepository, PipelineDefinitionRepository pipelineRepository,
                                    EtlJobContextText jobText, WorkflowGraphText graphText) {
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.jobRepository = jobRepository;
        this.pipelineRepository = pipelineRepository;
        this.jobText = jobText;
        this.graphText = graphText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.WORKFLOW;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        Long workflowId = requiredLong(target, "workflowId");
        List<EtlWorkflow> workflows = workflowRepository.findByDeletedAtIsNullOrderByNameAsc();
        Map<Long, EtlWorkflow> workflowById = workflows.stream()
                .collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));
        EtlWorkflow workflow = Optional.ofNullable(workflowById.get(workflowId)).orElseThrow(() ->
                new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND, "워크플로우를 찾을 수 없습니다: " + workflowId));
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflowId);

        LlmContext context = new LlmContext();
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);
        context.add("워크플로우", identity(workflow, nodes, workflowById), LlmContext.PRIORITY_IDENTITY);
        context.add("노드 구성", nodeLines(workflow, nodes, workflowById), LlmContext.PRIORITY_IDENTITY);
        nodes.stream()
                .map(EtlWorkflowNode::getJobId)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .map(jobRepository::findById)
                .flatMap(Optional::stream)
                .forEach(job -> context.add("job «" + job.getJobName() + "» 의 단계", jobText.steps(job), LlmContext.PRIORITY_DETAIL));
        return new CollectedTarget(workflow.getId() + "/*", workflow.getName(), context);
    }

    private String identity(EtlWorkflow workflow, List<EtlWorkflowNode> nodes, Map<Long, EtlWorkflow> workflowById) {
        List<String> lines = new ArrayList<>();
        lines.add("이름: " + workflow.getName());
        lines.add("설명: " + ContextText.orDash(workflow.getDescription()));
        List<EtlWorkflow> parents = nodeRepository.findBySubWorkflowIdAndDeletedAtIsNull(workflow.getId()).stream()
                .map(EtlWorkflowNode::getWorkflowId)
                .distinct()
                .map(workflowById::get)
                .filter(java.util.Objects::nonNull)
                .toList();
        List<String> path = graphText.parentPath(workflow.getId(), workflowById);
        lines.add("상위 워크플로우: " + (parents.isEmpty() ? "없음"
                : parents.stream().map(p -> p.getName() + (p.getPublishedAt() == null ? "(미게시)" : "")).collect(Collectors.joining(", "))
                + (path.isEmpty() ? "" : " · 경로 " + String.join(" > ", path))));
        boolean cdcWorkflow = !nodes.isEmpty() && nodes.stream().allMatch(n -> "CDC".equals(n.getNodeType()));
        boolean suppressed = parents.stream().anyMatch(p -> p.getPublishedAt() != null);
        if (cdcWorkflow) {
            lines.add("실행 방식: CDC 워크플로우 - 스케줄 · 선행 · 연결선 없이, 실행할 때 고른 지시(시작 · 중지 · 재시작 · 배포)를 모든 CDC 노드에 보낸다");
        } else {
            lines.add("스케줄(cron): " + ContextText.orDash(workflow.getScheduleCron())
                    + (suppressed ? " - 게시된 상위 워크플로우가 있어 이 스케줄은 돌지 않고 상위가 부를 때만 실행된다" : ""));
        }
        lines.add("게시: " + (workflow.getPublishedAt() == null ? "안 됨 (Airflow 에서 실행되지 않음)"
                : ContextText.time(workflow.getPublishedAt()) + " · " + ContextText.orDash(workflow.getPublishedBy())));
        lines.add("실패 시 중단: " + (workflow.isSuspendOnError() ? "예" : "아니오")
                + " · 동시 실행 수: " + workflow.getMaxActiveRuns() + " · 밀린 실행(catchup): " + (workflow.isCatchup() ? "예" : "아니오"));
        lines.add("메모: " + ContextText.orDash(workflow.getMemo()));
        return String.join("\n", lines);
    }

    private String nodeLines(EtlWorkflow workflow, List<EtlWorkflowNode> nodes, Map<Long, EtlWorkflow> workflowById) {
        if (nodes.isEmpty()) {
            return "(노드 없음)";
        }
        return nodes.stream()
                .map(node -> {
                    String name;
                    String kind;
                    if ("CDC".equals(node.getNodeType())) {
                        name = Optional.ofNullable(node.getPipelineId()).flatMap(pipelineRepository::findById)
                                .map(PipelineDefinition::getName).orElse(node.getNodeKey() + " (파이프라인 없음)");
                        kind = "CDC 파이프라인";
                    } else if (node.getSubWorkflowId() != null) {
                        name = graphText.label(node, node.getNodeKey(), workflowById);
                        kind = "하위 워크플로우";
                    } else if (node.getJobId() != null) {
                        name = jobRepository.findById(node.getJobId()).map(EtlJob::getJobName).orElse(node.getNodeKey() + " (job 없음)");
                        kind = "job";
                    } else {
                        name = node.getNodeKey();
                        kind = ContextText.orDash(node.getNodeType());
                    }
                    if ("CDC".equals(node.getNodeType())) {
                        return "- " + name + " (" + kind + ")";
                    }
                    WorkflowGraphText.Neighbors neighbors = graphText.neighbors(workflow.getId(), node.getNodeKey(), nodes, workflowById);
                    return "- " + name + " (" + kind + ")"
                            + (neighbors.predecessors().isEmpty() ? "" : " ← 선행: " + String.join(", ", neighbors.predecessors()))
                            + " · 실행 조건 " + ContextText.orDash(node.getTriggerRule());
                })
                .collect(Collectors.joining("\n"));
    }

    private static Long requiredLong(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        if (value == null || value.isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, key + " 가 필요합니다.");
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, key + " 형식이 올바르지 않습니다: " + value);
        }
    }
}
