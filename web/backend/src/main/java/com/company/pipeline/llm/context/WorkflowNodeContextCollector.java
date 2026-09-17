package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
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
 * 워크플로우 설계 화면(캔버스) 노드 속성창 GPT 탭의 정보 수집(대상 {@code WORKFLOW_NODE}).
 *
 * <p>화면이 보내는 것: {@code workflowId}(새로 만드는 중이면 없음) · {@code nodeKey} · {@code jobId} 또는 {@code subWorkflowId}.
 * <b>저장된 설계 기준</b>으로 모은다 - 캔버스에서 아직 저장하지 않은 연결 · 실행 조건은 반영되지 않는다(화면이 안내한다).
 * 저장 전에 놓은 노드도 job · 하위 워크플로우 자체는 이미 있는 것이라 그 정보는 보낸다.
 */
@Component
public class WorkflowNodeContextCollector implements LlmContextCollector {

    private static final String NOT_SAVED = "(이 노드는 아직 저장되지 않아 선행 · 후행 연결과 실행 조건 정보가 없다)";

    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlJobRepository jobRepository;
    private final EtlJobContextText jobText;
    private final WorkflowGraphText graphText;

    public WorkflowNodeContextCollector(EtlWorkflowRepository workflowRepository, EtlWorkflowNodeRepository nodeRepository,
                                       EtlJobRepository jobRepository, EtlJobContextText jobText, WorkflowGraphText graphText) {
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.jobRepository = jobRepository;
        this.jobText = jobText;
        this.graphText = graphText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.WORKFLOW_NODE;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        Long workflowId = optionalLong(target, "workflowId");
        String nodeKey = target.get("nodeKey");
        List<EtlWorkflow> workflows = workflowRepository.findByDeletedAtIsNullOrderByNameAsc();
        Map<Long, EtlWorkflow> workflowById = workflows.stream()
                .collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));

        Optional<EtlWorkflow> workflow = Optional.ofNullable(workflowId).map(workflowById::get);
        if (workflowId != null && workflow.isEmpty()) {
            throw new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND, "워크플로우를 찾을 수 없습니다: " + workflowId);
        }
        List<EtlWorkflowNode> nodes = workflow.map(w -> nodeRepository.findByWorkflowIdAndDeletedAtIsNull(w.getId()))
                .orElse(List.of());
        Optional<EtlWorkflowNode> saved = nodeKey == null ? Optional.empty()
                : nodes.stream().filter(n -> nodeKey.equals(n.getNodeKey())).findFirst();

        // 저장된 노드가 있으면 그 참조를, 없으면(저장 전) 화면이 보낸 참조를 쓴다. 둘 다 이미 있는 job · 워크플로우를 가리킨다.
        Long jobId = saved.map(EtlWorkflowNode::getJobId).orElse(optionalLong(target, "jobId"));
        Long subWorkflowId = saved.map(EtlWorkflowNode::getSubWorkflowId).orElse(optionalLong(target, "subWorkflowId"));
        Optional<EtlJob> job = Optional.ofNullable(jobId).flatMap(jobRepository::findById);
        Optional<EtlWorkflow> sub = Optional.ofNullable(subWorkflowId).map(workflowById::get);
        if (job.isEmpty() && sub.isEmpty()) {
            throw new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND,
                    "노드가 가리키는 job 이나 하위 워크플로우를 찾을 수 없습니다.");
        }

        LlmContext context = new LlmContext();
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);
        context.add("워크플로우 노드", identity(workflow, saved, nodes, workflowById, job, sub), LlmContext.PRIORITY_IDENTITY);
        job.ifPresent(j -> context.add("job «" + j.getJobName() + "» 의 단계", jobText.steps(j), LlmContext.PRIORITY_DETAIL));
        sub.ifPresent(s -> context.add("하위 워크플로우 «" + s.getName() + "» 의 구성", subWorkflow(s, workflowById),
                LlmContext.PRIORITY_DETAIL));
        job.ifPresent(j -> context.add("job «" + j.getJobName() + "» 의 최근 실행 (최신순)",
                jobText.recentRuns(j, null), LlmContext.PRIORITY_HISTORY));

        String label = job.map(EtlJob::getJobName).orElseGet(() -> sub.map(EtlWorkflow::getName).orElse(nodeKey));
        String targetId = workflow.map(w -> w.getId() + "/").orElse("draft/") + (nodeKey == null ? label : nodeKey);
        return new CollectedTarget(targetId, label, context);
    }

    private String identity(Optional<EtlWorkflow> workflow, Optional<EtlWorkflowNode> saved, List<EtlWorkflowNode> nodes,
                            Map<Long, EtlWorkflow> workflowById, Optional<EtlJob> job, Optional<EtlWorkflow> sub) {
        List<String> lines = new ArrayList<>();
        if (workflow.isPresent()) {
            EtlWorkflow w = workflow.get();
            lines.add("속한 워크플로우: " + w.getName());
            List<String> parents = graphText.parentPath(w.getId(), workflowById);
            if (!parents.isEmpty()) {
                lines.add("그 워크플로우의 상위: " + String.join(" > ", parents));
            }
            lines.add("스케줄(cron): " + ContextText.orDash(w.getScheduleCron())
                    + (parents.isEmpty() ? "" : " (상위가 있으면 이 스케줄은 돌지 않고 상위가 부를 때만 실행)"));
            lines.add("게시: " + (w.getPublishedAt() == null ? "안 됨 (Airflow 에서 실행되지 않음)" : ContextText.time(w.getPublishedAt())));
        } else {
            lines.add("속한 워크플로우: (아직 저장하지 않은 새 워크플로우)");
        }
        job.ifPresent(j -> lines.add("노드 유형: job 노드 - ETL job «" + j.getJobName() + "» 를 실행"));
        sub.ifPresent(s -> lines.add("노드 유형: 하위 워크플로우 노드 - «" + s.getName() + "» 를 실행하고 끝날 때까지 대기"));
        if (saved.isPresent() && workflow.isPresent()) {
            EtlWorkflowNode node = saved.get();
            WorkflowGraphText.Neighbors neighbors = graphText.neighbors(workflow.get().getId(), node.getNodeKey(), nodes, workflowById);
            lines.add("선행 노드: " + (neighbors.predecessors().isEmpty() ? "없음" : String.join(", ", neighbors.predecessors())));
            lines.add("후행 노드: " + (neighbors.successors().isEmpty() ? "없음" : String.join(", ", neighbors.successors())));
            lines.add("실행 조건(trigger rule): " + ContextText.orDash(node.getTriggerRule()));
            lines.add("재시도: " + node.getRetries() + "회, 간격 " + node.getRetryDelaySec() + "초");
        } else {
            lines.add(NOT_SAVED);
        }
        return String.join("\n", lines);
    }

    private String subWorkflow(EtlWorkflow sub, Map<Long, EtlWorkflow> workflowById) {
        return "스케줄(cron): " + ContextText.orDash(sub.getScheduleCron())
                + "\n게시: " + (sub.getPublishedAt() == null ? "안 됨" : ContextText.time(sub.getPublishedAt()))
                + "\n노드:\n" + graphText.nodeList(sub.getId(), workflowById);
    }

    private static Long optionalLong(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, key + " 형식이 올바르지 않습니다: " + value);
        }
    }
}
