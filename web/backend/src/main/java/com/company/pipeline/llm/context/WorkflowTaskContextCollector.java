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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 워크플로우 로그 실패 로그 창에서 부르는 오류 분석의 정보 수집(대상 {@code WORKFLOW_TASK}).
 *
 * <p>화면이 보내는 것: {@code dagId · runId · taskId} 와 창에 보이는 실패 원인·Airflow 기록. 실패 로그는 화면이 이미 잡음을
 * 걸러 냈으므로(utils/airflowFailureLog) 그대로 받고, 서버는 «이 task 가 무엇이었는지»를 붙인다:
 * 워크플로우 이름·경로·스케줄, 노드가 부르는 job 의 단계 SQL·대상 테이블, 선행 노드, 그 job 의 최근 실행.
 *
 * <p>task id 규칙은 DAG 생성기(etl_workflows_dynamic.py) 그대로다: {@code {노드키}.open_run|start_pg|await|stop_pg|verify}
 * (job 노드), {@code {노드키}.run} (하위 워크플로우 노드).
 *
 * <p>연결정보(DB 호스트 · 계정 · 비밀번호)는 읽지 않는다. 단계의 SQL · 테이블 이름까지만 보낸다.
 */
@Component
public class WorkflowTaskContextCollector implements LlmContextCollector {

    private static final Map<String, String> PHASE_DESCRIPTION = Map.of(
            "open_run", "실행 기록 열기 (Cerebro 실행 원장에 이번 실행을 등록)",
            "start_pg", "ETL 그룹 시작 (job 의 SQL 단계들이 ETL에서 돌기 시작)",
            "await", "ETL 처리 완료 대기 (ETL 오류 알림을 감시하고, 끝나면 적재 건수를 센다)",
            "stop_pg", "ETL 그룹 정지 (성패와 관계없이 항상 실행)",
            "verify", "적재 확인 (실행 원장이 성공으로 닫혔는지 확인)",
            "run", "하위 워크플로우 DAG 를 실행하고 끝날 때까지 대기",
            "marker", "시작/끝/분기 표시 노드");

    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlJobRepository jobRepository;
    private final EtlJobContextText jobText;
    private final WorkflowGraphText graphText;

    public WorkflowTaskContextCollector(EtlWorkflowRepository workflowRepository, EtlWorkflowNodeRepository nodeRepository,
                                        EtlJobRepository jobRepository, EtlJobContextText jobText, WorkflowGraphText graphText) {
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.jobRepository = jobRepository;
        this.jobText = jobText;
        this.graphText = graphText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.WORKFLOW_TASK;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String dagId = ContextText.required(target, "dagId");
        String runId = ContextText.required(target, "runId");
        String taskId = ContextText.required(target, "taskId");
        int dot = taskId.lastIndexOf('.');
        String nodeKey = dot > 0 ? taskId.substring(0, dot) : taskId;
        String phase = dot > 0 ? taskId.substring(dot + 1) : taskId;

        LlmContext context = new LlmContext();
        String error = ContextText.errorText(errorText);
        context.add("화면에 표시된 실패 로그", error, LlmContext.PRIORITY_ERROR);

        // 워크플로우 로그는 Airflow 실행 기록을 그대로 보여 줘서 지운 워크플로우의 실패도 남는다. 지운 워크플로우도 해석할 수 있어야 한다
        // (2026-10-06 - 살아 있는 워크플로우에서만 찾아 «찾을 수 없음»으로 막혔다). 기록마저 없으면 실패 로그만으로 해석한다.
        List<EtlWorkflow> all = workflowRepository.findAll();
        Optional<EtlWorkflow> found = findByDagId(all, dagId);
        if (found.isEmpty()) {
            if (error == null) {
                throw new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND, "워크플로우를 찾을 수 없습니다: " + dagId);
            }
            context.add("실패한 작업", unknownWorkflow(dagId, runId, taskId, phase), LlmContext.PRIORITY_IDENTITY);
            return new CollectedTarget(dagId + "/" + taskId, dagId + " · " + nodeKey, context);
        }
        EtlWorkflow workflow = found.get();

        // 지운 워크플로우의 노드는 그대로 남는다(워크플로우만 deleted_at) - 삭제 직전 설계로 설명한다.
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflow.getId());
        Optional<EtlWorkflowNode> node = nodes.stream().filter(n -> nodeKey.equals(n.getNodeKey())).findFirst();
        Map<Long, EtlWorkflow> workflowById = all.stream()
                .filter(w -> w.getDeletedAt() == null || w.getId().equals(workflow.getId()))
                .collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));
        Optional<EtlJob> job = node.map(EtlWorkflowNode::getJobId).filter(Objects::nonNull).flatMap(jobRepository::findById);

        context.add("실패한 작업", identity(workflow, workflowById, runId, taskId, phase, node, job), LlmContext.PRIORITY_IDENTITY);
        node.ifPresent(n -> context.add("노드 구성", nodeShape(n, nodes, workflow.getId(), workflowById, job),
                LlmContext.PRIORITY_IDENTITY));
        job.ifPresent(j -> context.add("job «" + j.getJobName() + "» 의 단계", jobText.steps(j), LlmContext.PRIORITY_DETAIL));
        job.ifPresent(j -> context.add("job «" + j.getJobName() + "» 의 최근 실행 (최신순)", jobText.recentRuns(j, null),
                LlmContext.PRIORITY_HISTORY));

        String label = job.map(EtlJob::getJobName)
                .or(() -> node.map(EtlWorkflowNode::getSubWorkflowId).map(workflowById::get).map(EtlWorkflow::getName))
                .orElse(nodeKey);
        return new CollectedTarget(dagId + "/" + taskId, workflow.getName() + " · " + label, context);
    }

    /**
     * dag_id 로 워크플로우를 찾는다 - 지운 것까지. 예전에는 지운 워크플로우의 키를 다시 썼기 때문에(지금은 막음 -
     * {@link EtlWorkflowRepository#existsByWorkflowKey}) 같은 dag_id 가 여럿일 수 있다: 살아 있는 것, 없으면 마지막에 만든 것.
     */
    private static Optional<EtlWorkflow> findByDagId(List<EtlWorkflow> all, String dagId) {
        return all.stream()
                .filter(w -> dagId.equals(w.dagId()))
                .max(Comparator.comparing((EtlWorkflow w) -> w.getDeletedAt() == null).thenComparing(EtlWorkflow::getId));
    }

    private static String unknownWorkflow(String dagId, String runId, String taskId, String phase) {
        return "Airflow DAG: " + dagId
                + "\nAirflow 실행 id: " + runId
                + "\nAirflow task id: " + taskId
                + "\n실패한 단계: " + phase + " - " + PHASE_DESCRIPTION.getOrDefault(phase, "알 수 없는 단계")
                + "\n(이 DAG 의 워크플로우 정보가 남아 있지 않습니다. 화면의 실패 로그만으로 분석하십시오.)";
    }

    private String identity(EtlWorkflow workflow, Map<Long, EtlWorkflow> workflowById, String runId, String taskId,
                            String phase, Optional<EtlWorkflowNode> node, Optional<EtlJob> job) {
        List<String> lines = new ArrayList<>();
        lines.add("워크플로우: " + workflow.getName());
        if (workflow.getDeletedAt() != null) {
            lines.add("상태: 삭제된 워크플로우(" + ContextText.time(workflow.getDeletedAt())
                    + " 삭제) - 이 실행은 삭제 전 기록이고, 아래 구성은 삭제 직전 설계입니다");
        }
        List<String> parents = graphText.parentPath(workflow.getId(), workflowById);
        if (!parents.isEmpty()) {
            lines.add("상위 워크플로우: " + String.join(" > ", parents));
        }
        lines.add("스케줄(cron): " + ContextText.orDash(workflow.getScheduleCron()));
        lines.add("Airflow 실행 id: " + runId);
        lines.add("Airflow task id: " + taskId);
        lines.add("실패한 단계: " + phase + " - " + PHASE_DESCRIPTION.getOrDefault(phase, "알 수 없는 단계"));
        job.ifPresent(j -> lines.add("노드가 실행하는 ETL job: " + j.getJobName()
                + (j.getDeletedAt() == null ? "" : " (삭제된 job - ETL 에는 이 그룹이 더 이상 없을 수 있습니다)")));
        if (node.isEmpty()) {
            lines.add("(이 task 에 해당하는 노드를 현재 워크플로우 설계에서 찾지 못했다 - 실행 뒤 설계가 바뀌었을 수 있다)");
        }
        return String.join("\n", lines);
    }

    private String nodeShape(EtlWorkflowNode node, List<EtlWorkflowNode> nodes, Long workflowId,
                             Map<Long, EtlWorkflow> workflowById, Optional<EtlJob> job) {
        List<String> predecessors = graphText.neighbors(workflowId, node.getNodeKey(), nodes, workflowById).predecessors();
        List<String> lines = new ArrayList<>();
        lines.add("노드 유형: " + node.getNodeType());
        if (node.getSubWorkflowId() != null) {
            lines.add("하위 워크플로우: " + Optional.ofNullable(workflowById.get(node.getSubWorkflowId()))
                    .map(EtlWorkflow::getName).orElse("(삭제됨)"));
        }
        lines.add("선행 노드: " + (predecessors.isEmpty() ? "없음" : String.join(", ", predecessors)));
        lines.add("실행 조건(trigger rule): " + ContextText.orDash(node.getTriggerRule()));
        lines.add("재시도: " + node.getRetries() + "회, 간격 " + node.getRetryDelaySec() + "초");
        return String.join("\n", lines);
    }
}
