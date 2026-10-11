package com.company.pipeline.llm.context;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.RunDetail;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.TaskInstance;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.llm.context.WorkflowRunFacts.NodeRun;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 워크플로우 로그 화면 AI 어시스턴트 - 가운데 목록에서 고른 «실행 차수» 한 건(2026-10-11 요청: 워크플로우 설명이 아니라 고른 회차 기준으로 답한다).
 *
 * <p>화면이 보내는 것: {@code dagId · runId}. 서버가 모으는 것:
 * <ul>
 *   <li>그 차수: 상태 · 실행 유형 · 대기열/시작/종료 · 소요 · 그날 몇 번째 실행인지(화면 «N차»와 같은 규칙)</li>
 *   <li>노드별 결과: Airflow task(노드 × 단계)를 노드로 묶어 상태 · 소요 · 재시도, job 은 실행 원장의 적재 건수 · 오류,
 *       하위 워크플로우는 그 하위 실행의 노드까지(화면 상세 이력과 같은 «호출 → 하위 job» 구조)</li>
 *   <li>적재 합계(하위 포함 - 화면 실행 줄 합계와 같다)와 실패한 단계 로그의 끝부분</li>
 *   <li>같은 워크플로우의 직전 실행들(비교용)</li>
 * </ul>
 * 노드 이름은 지금 설계에서 찾는다 - 실행 뒤 노드를 지웠으면 노드 키로 남긴다. 지운 워크플로우의 실행도 설명한다(실패 로그 창과 같은 규칙).
 */
@Component
public class WorkflowRunContextCollector implements LlmContextCollector {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRunContextCollector.class);
    /** 비교용으로 붙이는 직전 실행 수. */
    private static final int PREVIOUS_RUNS = 5;
    /** 로그를 붙이는 실패 단계 수 · 단계마다 줄 수. 원인은 보통 끝부분에 있다. */
    private static final int FAILED_LOGS = 3;
    private static final int LOG_TAIL_LINES = 25;
    /** 하위 워크플로우를 따라 들어가는 깊이(상위 → 하위 → 그 하위). */
    private static final int MAX_DEPTH = 3;

    private final AirflowDagRunClient dagRunClient;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlJobRepository jobRepository;
    private final EtlJobRunRepository runRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final WorkflowGraphText graphText;

    public WorkflowRunContextCollector(AirflowDagRunClient dagRunClient, EtlWorkflowRepository workflowRepository,
                                       EtlWorkflowNodeRepository nodeRepository, EtlJobRepository jobRepository,
                                       EtlJobRunRepository runRepository, PipelineDefinitionRepository pipelineRepository,
                                       WorkflowGraphText graphText) {
        this.dagRunClient = dagRunClient;
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.jobRepository = jobRepository;
        this.runRepository = runRepository;
        this.pipelineRepository = pipelineRepository;
        this.graphText = graphText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.WORKFLOW_RUN;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String dagId = ContextText.required(target, "dagId");
        String runId = ContextText.required(target, "runId");
        RunDetail run = dagRunClient.getDagRun(dagId, runId);
        if (run == null) {
            throw new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND, "실행을 찾을 수 없습니다: " + dagId + " / " + runId);
        }
        List<EtlWorkflow> all = workflowRepository.findAll();
        Map<String, EtlWorkflow> workflowByDag = byDagId(all);
        Map<Long, EtlWorkflow> workflowById = all.stream().collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));
        Optional<EtlWorkflow> workflow = Optional.ofNullable(workflowByDag.get(dagId));
        String workflowName = workflow.map(EtlWorkflow::getName).orElse(dagId);

        List<DagRun> recent = safeRecentRuns(dagId);
        int ordinal = WorkflowRunFacts.ordinal(recent, runId, run.startDate());

        LlmContext context = new LlmContext();
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);

        Totals totals = new Totals();
        List<FailedStep> failed = new ArrayList<>();
        String nodeLines = runLines(dagId, runId, workflow.orElse(null), workflowById, workflowByDag, 0, totals, failed);

        context.add("선택한 실행 차수", identity(workflowName, workflow.orElse(null), run, ordinal, totals), LlmContext.PRIORITY_IDENTITY);
        context.add("노드별 결과 (실행 순서, 하위 워크플로우는 들여 씀)", nodeLines, LlmContext.PRIORITY_IDENTITY);
        context.add("실패한 단계의 로그 끝부분", failedLogs(failed), LlmContext.PRIORITY_ERROR);
        context.add("같은 워크플로우의 직전 실행 (최신순, 비교용)", previousRuns(recent, run), LlmContext.PRIORITY_HISTORY);
        workflow.ifPresent(w -> context.add("워크플로우 설계 (지금 기준)",
                design(w, workflowById), LlmContext.PRIORITY_DETAIL));

        String label = workflowName + (run.startDate() == null ? "" : " · " + WorkflowRunFacts.stamp(run.startDate()).substring(5))
                + (ordinal > 0 ? " · " + ordinal + "차" : "");
        return new CollectedTarget(dagId + "/" + runId, label, context);
    }

    // ------------------------------------------------------------------ 차수

    private String identity(String workflowName, EtlWorkflow workflow, RunDetail run, int ordinal, Totals totals) {
        List<String> lines = new ArrayList<>();
        lines.add("워크플로우: " + workflowName);
        if (workflow != null && workflow.getDeletedAt() != null) {
            lines.add("(삭제된 워크플로우의 실행 기록이다 - " + ContextText.time(workflow.getDeletedAt()) + " 삭제)");
        }
        lines.add("실행 id: " + run.dagRunId());
        lines.add("차수: " + (ordinal > 0 ? "그날 " + ordinal + "번째 실행 (" + ordinal + "차)" : "확인되지 않음"));
        lines.add("실행 유형: " + WorkflowRunFacts.runTypeLabel(run.runType(), run.dagRunId()));
        lines.add("상태: " + WorkflowRunFacts.stateLabel(run.state()));
        lines.add("대기열 등록: " + WorkflowRunFacts.stamp(run.queuedAt()));
        lines.add("시작: " + WorkflowRunFacts.stamp(run.startDate()) + " · 종료: " + WorkflowRunFacts.stamp(run.endDate()));
        lines.add(run.endDate() != null
                ? "소요: " + WorkflowRunFacts.duration(run.startDate(), run.endDate())
                : "경과(아직 안 끝남): " + WorkflowRunFacts.duration(run.startDate(), OffsetDateTime.now()));
        lines.add("적재 합계: " + (totals.known ? String.format("%,d행", totals.own + totals.sub) : "확인되지 않음")
                + (totals.sub > 0 ? String.format(" (자체 job %,d행 + 하위 워크플로우 %,d행)", totals.own, totals.sub) : "")
                + (totals.live ? " - 실행 중이라 지금까지 건수" : ""));
        return String.join("\n", lines);
    }

    /**
     * 실행 한 건의 노드 줄들. 하위 워크플로우 노드는 그 하위 실행으로 들어가 한 칸 들여 적는다.
     * 적재 건수는 실행 원장(job 노드)에서 읽고, 하위 실행 건수는 {@code totals.sub} 로 모은다.
     */
    private String runLines(String dagId, String runId, EtlWorkflow workflow, Map<Long, EtlWorkflow> workflowById,
                            Map<String, EtlWorkflow> workflowByDag, int depth, Totals totals, List<FailedStep> failed) {
        List<TaskInstance> tasks = safeTasks(dagId, runId);
        if (tasks.isEmpty()) {
            return indent(depth) + "(이 실행의 단계를 읽지 못했다)";
        }
        Map<String, EtlWorkflowNode> nodeByKey = workflow == null ? Map.of()
                : nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflow.getId()).stream()
                        .collect(Collectors.toMap(EtlWorkflowNode::getNodeKey, Function.identity(), (a, b) -> a));
        Map<String, EtlJobRun> ledger = new LinkedHashMap<>();
        runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(runId).stream()
                .filter(entry -> entry.getNodeKey() != null)
                .forEach(entry -> ledger.put(entry.getNodeKey(), entry));   // 재시도로 여러 번 열렸으면 마지막 것

        List<String> lines = new ArrayList<>();
        int index = 0;
        for (NodeRun node : WorkflowRunFacts.nodes(tasks)) {
            index++;
            String prefix = indent(depth) + String.format("%02d ", index);
            if (node.isEnd()) {
                lines.add(prefix + node.nodeKey() + " (워크플로우 끝 표시) | " + WorkflowRunFacts.stateLabel(node.state()));
                continue;
            }
            EtlWorkflowNode design = nodeByKey.get(node.nodeKey());
            String common = " | " + WorkflowRunFacts.stateLabel(node.state())
                    + " | 시작 " + WorkflowRunFacts.clock(node.startedAt())
                    + " · 소요 " + WorkflowRunFacts.duration(node.startedAt(), node.endedAt())
                    + " | 재시도 " + node.retries() + "회"
                    + (node.isRemoved() ? " | 이 차수 실행 뒤 게시로 설계에서 빠진 노드" : "")
                    + (node.failedSteps().isEmpty() ? "" : " | 실패 단계 " + String.join(", ", node.failedSteps()));
            node.steps().stream().filter(step -> "failed".equals(step.state()))
                    .forEach(step -> failed.add(new FailedStep(dagId, runId, step, nameOf(node.nodeKey(), design, workflowById))));

            if (design != null && design.getSubWorkflowId() != null || design == null && isCall(node)) {
                EtlWorkflow child = design == null ? null : workflowById.get(design.getSubWorkflowId());
                String childName = child != null ? child.getName() : node.nodeKey();
                lines.add(prefix + childName + " (하위 워크플로우 호출)" + common);
                if (child != null && depth + 1 < MAX_DEPTH) {
                    Totals childTotals = new Totals();
                    String childRunId = WorkflowRunFacts.childRunId(runId, node.nodeKey());
                    RunDetail childRun = safeRun(child.dagId(), childRunId);
                    if (childRun == null) {
                        lines.add(indent(depth + 1) + "(하위 실행을 찾지 못했다 - 아직 시작 전이거나 기록이 지워졌다)");
                    } else {
                        lines.add(indent(depth + 1) + "하위 실행 " + childRunId + " | " + WorkflowRunFacts.stateLabel(childRun.state())
                                + " | 소요 " + WorkflowRunFacts.duration(childRun.startDate(), childRun.endDate()));
                        lines.add(runLines(child.dagId(), childRunId, child, workflowById, workflowByDag, depth + 1, childTotals, failed));
                        totals.addSub(childTotals);
                        lines.add(indent(depth + 1) + "하위 워크플로우 적재 합계: "
                                + (childTotals.known ? String.format("%,d행", childTotals.own + childTotals.sub) : "확인되지 않음"));
                    }
                }
                continue;
            }
            if (design != null && "CDC".equals(design.getNodeType())) {
                lines.add(prefix + nameOf(node.nodeKey(), design, workflowById) + " (CDC 파이프라인 지시)" + common);
                continue;
            }
            EtlJobRun entry = ledger.get(node.nodeKey());
            Long rows = entry == null ? null
                    : entry.getRowsProcessed() != null ? entry.getRowsProcessed()
                    : entry.getTotalInserted() > 0 ? Long.valueOf(entry.getTotalInserted()) : null;
            boolean live = "running".equals(node.state());
            if (rows != null) {
                totals.own += rows;
                totals.known = true;
                totals.live |= live;
            }
            lines.add(prefix + nameOf(node.nodeKey(), design, workflowById) + " (ETL job)" + common
                    + " | 적재 " + (rows == null ? "확인되지 않음" : String.format("%,d행", rows) + (live ? "(진행 중)" : ""))
                    + (entry != null && entry.getErrorMessage() != null && !entry.getErrorMessage().isBlank()
                            ? " | 원장 오류: " + ContextText.cut(entry.getErrorMessage(), 300) : ""));
        }
        return String.join("\n", lines);
    }

    private static boolean isCall(NodeRun node) {
        return node.steps().size() == 1 && node.steps().get(0).taskId().endsWith(".run");
    }

    private String nameOf(String nodeKey, EtlWorkflowNode design, Map<Long, EtlWorkflow> workflowById) {
        if (design == null) {
            return nodeKey;
        }
        if ("CDC".equals(design.getNodeType())) {
            return Optional.ofNullable(design.getPipelineId()).flatMap(pipelineRepository::findById)
                    .map(PipelineDefinition::getName).orElse(nodeKey);
        }
        if (design.getSubWorkflowId() != null) {
            return Optional.ofNullable(workflowById.get(design.getSubWorkflowId())).map(EtlWorkflow::getName).orElse(nodeKey);
        }
        if (design.getJobId() != null) {
            return jobRepository.findById(design.getJobId()).map(EtlJob::getJobName).orElse(nodeKey);
        }
        return nodeKey;
    }

    private String failedLogs(List<FailedStep> failed) {
        if (failed.isEmpty()) {
            return null;
        }
        List<String> blocks = new ArrayList<>();
        for (FailedStep step : failed.subList(0, Math.min(FAILED_LOGS, failed.size()))) {
            String tail = dagRunClient.getTaskLogTail(step.dagId(), step.runId(), step.task().taskId(),
                    step.task().tryNumber() == null ? 1 : step.task().tryNumber(), LOG_TAIL_LINES);
            blocks.add("[" + step.name() + " · " + WorkflowRunFacts.stepName(step.task().taskId()) + "]\n"
                    + (tail == null || tail.isBlank() ? "(로그를 읽지 못했다)" : tail));
        }
        if (failed.size() > FAILED_LOGS) {
            blocks.add("(실패 단계 " + (failed.size() - FAILED_LOGS) + "개는 로그를 생략했다)");
        }
        return String.join("\n\n", blocks);
    }

    /** 이 차수보다 먼저 시작한 실행들. 자체 job 적재 건수는 원장에서 센다(하위 워크플로우 건수는 빼고). */
    private String previousRuns(List<DagRun> recent, RunDetail run) {
        if (run.startDate() == null) {
            return null;
        }
        List<DagRun> before = recent.stream()
                .filter(r -> r.startDate() != null && r.startDate().isBefore(run.startDate()))
                .sorted(Comparator.comparing(DagRun::startDate).reversed())
                .limit(PREVIOUS_RUNS)
                .toList();
        if (before.isEmpty()) {
            return "(이 차수 앞의 실행 기록 없음)";
        }
        return before.stream()
                .map(r -> {
                    long rows = runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(r.dagRunId()).stream()
                            .map(EtlJobRun::getRowsProcessed).filter(Objects::nonNull).mapToLong(Long::longValue).sum();
                    return WorkflowRunFacts.stamp(r.startDate()) + " | " + WorkflowRunFacts.stateLabel(r.state())
                            + " | 소요 " + WorkflowRunFacts.duration(r.startDate(), r.endDate())
                            + " | 자체 job 적재 " + String.format("%,d행", rows);
                })
                .collect(Collectors.joining("\n"));
    }

    private String design(EtlWorkflow workflow, Map<Long, EtlWorkflow> workflowById) {
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflow.getId());
        if (nodes.isEmpty()) {
            return "(노드 없음)";
        }
        return nodes.stream()
                .map(node -> {
                    String name = nameOf(node.getNodeKey(), node, workflowById);
                    List<String> predecessors = graphText.neighbors(workflow.getId(), node.getNodeKey(), nodes, workflowById).predecessors();
                    return "- " + name + " (" + ContextText.orDash(node.getNodeType()) + ")"
                            + (predecessors.isEmpty() ? "" : " ← 선행: " + String.join(", ", predecessors));
                })
                .collect(Collectors.joining("\n"));
    }

    // ------------------------------------------------------------------ 조회(실패해도 요약은 이어 간다)

    private List<DagRun> safeRecentRuns(String dagId) {
        try {
            return dagRunClient.getDagRuns(dagId);
        } catch (RuntimeException ex) {
            log.debug("실행 목록 조회 실패 dagId={}: {}", dagId, ex.getMessage());
            return List.of();
        }
    }

    private List<TaskInstance> safeTasks(String dagId, String runId) {
        try {
            return dagRunClient.getTaskInstances(dagId, runId);
        } catch (RuntimeException ex) {
            log.debug("실행 단계 조회 실패 dagId={} runId={}: {}", dagId, runId, ex.getMessage());
            return List.of();
        }
    }

    private RunDetail safeRun(String dagId, String runId) {
        try {
            return dagRunClient.getDagRun(dagId, runId);
        } catch (RuntimeException ex) {
            log.debug("하위 실행 조회 실패 dagId={} runId={}: {}", dagId, runId, ex.getMessage());
            return null;
        }
    }

    /** dag_id → 워크플로우(지운 것 포함, 같은 dag_id 가 여럿이면 살아 있는 것 · 없으면 마지막에 만든 것). */
    private static Map<String, EtlWorkflow> byDagId(List<EtlWorkflow> all) {
        Map<String, EtlWorkflow> out = new LinkedHashMap<>();
        all.stream()
                .sorted(Comparator.comparing((EtlWorkflow w) -> w.getDeletedAt() == null).thenComparing(EtlWorkflow::getId))
                .forEach(w -> out.put(w.dagId(), w));
        return out;
    }

    private static String indent(int depth) {
        return "    ".repeat(depth);
    }

    private record FailedStep(String dagId, String runId, TaskInstance task, String name) {
    }

    /** 적재 건수 합계. own: 이 실행의 job, sub: 하위 워크플로우 실행(그 아래 하위 포함). */
    private static final class Totals {
        long own;
        long sub;
        boolean known;
        boolean live;

        void addSub(Totals child) {
            if (child.known) {
                sub += child.own + child.sub;
                known = true;
            }
            live |= child.live;
        }
    }
}
