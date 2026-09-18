package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowEdge;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import com.company.pipeline.workflow.WorkflowValidator;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 워크플로우 «생성» 화면 속성창 AI 어시스턴트의 정보 수집(대상 {@code WORKFLOW_DRAFT}, 2026-09-18 요청).
 *
 * <p>생성 중인 워크플로우는 서버에 아직 없어서, 화면이 저장 본문과 같은 재료(속성 입력 · 노드 · 연결)를 JSON 으로 보낸다
 * ({@code LlmChatRequest.draft}). 화면이 보낸 것은 «무엇을 어떻게 이었는지»뿐이고, job 이름 · 단계 SQL · 하위 워크플로우 구성 ·
 * CDC 파이프라인 이름은 여기서 id 로 찾아 붙인다 - 다른 수집기처럼 보낼 내용은 서버가 정한다.
 *
 * <p>보내는 것:
 * <ul>
 *   <li>작성 중 속성(이름 · 같은 이름 워크플로우 유무 · 설명 · 스케줄 · 실행 설정 · 메모)</li>
 *   <li>노드 구성(선행 · 연결 조건 · 실행 조건 · 재시도)과 구조 요약(시작 · 마지막 · 연결 안 된 노드)</li>
 *   <li>검증 결과 - 게시 때 쓰는 {@link WorkflowValidator} 를 저장하지 않은 초안에 그대로 돌린 것(읽기만 한다)</li>
 *   <li>job 단계 · 하위 워크플로우 구성(길면 뒤에서부터 잘린다)</li>
 * </ul>
 */
@Component
public class WorkflowDraftContextCollector implements LlmContextCollector {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDraftContextCollector.class);
    static final int MAX_NODES = 300;
    static final int MAX_EDGES = 1_000;

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Draft(String name, String description, String scheduleCron, String timezone, Boolean catchup,
                 Integer maxActiveRuns, Boolean suspendOnError, String memo, List<DraftNode> nodes, List<DraftEdge> edges) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DraftNode(String nodeKey, String nodeType, Long jobId, Long subWorkflowId, Long pipelineId, String triggerRule,
                     Integer retries, Integer retryDelaySec, String label) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record DraftEdge(String fromNodeKey, String toNodeKey, String conditionType) {
    }

    private final ObjectMapper objectMapper;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlJobRepository jobRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final WorkflowValidator validator;
    private final EtlJobContextText jobText;
    private final WorkflowGraphText graphText;

    public WorkflowDraftContextCollector(ObjectMapper objectMapper, EtlWorkflowRepository workflowRepository,
                                         EtlJobRepository jobRepository, PipelineDefinitionRepository pipelineRepository,
                                         WorkflowValidator validator, EtlJobContextText jobText, WorkflowGraphText graphText) {
        this.objectMapper = objectMapper;
        this.workflowRepository = workflowRepository;
        this.jobRepository = jobRepository;
        this.pipelineRepository = pipelineRepository;
        this.validator = validator;
        this.jobText = jobText;
        this.graphText = graphText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.WORKFLOW_DRAFT;
    }

    @Override
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        return collect(target, errorText, null);
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText, String draftJson) {
        Draft draft = parse(draftJson);
        List<DraftNode> nodes = draft.nodes() == null ? List.of()
                : draft.nodes().stream().filter(n -> n != null && n.nodeKey() != null && !n.nodeKey().isBlank()).toList();
        List<DraftEdge> edges = draft.edges() == null ? List.of()
                : draft.edges().stream().filter(e -> e != null && e.fromNodeKey() != null && e.toNodeKey() != null).toList();
        if (nodes.size() > MAX_NODES || edges.size() > MAX_EDGES) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "노드는 " + MAX_NODES + "개, 연결은 " + MAX_EDGES + "개까지 보낼 수 있습니다.");
        }

        Map<Long, EtlWorkflow> workflowById = workflowRepository.findByDeletedAtIsNullOrderByNameAsc().stream()
                .collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));
        Map<String, String> labels = nodes.stream()
                .collect(Collectors.toMap(DraftNode::nodeKey, n -> label(n, workflowById), (a, b) -> a));

        LlmContext context = new LlmContext();
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);
        context.add("작성 중인 새 워크플로우 (저장 전)", identity(draft, nodes, workflowById), LlmContext.PRIORITY_IDENTITY);
        context.add("노드 구성", nodeLines(nodes, edges, labels), LlmContext.PRIORITY_IDENTITY);
        context.add("구조 요약", structure(nodes, edges, labels), LlmContext.PRIORITY_IDENTITY);
        context.add("검증 결과 (게시 검증을 이 초안에 돌린 것)", validation(draft, nodes, edges, labels), LlmContext.PRIORITY_IDENTITY);
        nodes.stream().map(DraftNode::jobId).filter(Objects::nonNull).distinct()
                .map(jobRepository::findById).flatMap(Optional::stream)
                .forEach(job -> context.add("job «" + job.getJobName() + "» 의 단계", jobText.steps(job), LlmContext.PRIORITY_DETAIL));
        nodes.stream().map(DraftNode::subWorkflowId).filter(Objects::nonNull).distinct()
                .map(workflowById::get).filter(Objects::nonNull)
                .forEach(sub -> context.add("하위 워크플로우 «" + sub.getName() + "» 의 구성",
                        "스케줄(cron): " + ContextText.orDash(sub.getScheduleCron())
                                + " (하위로 얹으면 자체 스케줄 대신 이 워크플로우가 부를 때 실행)\n노드:\n"
                                + graphText.nodeList(sub.getId(), workflowById),
                        LlmContext.PRIORITY_DETAIL));

        String name = blank(draft.name()) ? "새 워크플로우" : draft.name().strip();
        return new CollectedTarget("draft/*", name, context);
    }

    private Draft parse(String draftJson) {
        if (draftJson == null || draftJson.isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "작성 중인 설계 정보가 없습니다.");
        }
        try {
            return objectMapper.readValue(draftJson, Draft.class);
        } catch (JsonProcessingException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "작성 중인 설계 정보를 읽을 수 없습니다.");
        }
    }

    private String identity(Draft draft, List<DraftNode> nodes, Map<Long, EtlWorkflow> workflowById) {
        List<String> lines = new ArrayList<>();
        if (blank(draft.name())) {
            lines.add("이름: (비어 있음 - 이름을 적어야 저장할 수 있다)");
        } else {
            String name = draft.name().strip();
            boolean taken = workflowById.values().stream().anyMatch(w -> name.equalsIgnoreCase(w.getName()));
            lines.add("이름: " + name + (taken ? " (같은 이름의 워크플로우가 이미 있다)" : ""));
        }
        lines.add("설명: " + ContextText.orDash(draft.description()));
        boolean cdcWorkflow = !nodes.isEmpty() && nodes.stream().allMatch(n -> EtlWorkflowNode.TYPE_CDC.equals(n.nodeType()));
        if (cdcWorkflow) {
            lines.add("실행 방식: CDC 워크플로우 - 스케줄 · 선행 · 연결선 없이, 실행할 때 고른 지시(시작 · 중지 · 재시작 · 배포)를 모든 CDC 노드에 보낸다");
        } else {
            lines.add("스케줄(cron): " + (blank(draft.scheduleCron()) ? "없음 (수동 실행 전용)" : draft.scheduleCron().strip())
                    + " · 시간대 " + ContextText.orDash(draft.timezone()));
        }
        lines.add("실패 시 중단: " + yesNo(draft.suspendOnError())
                + " · 동시 실행 수: " + ContextText.orDash(draft.maxActiveRuns())
                + " · 밀린 실행(catchup): " + yesNo(draft.catchup()));
        lines.add("메모: " + ContextText.orDash(draft.memo()));
        lines.add("게시: 안 됨 (저장 후 게시해야 Airflow 에서 실행된다)");
        return String.join("\n", lines);
    }

    private String nodeLines(List<DraftNode> nodes, List<DraftEdge> edges, Map<String, String> labels) {
        if (nodes.isEmpty()) {
            return "(노드 없음 - 캔버스에 아직 아무것도 놓지 않았다)";
        }
        return nodes.stream().map(node -> {
            String line = "- " + labels.get(node.nodeKey()) + " (" + kind(node) + ")";
            if (EtlWorkflowNode.TYPE_CDC.equals(node.nodeType())) {
                return line;
            }
            List<String> predecessors = edges.stream().filter(e -> node.nodeKey().equals(e.toNodeKey()))
                    .map(e -> labels.getOrDefault(e.fromNodeKey(), e.fromNodeKey()) + condition(e.conditionType()))
                    .toList();
            return line
                    + (predecessors.isEmpty() ? "" : " ← 선행: " + String.join(", ", predecessors))
                    + " · 실행 조건 " + ContextText.orDash(node.triggerRule())
                    + " · 재시도 " + (node.retries() == null ? 0 : node.retries()) + "회"
                    + (node.retries() != null && node.retries() > 0 ? " (간격 " + ContextText.orDash(node.retryDelaySec()) + "초)" : "");
        }).collect(Collectors.joining("\n"));
    }

    private static String structure(List<DraftNode> nodes, List<DraftEdge> edges, Map<String, String> labels) {
        if (nodes.isEmpty()) {
            return null;
        }
        Set<String> hasIncoming = edges.stream().map(DraftEdge::toNodeKey).collect(Collectors.toSet());
        Set<String> hasOutgoing = edges.stream().map(DraftEdge::fromNodeKey).collect(Collectors.toSet());
        List<String> flowNodes = nodes.stream()
                .filter(n -> !EtlWorkflowNode.TYPE_CDC.equals(n.nodeType()))
                .map(DraftNode::nodeKey).toList();
        List<String> lines = new ArrayList<>();
        lines.add("노드 " + nodes.size() + "개 · 연결 " + edges.size() + "개");
        if (!flowNodes.isEmpty()) {
            lines.add("먼저 시작하는 노드(선행 없음): " + names(flowNodes.stream().filter(k -> !hasIncoming.contains(k)).toList(), labels));
            lines.add("마지막 노드(후행 없음): " + names(flowNodes.stream().filter(k -> !hasOutgoing.contains(k)).toList(), labels));
            if (flowNodes.size() > 1) {
                List<String> isolated = flowNodes.stream()
                        .filter(k -> !hasIncoming.contains(k) && !hasOutgoing.contains(k)).toList();
                if (!isolated.isEmpty()) {
                    lines.add("어디에도 연결되지 않은 노드: " + names(isolated, labels));
                }
            }
        }
        return String.join("\n", lines);
    }

    /** 게시 검증을 저장하지 않은 엔티티로 돌린다. 검증기는 조회만 한다. 실패해도 대화는 막지 않고 구역만 뺀다. */
    private String validation(Draft draft, List<DraftNode> nodes, List<DraftEdge> edges, Map<String, String> labels) {
        try {
            EtlWorkflow workflow = new EtlWorkflow("draft", blank(draft.name()) ? "새 워크플로우" : draft.name().strip());
            workflow.updateSettings(null, draft.description(), blank(draft.scheduleCron()) ? null : draft.scheduleCron().strip(),
                    draft.timezone(), draft.catchup(), draft.maxActiveRuns(), draft.suspendOnError());
            List<EtlWorkflowNode> nodeEntities = nodes.stream()
                    .map(n -> new EtlWorkflowNode(null, n.nodeKey(), n.nodeType(), n.jobId(), n.subWorkflowId(), n.pipelineId(),
                            n.triggerRule(), null, n.retries(), n.retryDelaySec(), null, null))
                    .toList();
            List<EtlWorkflowEdge> edgeEntities = edges.stream()
                    .map(e -> new EtlWorkflowEdge(null, e.fromNodeKey(), e.toNodeKey(), e.conditionType(), null))
                    .toList();
            WorkflowValidationResult result = validator.validate(workflow, nodeEntities, edgeEntities);
            List<String> lines = new ArrayList<>();
            lines.add(result.valid() ? "오류 없음 (게시 가능)" : "오류 " + result.errors().size() + "건 (이대로는 게시할 수 없다)");
            result.errors().forEach(issue -> lines.add("- 오류: " + issue(issue, labels)));
            result.warnings().forEach(issue -> lines.add("- 경고: " + issue(issue, labels)));
            return String.join("\n", lines);
        } catch (RuntimeException ex) {
            log.warn("초안 검증을 AI 정보에 넣지 못했습니다: {}", ex.getMessage());
            return null;
        }
    }

    private static String issue(WorkflowValidationResult.Issue issue, Map<String, String> labels) {
        return issue.message() + (issue.nodeKey() == null ? "" : " [노드 " + labels.getOrDefault(issue.nodeKey(), issue.nodeKey()) + "]");
    }

    private String label(DraftNode node, Map<Long, EtlWorkflow> workflowById) {
        if (node.jobId() != null) {
            Optional<String> name = jobRepository.findById(node.jobId()).map(EtlJob::getJobName);
            if (name.isPresent()) {
                return name.get();
            }
        }
        if (node.subWorkflowId() != null && workflowById.containsKey(node.subWorkflowId())) {
            return workflowById.get(node.subWorkflowId()).getName();
        }
        if (node.pipelineId() != null) {
            Optional<String> name = pipelineRepository.findById(node.pipelineId()).map(PipelineDefinition::getName);
            if (name.isPresent()) {
                return name.get();
            }
        }
        return blank(node.label()) ? node.nodeKey() : node.label().strip();
    }

    private static String kind(DraftNode node) {
        if (EtlWorkflowNode.TYPE_CDC.equals(node.nodeType())) {
            return "CDC 파이프라인";
        }
        if (node.subWorkflowId() != null) {
            return "하위 워크플로우";
        }
        if (node.jobId() != null) {
            return "job";
        }
        return ContextText.orDash(node.nodeType());
    }

    private static String condition(String conditionType) {
        return conditionType == null || EtlWorkflowEdge.COND_SUCCESS.equals(conditionType) ? "" : " (연결 조건 " + conditionType + ")";
    }

    private static String names(List<String> keys, Map<String, String> labels) {
        if (keys.isEmpty()) {
            return "없음";
        }
        return keys.stream().map(k -> labels.getOrDefault(k, k)).collect(Collectors.toCollection(LinkedHashSet::new))
                .stream().collect(Collectors.joining(", "));
    }

    private static String yesNo(Boolean value) {
        return value == null ? "-" : value ? "예" : "아니오";
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
