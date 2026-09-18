package com.company.pipeline.workflow;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import com.company.pipeline.workflow.dto.WorkflowValidationResult.Issue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * 게시 전 캔버스 검증(구현계획 §4.1의 V1~V9).
 *
 * <p>저장(draft)은 그리다 만 상태도 허용하지만, 게시는 "Airflow가 실제로 돌릴 수 있는 그래프"만
 * 통과시킨다. 여기서 막지 못한 오류는 DAG 파싱 실패로 넘어가고, 그러면 화면이 아니라 Airflow
 * 로그를 봐야 원인을 알 수 있어서 훨씬 비싸진다.
 */
@Component
public class WorkflowValidator {

    /** Airflow 프리셋. 5필드 크론은 아래 정규식으로 따로 본다. */
    private static final Set<String> CRON_PRESETS = Set.of(
            "@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@annually");
    private static final Pattern CRON_5FIELD =
            Pattern.compile("^\\s*\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s*$");

    private final EtlJobRepository jobRepository;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final com.company.pipeline.pipeline.PipelineDefinitionRepository pipelineRepository;

    public WorkflowValidator(EtlJobRepository jobRepository,
                             EtlWorkflowRepository workflowRepository,
                             EtlWorkflowNodeRepository nodeRepository,
                             com.company.pipeline.pipeline.PipelineDefinitionRepository pipelineRepository) {
        this.jobRepository = jobRepository;
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.pipelineRepository = pipelineRepository;
    }

    public WorkflowValidationResult validate(EtlWorkflow workflow,
                                             List<EtlWorkflowNode> nodes,
                                             List<EtlWorkflowEdge> edges) {
        List<Issue> errors = new ArrayList<>();
        List<Issue> warnings = new ArrayList<>();

        if (nodes.isEmpty()) {
            errors.add(Issue.of("V0", "노드가 하나도 없습니다. job을 캔버스에 배치해주세요."));
            return WorkflowValidationResult.of(errors, warnings);
        }

        Set<String> keys = nodes.stream().map(EtlWorkflowNode::getNodeKey).collect(Collectors.toSet());

        validateEdges(edges, keys, errors);              // V5
        validateNodes(nodes, edges, errors, warnings);   // V3, V4, V8
        validateCycle(nodes, edges, errors);             // V1
        validateOrphans(nodes, edges, warnings);         // V2
        validateSchedule(workflow, errors);              // V6
        validateJobReuse(workflow, nodes, warnings);     // V7, V9
        validateChaining(workflow, errors, warnings);    // V10, V11
        validateCdcComposition(workflow, nodes, edges, errors, warnings);  // V12

        return WorkflowValidationResult.of(errors, warnings);
    }

    /**
     * V10 - 워크플로우 간 연결의 순환(A→B→A)과 실존 여부.
     * V11 - 선행이 있는데 스케줄도 있으면 두 번 돈다(경고).
     */
    private void validateChaining(EtlWorkflow workflow, List<Issue> errors, List<Issue> warnings) {
        List<Long> upstream = parseUpstream(workflow.getUpstreamWorkflowIds());
        if (upstream.isEmpty()) {
            return;
        }
        if (upstream.contains(workflow.getId())) {
            errors.add(Issue.of("V10", "자기 자신을 선행 워크플로우로 지정할 수 없습니다."));
            return;
        }
        for (Long id : upstream) {
            var found = workflowRepository.findByIdAndDeletedAtIsNull(id);
            if (found.isEmpty()) {
                errors.add(Issue.of("V10", "선행 워크플로우를 찾을 수 없습니다: " + id));
                continue;
            }
            // CDC 워크플로우는 지시만 쏘고 끝나서 «완료 신호»를 내지 않는다 - 선행으로 걸면 이 워크플로우는 영영 안 돈다.
            if (isCdcWorkflow(id)) {
                errors.add(Issue.of("V10",
                        "CDC 워크플로우는 선행으로 지정할 수 없습니다(완료 신호가 없어 이 워크플로우가 시작되지 않습니다): "
                                + found.get().getName()));
                continue;
            }
            if (!found.get().isPublished()) {
                errors.add(Issue.of("V10",
                        "선행 워크플로우가 아직 게시되지 않았습니다: " + found.get().getName()
                                + " (먼저 게시해야 이 워크플로우가 신호를 받을 수 있습니다)"));
            }
            if (reachesBack(id, workflow.getId(), new HashSet<>())) {
                errors.add(Issue.of("V10",
                        "워크플로우 간 순환입니다: " + found.get().getName() + " 이(가) 다시 이 워크플로우를 기다립니다."));
            }
        }
        if (workflow.getScheduleCron() != null && !workflow.getScheduleCron().isBlank()) {
            warnings.add(Issue.of("V11",
                    "선행 워크플로우와 스케줄이 함께 설정되어 있습니다. 선행 완료와 스케줄 두 경로로 각각 실행됩니다."));
        }
    }

    /** from 에서 출발해 target 에 닿는지(선행 관계를 거슬러) - 순환 탐지. */
    private boolean reachesBack(Long from, Long target, Set<Long> seen) {
        if (!seen.add(from)) {
            return false;
        }
        var wf = workflowRepository.findByIdAndDeletedAtIsNull(from);
        if (wf.isEmpty()) {
            return false;
        }
        for (Long up : parseUpstream(wf.get().getUpstreamWorkflowIds())) {
            if (up.equals(target) || reachesBack(up, target, seen)) {
                return true;
            }
        }
        return false;
    }

    private List<Long> parseUpstream(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(json, new com.fasterxml.jackson.core.type.TypeReference<List<Long>>() {});
        } catch (Exception ex) {
            return List.of();
        }
    }

    /** V5 - 엣지가 실재하는 노드를 잇는지, 자기 자신을 잇지 않는지. */
    private void validateEdges(List<EtlWorkflowEdge> edges, Set<String> keys, List<Issue> errors) {
        for (EtlWorkflowEdge edge : edges) {
            if (!keys.contains(edge.getFromNodeKey()) || !keys.contains(edge.getToNodeKey())) {
                errors.add(Issue.of("V5", "존재하지 않는 노드를 잇는 연결입니다: "
                        + edge.getFromNodeKey() + " → " + edge.getToNodeKey()));
            }
            if (Objects.equals(edge.getFromNodeKey(), edge.getToNodeKey())) {
                errors.add(Issue.at("V5", "자기 자신으로 연결할 수 없습니다.", edge.getFromNodeKey()));
            }
        }
    }

    /** 노드가 가리키는 job들을 한 번에 읽어 id로 찾을 수 있게 만든다. */
    private Map<Long, EtlJob> jobsOf(List<EtlWorkflowNode> nodes) {
        return jobRepository.findAllById(
                        nodes.stream().map(EtlWorkflowNode::getJobId).filter(Objects::nonNull).toList())
                .stream().collect(Collectors.toMap(EtlJob::getId, j -> j));
    }

    /** V3(JOB 참조 유효성), V4(BRANCH 구성), V8(SUBWF 참조). */
    private void validateNodes(List<EtlWorkflowNode> nodes, List<EtlWorkflowEdge> edges,
                               List<Issue> errors, List<Issue> warnings) {
        Map<Long, EtlJob> jobs = jobsOf(nodes);

        for (EtlWorkflowNode node : nodes) {
            switch (node.getNodeType()) {
                case EtlWorkflowNode.TYPE_JOB -> validateJobNode(node, jobs, errors, warnings);
                case EtlWorkflowNode.TYPE_BRANCH -> {
                    if (node.getBranchExpr() == null || node.getBranchExpr().isBlank()) {
                        errors.add(Issue.at("V4", "분기 노드에 조건식이 없습니다.", node.getNodeKey()));
                    }
                    long outs = edges.stream()
                            .filter(e -> e.getFromNodeKey().equals(node.getNodeKey())).count();
                    if (outs < 2) {
                        errors.add(Issue.at("V4",
                                "분기 노드는 나가는 연결이 2개 이상이어야 합니다.", node.getNodeKey()));
                    }
                }
                case EtlWorkflowNode.TYPE_SUBWF -> validateSubWorkflow(node, errors);
                case EtlWorkflowNode.TYPE_CDC -> validateCdcNode(node, errors);
                default -> { /* JOIN/START/END는 추가 제약 없음 */ }
            }
        }
    }

    private void validateJobNode(EtlWorkflowNode node, Map<Long, EtlJob> jobs,
                                 List<Issue> errors, List<Issue> warnings) {
        if (node.getJobId() == null) {
            errors.add(Issue.at("V3", "job이 지정되지 않은 노드입니다.", node.getNodeKey()));
            return;
        }
        EtlJob job = jobs.get(node.getJobId());
        if (job == null || job.getDeletedAt() != null) {
            errors.add(Issue.at("V3",
                    "참조하는 job이 삭제되었습니다. 노드를 지우거나 다른 job으로 바꿔주세요.",
                    node.getNodeKey()));
            return;
        }
        if (job.getNifiPgId() == null || job.getNifiPgId().isBlank()) {
            errors.add(Issue.at("V3",
                    "job에 NiFi 프로세스 그룹이 연결되어 있지 않습니다: " + job.getJobName(),
                    node.getNodeKey()));
        }
        // 스텝이 없는 job은 실행해도 아무 일이 없다. 막지는 않되 알려준다.
        if (job.getStepCount() == 0) {
            warnings.add(Issue.at("V3", "이 job에는 실행할 프로세서가 없습니다: " + job.getJobName(),
                    node.getNodeKey()));
        }
    }

    /** V8 - SUBWF는 실재해야 하고 자기 자신을 품을 수 없다(무한 중첩 방지). */
    private void validateSubWorkflow(EtlWorkflowNode node, List<Issue> errors) {
        if (node.getSubWorkflowId() == null) {
            errors.add(Issue.at("V8", "하위 워크플로우가 지정되지 않았습니다.", node.getNodeKey()));
            return;
        }
        if (Objects.equals(node.getSubWorkflowId(), node.getWorkflowId())) {
            errors.add(Issue.at("V8", "자기 자신을 하위 워크플로우로 넣을 수 없습니다.", node.getNodeKey()));
            return;
        }
        if (workflowRepository.findByIdAndDeletedAtIsNull(node.getSubWorkflowId()).isEmpty()) {
            errors.add(Issue.at("V8", "하위 워크플로우를 찾을 수 없습니다.", node.getNodeKey()));
            return;
        }
        // 하위 워크플로우는 conf 없이 뜬다 - CDC 워크플로우면 지시 기본값(start)이 매 회차 나가서, 일부러 멈춰 둔
        // 파이프라인까지 다시 켠다(2026-09-17 실측: 상위 ETL 실행 → 하위 CDC → 제어 DAG action=start → DEPLOYED).
        if (isCdcWorkflow(node.getSubWorkflowId())) {
            errors.add(Issue.at("V8",
                    "CDC 워크플로우는 하위 워크플로우로 넣을 수 없습니다. 상위가 돌 때마다 CDC 시작 지시가 나갑니다 - "
                            + "CDC 워크플로우는 실시간 모니터링의 «실행 설정»에서 따로 실행하세요.",
                    node.getNodeKey()));
        }
    }

    /** V12 - CDC 노드는 파이프라인을 가리켜야 하고, 그 파이프라인이 실재해야 한다. */
    private void validateCdcNode(EtlWorkflowNode node, List<Issue> errors) {
        if (node.getPipelineId() == null) {
            errors.add(Issue.at("V12", "CDC 파이프라인이 지정되지 않은 노드입니다.", node.getNodeKey()));
            return;
        }
        if (pipelineRepository.findById(node.getPipelineId()).isEmpty()) {
            errors.add(Issue.at("V12",
                    "참조하는 CDC 파이프라인이 삭제되었습니다. 노드를 지우거나 다른 파이프라인으로 바꿔주세요.",
                    node.getNodeKey()));
        }
    }

    /**
     * V12 - CDC 워크플로우의 구성 규칙(2026-09-10 설계 «방안 B»).
     *
     * <p>CDC 노드는 제어 DAG 를 부르고 <b>기다리지 않는다</b>. «끝»이 없는 것을 기다리면 워커 슬롯이
     * 영구히 묶이기 때문이다. 기다리지 않으므로 순서도 성립하지 않는다 - 그래서 잇지 못하게 막고,
     * 순서가 있는 ETL job·하위 워크플로우와 한 워크플로우에 섞지 못하게 한다. 섞으면 화면의 그림과
     * 실제 실행이 어긋난다(«CDC 가 끝나면 다음»처럼 읽히는데 그렇게 돌지 않는다).
     *
     * <p>스케줄도 두지 않는다. CDC 는 한 번 켜면 계속 도는 것이라 «매일 2시에 다시 켜기»가
     * 뜻을 갖지 않는다 - 이미 돌고 있으면 건너뛰고, 멈춰 있다면 그건 사람이 볼 일이다.
     */
    private void validateCdcComposition(EtlWorkflow workflow, List<EtlWorkflowNode> nodes,
                                        List<EtlWorkflowEdge> edges,
                                        List<Issue> errors, List<Issue> warnings) {
        Set<String> cdcKeys = nodes.stream()
                .filter(n -> EtlWorkflowNode.TYPE_CDC.equals(n.getNodeType()))
                .map(EtlWorkflowNode::getNodeKey)
                .collect(Collectors.toSet());
        if (cdcKeys.isEmpty()) {
            return;
        }
        // 섞였을 때는 «적은 쪽» 노드를 짚는다. 기존 ETL 워크플로우에 CDC 하나를 잘못 놓았으면 그 CDC 가,
        // CDC 워크플로우에 job 하나를 놓았으면 그 job 이 빨갛게 된다(같으면 CDC 쪽).
        List<EtlWorkflowNode> others = nodes.stream()
                .filter(n -> !EtlWorkflowNode.TYPE_CDC.equals(n.getNodeType()))
                .toList();
        if (!others.isEmpty()) {
            List<EtlWorkflowNode> marked = others.size() < cdcKeys.size() ? others
                    : nodes.stream().filter(n -> EtlWorkflowNode.TYPE_CDC.equals(n.getNodeType())).toList();
            marked.forEach(n -> errors.add(Issue.at("V12",
                    "ETL job·하위 워크플로우와 CDC 노드는 한 워크플로우에 섞을 수 없습니다. "
                            + "CDC 노드만 두거나 워크플로우를 나누세요(CDC " + cdcKeys.size() + "개 · 그 밖 " + others.size() + "개).",
                    n.getNodeKey())));
        }
        edges.stream()
                .filter(e -> cdcKeys.contains(e.getFromNodeKey()) || cdcKeys.contains(e.getToNodeKey()))
                .forEach(e -> errors.add(Issue.at("V12",
                        "CDC 노드는 연결할 수 없습니다: " + e.getFromNodeKey() + " → " + e.getToNodeKey(),
                        e.getFromNodeKey())));
        if (!others.isEmpty()) {
            // 섞였으면 섞임만 알린다. 아래 «CDC 워크플로우» 규칙(스케줄·선행 금지)까지 내면, ETL 워크플로우에 CDC 하나를
            // 잘못 놓은 사람에게 «스케줄을 지우라»는 엉뚱한 안내가 된다(2026-09-17 확인).
            return;
        }
        if (workflow.getScheduleCron() != null && !workflow.getScheduleCron().isBlank()) {
            errors.add(Issue.of("V12",
                    "CDC 워크플로우에는 스케줄을 둘 수 없습니다. 실행할 때 지시(시작·중지 등)를 골라 보냅니다."));
        }
        // 선행 워크플로우도 무시된다 - 팩토리가 CDC 워크플로우의 스케줄(선행 신호 포함)을 끈다. 걸어 두면 «선행이 끝나면
        // 돈다»로 읽히는데 실제로는 안 돈다.
        if (!parseUpstream(workflow.getUpstreamWorkflowIds()).isEmpty()) {
            errors.add(Issue.of("V12",
                    "CDC 워크플로우에는 선행 워크플로우를 둘 수 없습니다(수동 실행 전용). 선행 설정을 비워 주세요."));
        }
        // 반대 방향 - 이미 다른 워크플로우가 이 워크플로우를 하위 노드나 선행으로 쓰고 있으면, 여기를 CDC 로 바꿔
        // 게시하는 순간 그쪽이 매 회차 CDC 시작 지시를 보내거나(하위) 영영 안 돈다(선행).
        if (workflow.getId() != null) {
            List<String> parents = nodeRepository.findBySubWorkflowIdAndDeletedAtIsNull(workflow.getId()).stream()
                    .map(EtlWorkflowNode::getWorkflowId)
                    .distinct()
                    .map(workflowRepository::findById)
                    .flatMap(java.util.Optional::stream)
                    .filter(w -> w.getDeletedAt() == null)
                    .map(EtlWorkflow::getName)
                    .toList();
            if (!parents.isEmpty()) {
                errors.add(Issue.of("V12",
                        "다른 워크플로우가 이 워크플로우를 하위 노드로 쓰고 있어 CDC 워크플로우로 게시할 수 없습니다: "
                                + String.join(", ", parents)));
            }
            List<String> downstream = workflowRepository.findByDeletedAtIsNullOrderByNameAsc().stream()
                    .filter(w -> !Objects.equals(w.getId(), workflow.getId()))
                    .filter(w -> parseUpstream(w.getUpstreamWorkflowIds()).contains(workflow.getId()))
                    .map(EtlWorkflow::getName)
                    .toList();
            if (!downstream.isEmpty()) {
                errors.add(Issue.of("V12",
                        "다른 워크플로우가 이 워크플로우를 선행으로 지정해 CDC 워크플로우로 게시할 수 없습니다: "
                                + String.join(", ", downstream)));
            }
        }
        // 같은 파이프라인을 두 번 얹으면 같은 지시가 두 번 간다. 해롭지는 않지만(두 번째는 건너뛴다) 실수일 가능성이 크다.
        Map<Long, Long> byPipeline = nodes.stream()
                .filter(n -> EtlWorkflowNode.TYPE_CDC.equals(n.getNodeType()) && n.getPipelineId() != null)
                .collect(Collectors.groupingBy(EtlWorkflowNode::getPipelineId, Collectors.counting()));
        byPipeline.forEach((pipelineId, count) -> {
            if (count > 1) {
                warnings.add(Issue.of("V12", "같은 CDC 파이프라인이 " + count + "번 놓여 있습니다(id=" + pipelineId + ")."));
            }
        });
    }

    /** 노드 구성에서 읽는 «CDC 워크플로우»(CDC 노드가 하나라도 있으면). DAG 팩토리 {@code _is_cdc} 와 같은 기준. */
    private boolean isCdcWorkflow(Long workflowId) {
        return nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflowId).stream()
                .anyMatch(n -> EtlWorkflowNode.TYPE_CDC.equals(n.getNodeType()));
    }

    /** V1 - 위상 정렬로 사이클을 찾는다(Kahn). 남은 노드가 있으면 그게 순환이다. */
    private void validateCycle(List<EtlWorkflowNode> nodes, List<EtlWorkflowEdge> edges,
                              List<Issue> errors) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> adjacency = new HashMap<>();
        nodes.forEach(n -> {
            indegree.put(n.getNodeKey(), 0);
            adjacency.put(n.getNodeKey(), new ArrayList<>());
        });
        for (EtlWorkflowEdge e : edges) {
            if (!indegree.containsKey(e.getFromNodeKey()) || !indegree.containsKey(e.getToNodeKey())) {
                continue;   // V5가 이미 오류로 잡았다
            }
            adjacency.get(e.getFromNodeKey()).add(e.getToNodeKey());
            indegree.merge(e.getToNodeKey(), 1, Integer::sum);
        }

        Deque<String> queue = new ArrayDeque<>();
        indegree.forEach((key, deg) -> {
            if (deg == 0) {
                queue.add(key);
            }
        });
        int visited = 0;
        while (!queue.isEmpty()) {
            String current = queue.poll();
            visited++;
            for (String next : adjacency.get(current)) {
                if (indegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }
        if (visited < indegree.size()) {
            String stuck = indegree.entrySet().stream()
                    .filter(entry -> entry.getValue() > 0)
                    .map(Map.Entry::getKey)
                    .sorted()
                    .collect(Collectors.joining(", "));
            errors.add(Issue.of("V1", "순환 참조가 있습니다. 관련 노드: " + stuck));
        }
    }

    /**
     * V2 - 연결이 없어 실행 순서가 정해지지 않은 노드.
     *
     * <p>연결이 하나도 없는 워크플로우는 "이 job들을 한꺼번에 돌린다"는 뜻이고, 실제로
     * 지금 운영 중인 옛 DAG가 그렇게 동작한다(그룹 전체를 켜고 잡들이 병렬로 돈다).
     * 그래서 그 경우는 문제로 보지 않는다.
     *
     * <p>반대로 순서를 그려 놓고 한 노드만 떨어져 있으면 대개 잇다 만 것이다. 다만 그래도
     * 병렬로 도는 데는 지장이 없으므로 게시를 막지 않고 경고만 남긴다.
     */
    private void validateOrphans(List<EtlWorkflowNode> nodes, List<EtlWorkflowEdge> edges,
                                List<Issue> warnings) {
        if (nodes.size() <= 1 || edges.isEmpty()) {
            return;
        }
        Set<String> connected = new HashSet<>();
        edges.forEach(e -> {
            connected.add(e.getFromNodeKey());
            connected.add(e.getToNodeKey());
        });
        nodes.stream()
                .filter(n -> !connected.contains(n.getNodeKey()))
                .forEach(n -> warnings.add(Issue.at("V2",
                        "다른 노드와 연결되지 않아 단독으로 실행됩니다.", n.getNodeKey())));
    }

    /** V6 - Airflow가 받아들이는 스케줄인지. 빈 값이면 수동 전용이라 정상이다. */
    private void validateSchedule(EtlWorkflow workflow, List<Issue> errors) {
        String cron = workflow.getScheduleCron();
        if (cron == null || cron.isBlank()) {
            return;
        }
        String trimmed = cron.trim();
        if (CRON_PRESETS.contains(trimmed) || CRON_5FIELD.matcher(trimmed).matches()) {
            return;
        }
        errors.add(Issue.of("V6",
                "스케줄 형식이 올바르지 않습니다. 5필드 크론(예: 0 2 * * *) 또는 @daily 형식을 써주세요."));
    }

    /** V7 - 한 워크플로우 안 중복 참조 / V9 - 다른 게시본과 중복 참조. 둘 다 경고. */
    private void validateJobReuse(EtlWorkflow workflow, List<EtlWorkflowNode> nodes,
                                 List<Issue> warnings) {
        Map<Long, List<EtlWorkflowNode>> byJob = nodes.stream()
                .filter(n -> n.getJobId() != null)
                .collect(Collectors.groupingBy(EtlWorkflowNode::getJobId));

        byJob.forEach((jobId, sharing) -> {
            if (sharing.size() > 1) {
                warnings.add(Issue.of("V7", "같은 job을 여러 노드가 참조합니다: "
                        + sharing.stream().map(EtlWorkflowNode::getNodeKey).sorted()
                        .collect(Collectors.joining(", "))
                        + " (동시에 실행되면 한쪽이 대기합니다)"));
            }
            // 다른 게시본이 같은 job을 물고 있으면 두 스케줄이 서로를 밀어낼 수 있다.
            nodeRepository.findByJobIdAndDeletedAtIsNull(jobId).stream()
                    .map(EtlWorkflowNode::getWorkflowId)
                    .filter(id -> !Objects.equals(id, workflow.getId()))
                    .distinct()
                    .map(workflowRepository::findById)
                    .flatMap(java.util.Optional::stream)
                    .filter(other -> other.getDeletedAt() == null && other.isPublished())
                    .forEach(other -> warnings.add(Issue.of("V9",
                            "이미 게시된 다른 워크플로우가 같은 job을 사용합니다: " + other.getName())));
        });
    }
}
