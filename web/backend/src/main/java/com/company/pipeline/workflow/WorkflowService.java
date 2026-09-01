package com.company.pipeline.workflow;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.workflow.dto.WorkflowRequests.CreateWorkflow;
import com.company.pipeline.workflow.dto.WorkflowRequests.EdgeRequest;
import com.company.pipeline.workflow.dto.WorkflowRequests.NodeRequest;
import com.company.pipeline.workflow.dto.WorkflowRequests.SaveGraph;
import com.company.pipeline.workflow.dto.WorkflowRequests.UpdateWorkflow;
import com.company.pipeline.workflow.dto.WorkflowResponses.EdgeView;
import com.company.pipeline.workflow.dto.WorkflowResponses.NodeView;
import com.company.pipeline.workflow.dto.WorkflowResponses.WorkflowDetail;
import com.company.pipeline.workflow.dto.WorkflowResponses;
import com.company.pipeline.workflow.dto.WorkflowResponses.WorkflowSummary;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * 워크플로우 캔버스의 CRUD. 컴파일·게시는 {@code WorkflowPublishService}(P2)가 맡는다.
 *
 * <p>캔버스 저장은 노드·엣지를 <b>전량 교체</b>한다. 화면이 그래프 전체를 들고 있어서
 * 부분 갱신보다 단순하고, 중간에 끊겨도 이전 상태가 남지 않는다.
 */
@Service
public class WorkflowService {

    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlWorkflowEdgeRepository edgeRepository;
    private final EtlJobRepository jobRepository;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();
    private final com.company.pipeline.nifi.NifiProcessGroupMetadataRepository pgRepository;

    public WorkflowService(EtlWorkflowRepository workflowRepository,
                           EtlWorkflowNodeRepository nodeRepository,
                           EtlWorkflowEdgeRepository edgeRepository,
                           EtlJobRepository jobRepository,
                           com.company.pipeline.nifi.NifiProcessGroupMetadataRepository pgRepository) {
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
        this.jobRepository = jobRepository;
        this.pgRepository = pgRepository;
    }

    @Transactional(readOnly = true)
    public List<WorkflowSummary> list() {
        return workflowRepository.findByDeletedAtIsNullOrderByNameAsc().stream()
                .map(w -> WorkflowSummary.from(
                        w, nodeRepository.findByWorkflowIdAndDeletedAtIsNull(w.getId()).size(),
                        upstreamIds(w).size()))
                .toList();
    }

    @Transactional(readOnly = true)
    public WorkflowDetail get(Long id) {
        EtlWorkflow workflow = require(id);
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(id);
        List<EtlWorkflowEdge> edges = edgeRepository.findByWorkflowId(id);

        Map<Long, EtlJob> jobsById = jobRepository.findAllById(
                        nodes.stream().map(EtlWorkflowNode::getJobId).filter(Objects::nonNull).toList())
                .stream().collect(Collectors.toMap(EtlJob::getId, Function.identity()));
        Map<Long, EtlWorkflow> subById = workflowRepository.findAllById(
                        nodes.stream().map(EtlWorkflowNode::getSubWorkflowId).filter(Objects::nonNull).toList())
                .stream().collect(Collectors.toMap(EtlWorkflow::getId, Function.identity()));

        List<NodeView> nodeViews = nodes.stream().map(n -> {
            EtlJob job = n.getJobId() == null ? null : jobsById.get(n.getJobId());
            // 참조하던 job이 삭제(소프트 포함)됐으면 화면에서 표시하고 게시도 막는다.
            boolean missing = n.getJobId() != null && (job == null || job.getDeletedAt() != null);
            EtlWorkflow sub = n.getSubWorkflowId() == null ? null : subById.get(n.getSubWorkflowId());
            String parentName = job == null || job.getParentPgId() == null ? null
                    : pgRepository.findById(job.getParentPgId())
                            .map(com.company.pipeline.nifi.NifiProcessGroupMetadata::getProcessGroupName)
                            .orElse(null);
            return NodeView.of(n,
                    job == null ? null : job.getJobName(),
                    parentName,
                    job == null ? null : job.getNifiPgId(),
                    sub == null ? null : sub.getName(),
                    missing);
        }).toList();

        return WorkflowDetail.of(workflow, nodeViews, edges.stream().map(EdgeView::from).toList(),
                isDirty(workflow), upstreamIds(workflow), parentsOf(id));
    }

    @Transactional
    public WorkflowSummary create(CreateWorkflow request) {
        String key = StringUtils.hasText(request.workflowKey())
                ? request.workflowKey()
                : generateKey(request.nifiGroupPgId(), request.name());
        workflowRepository.findByWorkflowKeyAndDeletedAtIsNull(key)
                .ifPresent(existing -> {
                    throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                            "이미 사용 중인 workflowKey입니다: " + key);
                });
        EtlWorkflow workflow = new EtlWorkflow(key, request.name());
        workflow.updateSettings(request.name(), request.description(), request.nifiGroupPgId(),
                request.scheduleCron(), request.timezone(), request.catchup(),
                request.maxActiveRuns(), request.suspendOnError());
        return WorkflowSummary.from(workflowRepository.save(workflow), 0, 0);
    }

    /**
     * workflowKey를 그룹·이름에서 만든다. 사용자는 키를 입력하지 않는다.
     *
     * <p>키는 dag_id({@code etl_wf_{key}})의 축이라 전역에서 유일해야 하는데, 그룹별로
     * 워크플로우를 그리면 DW와 DZ 양쪽에 같은 이름이 생기기 마련이다(실제로 두 그룹 다
     * COM001M을 갖고 있다). 그래서 그룹 이름을 앞에 붙여 충돌을 구조적으로 없앤다.
     *
     * <p>한글 이름은 슬러그가 비므로 그룹 이름만 쓰고, 그래도 겹치면 뒤에 번호를 붙인다.
     */
    private String generateKey(String groupPgId, String name) {
        String groupSlug = groupPgId == null ? "" : pgRepository.findById(groupPgId)
                .map(com.company.pipeline.nifi.NifiProcessGroupMetadata::getProcessGroupName)
                .map(WorkflowService::slug)
                .orElse("");
        String nameSlug = slug(name);
        // "DW 일배치"처럼 이름이 그룹 이름으로 시작하면 dw_dw가 되어버린다. 겹치면 한 번만 쓴다.
        String base;
        if (!StringUtils.hasText(groupSlug) || nameSlug.equals(groupSlug)
                || nameSlug.startsWith(groupSlug + "_")) {
            base = StringUtils.hasText(nameSlug) ? nameSlug : groupSlug;
        } else {
            base = Stream.of(groupSlug, nameSlug)
                    .filter(StringUtils::hasText)
                    .collect(Collectors.joining("_"));
        }
        if (!StringUtils.hasText(base)) {
            base = "wf";
        }
        if (base.length() > 74) {
            base = base.substring(0, 74);
        }
        String candidate = base;
        // 소프트 삭제된 키는 다시 쓸 수 있다(유일 인덱스가 deleted_at IS NULL 조건부라서).
        for (int suffix = 2; workflowRepository.findByWorkflowKeyAndDeletedAtIsNull(candidate).isPresent();
             suffix += 1) {
            candidate = base + "_" + suffix;
        }
        return candidate;
    }

    /** dag_id에 쓸 수 있는 형태로 깎는다. 한글 등 ASCII 밖 문자는 버린다. */
    private static String slug(String raw) {
        if (raw == null) {
            return "";
        }
        String cleaned = raw.toLowerCase(java.util.Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
        return cleaned.length() > 40 ? cleaned.substring(0, 40) : cleaned;
    }

    /**
     * 이 워크플로우를 노드로 품고 있는 상위 워크플로우들.
     *
     * <p>상위가 있으면 자체 스케줄은 돌지 않는다(상위가 지시할 때만 돈다). 화면이 그 사실을
     * 보여줘야 «분명히 2시로 걸어뒀는데 왜 안 도나»를 겪지 않는다.
     */
    @Transactional(readOnly = true)
    public List<WorkflowResponses.ParentRef> parentsOf(Long id) {
        List<EtlWorkflowNode> refs = nodeRepository.findBySubWorkflowIdAndDeletedAtIsNull(id);
        return refs.stream()
                .map(EtlWorkflowNode::getWorkflowId)
                .distinct()
                .map(workflowRepository::findById)
                .flatMap(java.util.Optional::stream)
                .filter(parent -> parent.getDeletedAt() == null)
                .map(parent -> new WorkflowResponses.ParentRef(
                        parent.getId(), parent.getWorkflowKey(), parent.getName(), parent.dagId(),
                        parent.getScheduleCron(), parent.isPublished()))
                .toList();
    }

    @Transactional
    public WorkflowSummary update(Long id, UpdateWorkflow request) {
        EtlWorkflow workflow = require(id);
        workflow.updateSettings(request.name(), request.description(), request.nifiGroupPgId(),
                request.scheduleCron(), request.timezone(), request.catchup(),
                request.maxActiveRuns(), request.suspendOnError(),
                writeIds(request.upstreamWorkflowIds()), request.upstreamMode(), request.memo());
        workflowRepository.save(workflow);
        return WorkflowSummary.from(workflow,
                nodeRepository.findByWorkflowIdAndDeletedAtIsNull(id).size(),
                upstreamIds(workflow).size());
    }

    /**
     * 캔버스 저장(draft). 노드·엣지를 전량 교체한다.
     *
     * <p>여기서는 구조적으로 저장 불가능한 것(중복 키, 없는 노드를 가리키는 엣지)만 막는다.
     * 사이클·고아 같은 "게시하면 안 되는" 판정은 P2 컴파일러가 한다 - 그리다 만 상태도
     * 저장은 돼야 하기 때문이다.
     */
    @Transactional
    public WorkflowDetail saveGraph(Long id, SaveGraph request) {
        EtlWorkflow workflow = require(id);
        List<NodeRequest> nodes = request.nodes() == null ? List.of() : request.nodes();
        List<EdgeRequest> edges = request.edges() == null ? List.of() : request.edges();

        Set<String> keys = new HashSet<>();
        for (NodeRequest node : nodes) {
            if (!keys.add(node.nodeKey())) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "노드 키가 중복됩니다: " + node.nodeKey());
            }
        }
        for (EdgeRequest edge : edges) {
            if (!keys.contains(edge.fromNodeKey()) || !keys.contains(edge.toNodeKey())) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "존재하지 않는 노드를 잇는 연결입니다: "
                                + edge.fromNodeKey() + " → " + edge.toNodeKey());
            }
        }
        // 없는 job을 참조하면 FK 위반이 500으로 새어나가므로 여기서 먼저 잡는다.
        // (이미 삭제된 job을 참조하는 경우는 저장은 되고 게시 때 V3가 막는다 - 화면에서
        //  어느 노드가 깨졌는지 보여주고 고칠 기회를 줘야 하기 때문이다.)
        List<Long> jobIds = nodes.stream()
                .map(NodeRequest::jobId).filter(Objects::nonNull).distinct().toList();
        if (!jobIds.isEmpty()) {
            Set<Long> found = jobRepository.findAllById(jobIds).stream()
                    .map(EtlJob::getId).collect(Collectors.toSet());
            List<Long> missing = jobIds.stream().filter(jobId -> !found.contains(jobId)).toList();
            if (!missing.isEmpty()) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "존재하지 않는 job을 참조합니다: " + missing);
            }
        }

        edgeRepository.deleteByWorkflowId(id);
        nodeRepository.deleteByWorkflowId(id);
        nodeRepository.flush();

        nodes.forEach(n -> nodeRepository.save(new EtlWorkflowNode(
                id, n.nodeKey(), n.nodeType(), n.jobId(), n.subWorkflowId(),
                n.triggerRule(), n.branchExpr(), n.retries(), n.retryDelaySec(),
                n.displayX(), n.displayY())));
        edges.forEach(e -> edgeRepository.save(new EtlWorkflowEdge(
                id, e.fromNodeKey(), e.toNodeKey(), e.conditionType(), e.conditionExpr())));

        workflow.touch();
        workflowRepository.save(workflow);
        return get(id);
    }

    /** 게시 중인 워크플로우는 지우지 못한다 - DAG가 남아 유령이 된다. 먼저 게시를 내려야 한다. */
    @Transactional
    public void delete(Long id) {
        EtlWorkflow workflow = require(id);
        if (workflow.isPublished()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "게시 중인 워크플로우는 삭제할 수 없습니다. 먼저 게시를 내려주세요.");
        }
        workflow.softDelete();
        workflowRepository.save(workflow);
    }

    /** job 삭제 가드 - 이 job을 참조하는 워크플로우 이름 목록(비어 있으면 삭제 가능). */
    @Transactional(readOnly = true)
    public List<String> workflowsReferencing(Long jobId) {
        return nodeRepository.findByJobIdAndDeletedAtIsNull(jobId).stream()
                .map(EtlWorkflowNode::getWorkflowId)
                .distinct()
                .map(workflowRepository::findById)
                .flatMap(java.util.Optional::stream)
                .filter(w -> w.getDeletedAt() == null)
                .map(EtlWorkflow::getName)
                .toList();
    }

    /** 선행 워크플로우 id 목록(JSON 저장분을 풀어서). */
    List<Long> upstreamIds(EtlWorkflow workflow) {
        String json = workflow.getUpstreamWorkflowIds();
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<List<Long>>() {});
        } catch (Exception ex) {
            return List.of();
        }
    }

    private String writeIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(ids);
        } catch (Exception ex) {
            return null;
        }
    }

    EtlWorkflow require(Long id) {
        return workflowRepository.findByIdAndDeletedAtIsNull(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "워크플로우를 찾을 수 없습니다: " + id));
    }

    /** 게시 이후에 캔버스가 바뀌었는지. 정확한 비교는 P2에서 spec 해시로 대체한다. */
    private boolean isDirty(EtlWorkflow workflow) {
        return workflow.isPublished()
                && workflow.getUpdatedAt() != null
                && workflow.getUpdatedAt().isAfter(workflow.getPublishedAt());
    }
}
