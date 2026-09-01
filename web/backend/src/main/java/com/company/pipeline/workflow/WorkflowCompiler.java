package com.company.pipeline.workflow;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiProcessGroupMetadata;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * 캔버스 → Airflow가 읽을 spec JSON. 구현계획 §4.2.
 *
 * <p>Airflow 팩토리는 이 spec만 읽고 NiFi에는 묻지 않는다 - 그래야 NiFi가 잠깐 죽어도 DAG
 * 파싱이 멈추지 않는다(현행 팩토리가 파싱 때마다 NiFi를 조회해서 생기던 취약점).
 *
 * <p>그래서 팩토리가 실행 시점에 필요한 값(프로세스 그룹 id, 적재 검증 대상 테이블)을
 * 컴파일 시점에 미리 박아 넣는다. 예전에는 이걸 Airflow Variable(`{dag_id}__target_table` 등)에
 * 손으로 넣어야 했고, 오타 한 글자면 검증이 조용히 skip됐다.
 */
@Component
public class WorkflowCompiler {

    static final int SPEC_VERSION = 1;

    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final NifiProcessGroupMetadataRepository pgRepository;
    /** SUBWF 노드가 «어느 DAG를 띄울지» 알아야 해서 워크플로우 자체도 조회한다. */
    private final EtlWorkflowRepository workflowRepository;

    public WorkflowCompiler(EtlJobRepository jobRepository,
                            EtlJobStepRepository stepRepository,
                            NifiProcessGroupMetadataRepository pgRepository,
                            EtlWorkflowRepository workflowRepository) {
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.pgRepository = pgRepository;
        this.workflowRepository = workflowRepository;
    }

    public Map<String, Object> compile(EtlWorkflow workflow,
                                       List<EtlWorkflowNode> nodes,
                                       List<EtlWorkflowEdge> edges) {
        return compile(workflow, nodes, edges, List.of());
    }

    /**
     * @param upstreamAssetUris 선행 워크플로우들의 산출 Asset. 비어 있지 않으면 이 DAG는
     *                          시간 스케줄 대신 그 Asset들이 갱신될 때 실행된다.
     */
    public Map<String, Object> compile(EtlWorkflow workflow,
                                       List<EtlWorkflowNode> nodes,
                                       List<EtlWorkflowEdge> edges,
                                       List<String> upstreamAssetUris) {
        Map<Long, EtlJob> jobs = jobRepository.findAllById(
                        nodes.stream().map(EtlWorkflowNode::getJobId).filter(Objects::nonNull).toList())
                .stream().collect(Collectors.toMap(EtlJob::getId, Function.identity()));

        List<Map<String, Object>> nodeSpecs = new ArrayList<>();
        Set<String> governedRoots = new TreeSet<>();

        for (EtlWorkflowNode node : nodes) {
            Map<String, Object> spec = new LinkedHashMap<>();
            spec.put("key", node.getNodeKey());
            spec.put("type", node.getNodeType());
            spec.put("trigger_rule", airflowTriggerRule(node.getTriggerRule()));
            spec.put("retries", node.getRetries());
            spec.put("retry_delay_sec", node.getRetryDelaySec());

            if (EtlWorkflowNode.TYPE_JOB.equals(node.getNodeType()) && node.getJobId() != null) {
                EtlJob job = jobs.get(node.getJobId());
                spec.put("job_id", node.getJobId());
                spec.put("name", job == null ? node.getNodeKey() : job.getJobName());
                spec.put("nifi_pg_id", job == null ? null : job.getNifiPgId());
                spec.put("target_tables", targetTables(node.getJobId()));
                if (job != null && job.getNifiPgId() != null) {
                    governedRootOf(job.getNifiPgId()).ifPresent(governedRoots::add);
                }
            } else {
                spec.put("name", node.getNodeKey());
                if (node.getSubWorkflowId() != null) {
                    spec.put("sub_workflow_id", node.getSubWorkflowId());
                    // 팩토리는 워크플로우 id가 아니라 «어느 DAG를 띄울지»를 알아야 한다.
                    // 이름도 같이 실어 화면·로그에서 키가 아닌 사람 말로 보이게 한다.
                    workflowRepository.findById(node.getSubWorkflowId()).ifPresent(sub -> {
                        spec.put("sub_dag_id", sub.dagId());
                        spec.put("name", sub.getName());
                        spec.put("sub_workflow_key", sub.getWorkflowKey());
                    });
                }
                if (node.getBranchExpr() != null) {
                    spec.put("branch_expr", node.getBranchExpr());
                }
            }
            nodeSpecs.add(spec);
        }

        List<Map<String, Object>> edgeSpecs = edges.stream()
                .map(e -> {
                    Map<String, Object> spec = new LinkedHashMap<>();
                    spec.put("from", e.getFromNodeKey());
                    spec.put("to", e.getToNodeKey());
                    spec.put("condition", e.getConditionType());
                    if (e.getConditionExpr() != null) {
                        spec.put("condition_expr", e.getConditionExpr());
                    }
                    return spec;
                })
                .toList();

        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("workflow_key", workflow.getWorkflowKey());
        spec.put("dag_id", workflow.dagId());
        spec.put("name", workflow.getName());
        spec.put("description", workflow.getDescription());
        spec.put("schedule", blankToNull(workflow.getScheduleCron()));
        spec.put("timezone", workflow.getTimezone());
        spec.put("catchup", workflow.isCatchup());
        spec.put("max_active_runs", workflow.getMaxActiveRuns());
        spec.put("suspend_on_error", workflow.isSuspendOnError());
        // 워크플로우 간 연결. 소비(consumes)가 있으면 팩토리가 schedule 대신 Asset로 건다.
        spec.put("produces_asset", workflow.getProducesAssetUri());
        spec.put("consumes_assets", upstreamAssetUris);
        spec.put("upstream_mode", workflow.getUpstreamMode());
        // 공존 기간에 기존 팩토리가 "이 root 직하 그룹은 새 팩토리가 맡는다"를 알아보는 키.
        spec.put("governed_root_pg_ids", List.copyOf(governedRoots));
        spec.put("nodes", nodeSpecs);
        spec.put("edges", edgeSpecs);
        spec.put("compiled_at", LocalDateTime.now().toString());
        spec.put("spec_version", SPEC_VERSION);
        return spec;
    }

    /** PutDatabaseRecord 등이 적는 타깃 테이블. verify 태스크가 적재 건수를 확인할 대상이다. */
    private List<String> targetTables(Long jobId) {
        return stepRepository.findByJobIdAndDeletedAtIsNull(jobId).stream()
                .map(EtlJobStep::getTargetTable)
                .filter(t -> t != null && !t.isBlank())
                .distinct()
                .sorted()
                .toList();
    }

    /**
     * 이 프로세스 그룹이 속한 <b>root 직하</b> 그룹 id를 찾는다.
     *
     * <p>미러가 {@code parent_group_id}로 계층을 들고 있으므로 부모를 타고 올라가다가, 부모가
     * 없거나 부모가 root면 그 지점이 root 직하다. 순환이 있어도(있을 리 없지만) 방문 집합으로
     * 멈춘다.
     */
    private Optional<String> governedRootOf(String pgId) {
        Set<String> seen = new HashSet<>();
        String current = pgId;
        while (current != null && seen.add(current)) {
            Optional<NifiProcessGroupMetadata> meta = pgRepository.findById(current);
            if (meta.isEmpty()) {
                return Optional.of(current);        // 미러에 없으면 그 자체를 경계로 본다
            }
            String parent = meta.get().getParentGroupId();
            if (parent == null || parent.isBlank() || isRoot(parent)) {
                return Optional.of(current);
            }
            current = parent;
        }
        return Optional.ofNullable(current);
    }

    /** root는 리터럴 "root" 또는 부모가 없는 그룹으로 기록된다(V55 마이그레이션). */
    private boolean isRoot(String pgId) {
        if ("root".equals(pgId)) {
            return true;
        }
        return pgRepository.findById(pgId)
                .map(meta -> meta.getParentGroupId() == null || meta.getParentGroupId().isBlank())
                .orElse(false);
    }

    /** 캔버스의 조건 표기를 Airflow trigger_rule 문자열로. */
    private String airflowTriggerRule(String rule) {
        return rule == null ? "all_success" : rule.toLowerCase();
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
