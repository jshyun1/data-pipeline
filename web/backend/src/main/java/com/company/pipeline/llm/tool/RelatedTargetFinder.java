package com.company.pipeline.llm.tool;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.llm.context.LlmTargetType;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * 질문 속 대상 찾기(2026-10-02, «① 질문에 맞춰 상세 붙이기»).
 *
 * <p>속성창 AI 어시스턴트는 처음에 «지금 고른 대상»의 정보만 보낸다. 운영자가 그 안의 job · 파이프라인을 이름으로 물으면
 * (예: «tb_imp018m 은 어떤 방식으로 구성된 job 이야?») 모델은 그 대상의 구성을 받은 적이 없어 «알 수 없다»고 답했다.
 * 여기서 질문에 나온 이름을 프로젝트 전체의 ETL job · CDC 파이프라인 · 워크플로우와 맞춰 보고, 찾은 대상을 돌려준다.
 * 상세 본문은 {@link LlmToolbox#describe} 가 만든다(도구 호출과 같은 내용).
 *
 * <p>모델과 무관하게 서버가 보내기 <b>전</b>에 하는 일이라, 도구 호출을 못 받는 모델에서도 동작한다.
 *
 * <p>맞추는 규칙: 이름 전체가 질문에 있거나, 이름을 «- 공백 / . ( )» 로 나눈 조각 중 5자 이상인 것이 질문에 있으면 맞다고 본다
 * (AAA-tb_imp018m → tb_imp018m). 워크플로우를 보는 중에 «job들 · 각 · 전체 · 모든 · 구성» 처럼 전체를 묻고 이름이 없으면
 * 그 워크플로우의 노드를 전부 돌려준다.
 */
@Component
public class RelatedTargetFinder {

    /** 한 질문에 붙이는 대상 수 상한. 넘치면 앞에서부터. */
    static final int MAX_TARGETS = 6;
    private static final int MIN_TOKEN = 5;
    private static final Pattern SPLIT = Pattern.compile("[-\\s/.()]+");
    private static final Pattern WHOLE_INTENT = Pattern.compile("(job|잡|노드|파이프라인|태스크)\\s*(들|마다|별)|각각|각\\s|전체|모든|모두|구성");

    private final EtlJobRepository jobRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;

    public RelatedTargetFinder(EtlJobRepository jobRepository, PipelineDefinitionRepository pipelineRepository,
                               EtlWorkflowRepository workflowRepository, EtlWorkflowNodeRepository nodeRepository) {
        this.jobRepository = jobRepository;
        this.pipelineRepository = pipelineRepository;
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
    }

    /** 찾은 대상 하나. {@code target} 은 수집기에 넘길 식별자 맵이다. */
    public record Found(LlmTargetType type, String key, Map<String, String> target) {
    }

    /**
     * @param question     이번 질문
     * @param currentType  지금 보고 있는 대상 종류(그 대상 자신은 이미 대상 정보에 있으니 빼고, 워크플로우면 «전체» 질문을 판단한다)
     * @param currentTarget 지금 보고 있는 대상의 식별자
     */
    public List<Found> find(String question, LlmTargetType currentType, Map<String, String> currentTarget) {
        if (question == null || question.isBlank()) {
            return List.of();
        }
        String q = question.toLowerCase(Locale.ROOT);
        String currentWorkflowId = currentType == LlmTargetType.WORKFLOW && currentTarget != null
                ? currentTarget.get("workflowId") : null;
        Set<Long> jobsInCurrentWorkflow = currentWorkflowId == null ? Set.of() : jobsOf(Long.valueOf(currentWorkflowId));
        Map<String, Found> found = new LinkedHashMap<>();

        for (PipelineDefinition pipeline : pipelineRepository.findAll()) {
            if (mentioned(q, pipeline.getName(), pipeline.getSourceTable(), pipeline.getTargetTable())) {
                add(found, LlmTargetType.CDC_PIPELINE, pipeline.getId(), "pipelineId");
            }
        }
        for (EtlJob job : jobRepository.findByDeletedAtIsNullOrderByJobNameAsc()) {
            // 지금 보는 워크플로우의 job 은 그 «단계»가 이미 대상 정보에 들어 있다.
            if (!jobsInCurrentWorkflow.contains(job.getId()) && mentioned(q, job.getJobName())) {
                add(found, LlmTargetType.ETL_JOB, job.getId(), "jobId");
            }
        }
        for (EtlWorkflow workflow : workflowRepository.findByDeletedAtIsNullOrderByNameAsc()) {
            if (String.valueOf(workflow.getId()).equals(currentWorkflowId)) {
                continue;
            }
            if (mentioned(q, workflow.getName(), workflow.getWorkflowKey())) {
                add(found, LlmTargetType.WORKFLOW, workflow.getId(), "workflowId");
            }
        }

        if (found.isEmpty() && currentWorkflowId != null && WHOLE_INTENT.matcher(q).find()) {
            // 이름 없이 «job들 구성 알려줘»처럼 전체를 물었다 - 이 워크플로우의 CDC · 하위 워크플로우 노드를 붙인다
            // (ETL job 단계는 이미 대상 정보에 있다).
            for (EtlWorkflowNode node : nodeRepository.findByWorkflowIdAndDeletedAtIsNull(Long.valueOf(currentWorkflowId))) {
                if (node.getPipelineId() != null) {
                    add(found, LlmTargetType.CDC_PIPELINE, node.getPipelineId(), "pipelineId");
                } else if (node.getSubWorkflowId() != null) {
                    add(found, LlmTargetType.WORKFLOW, node.getSubWorkflowId(), "workflowId");
                }
            }
        }
        List<Found> out = new ArrayList<>(found.values());
        return out.size() > MAX_TARGETS ? out.subList(0, MAX_TARGETS) : out;
    }

    private Set<Long> jobsOf(Long workflowId) {
        Set<Long> ids = new java.util.HashSet<>();
        nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflowId).forEach(node -> {
            if (node.getJobId() != null) {
                ids.add(node.getJobId());
            }
        });
        return ids;
    }

    private static void add(Map<String, Found> found, LlmTargetType type, Long id, String key) {
        String foundKey = type + ":" + id;
        found.putIfAbsent(foundKey, new Found(type, foundKey, Map.of(key, String.valueOf(id))));
    }

    /** 이름(또는 그 조각)이 질문에 나왔나. */
    static boolean mentioned(String question, String... names) {
        for (String name : names) {
            if (name == null || name.isBlank()) {
                continue;
            }
            String lower = name.toLowerCase(Locale.ROOT).strip();
            if (lower.length() >= 3 && question.contains(lower)) {
                return true;
            }
            for (String token : SPLIT.split(lower)) {
                if (token.length() >= MIN_TOKEN && question.contains(token)) {
                    return true;
                }
            }
        }
        return false;
    }
}
