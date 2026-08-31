package com.company.pipeline.workflow;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 검증 → 컴파일 → 게시. 구현계획 §4.3.
 *
 * <p>게시해야만 DAG가 생긴다. 저장(draft)은 캔버스만 바꾸므로 그리다 만 그래프가 운영으로
 * 새지 않는다.
 *
 * <p>Variable 쓰기는 트랜잭션 밖의 외부 호출이라, 메타DB에 게시본을 먼저 남기고 Variable을
 * 갱신한다. Variable 쪽이 실패하면 게시를 되돌린다 - 반대로 두면 "Airflow엔 DAG가 있는데
 * 메타DB는 모르는" 상태가 남는다.
 */
@Service
public class WorkflowPublishService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowPublishService.class);

    private final WorkflowService workflowService;
    private final WorkflowValidator validator;
    private final WorkflowCompiler compiler;
    private final AirflowVariableClient variableClient;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlWorkflowEdgeRepository edgeRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WorkflowPublishService(WorkflowService workflowService,
                                  WorkflowValidator validator,
                                  WorkflowCompiler compiler,
                                  AirflowVariableClient variableClient,
                                  EtlWorkflowRepository workflowRepository,
                                  EtlWorkflowNodeRepository nodeRepository,
                                  EtlWorkflowEdgeRepository edgeRepository) {
        this.workflowService = workflowService;
        this.validator = validator;
        this.compiler = compiler;
        this.variableClient = variableClient;
        this.workflowRepository = workflowRepository;
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
    }

    /** 게시하지 않고 검증만. 화면의 [검증] 버튼. */
    @Transactional(readOnly = true)
    public WorkflowValidationResult validate(Long id) {
        EtlWorkflow workflow = workflowService.require(id);
        return validator.validate(workflow,
                nodeRepository.findByWorkflowIdAndDeletedAtIsNull(id),
                edgeRepository.findByWorkflowId(id));
    }

    /** 검증 → 컴파일 → spec 저장 → Airflow Variable 갱신. */
    @Transactional
    public WorkflowValidationResult publish(Long id, String actor) {
        EtlWorkflow workflow = workflowService.require(id);
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(id);
        List<EtlWorkflowEdge> edges = edgeRepository.findByWorkflowId(id);

        WorkflowValidationResult result = validator.validate(workflow, nodes, edges);
        if (!result.valid()) {
            return result;      // 오류가 있으면 게시하지 않고 그대로 돌려준다(화면이 노드를 표시)
        }

        // 게시 시점에 산출 Asset을 확정하고(불변 key 기반), 선행들의 Asset을 모아 넘긴다.
        workflow.ensureAssetUri();
        List<String> upstreamAssets = upstreamAssetUris(workflow);
        Map<String, Object> spec = compiler.compile(workflow, nodes, edges, upstreamAssets);
        String specJson = writeJson(spec);

        workflow.markPublished(specJson, actor);
        workflowRepository.save(workflow);

        try {
            variableClient.upsert(AirflowVariableClient.specKey(workflow.getWorkflowKey()), specJson);
            variableClient.upsert(AirflowVariableClient.INDEX_KEY, writeJson(publishedKeysIncluding(workflow)));
        } catch (RuntimeException ex) {
            log.warn("워크플로우 게시 실패(Airflow Variable 갱신) - id={} key={}: {}",
                    id, workflow.getWorkflowKey(), ex.getMessage());
            throw new BusinessException(ErrorCode.INTERNAL_ERROR,
                    "Airflow에 게시하지 못했습니다: " + ex.getMessage());
        }
        log.info("워크플로우 게시 - id={} key={} dagId={} nodes={}",
                id, workflow.getWorkflowKey(), workflow.dagId(), nodes.size());
        return result;
    }

    /** 게시를 내린다. index에서 빠지고 spec Variable도 지워 다음 파싱 주기에 DAG가 사라진다. */
    @Transactional
    public void unpublish(Long id) {
        EtlWorkflow workflow = workflowService.require(id);
        if (!workflow.isPublished()) {
            return;
        }
        workflow.markUnpublished();
        workflowRepository.save(workflow);
        try {
            variableClient.upsert(AirflowVariableClient.INDEX_KEY, writeJson(publishedKeysExcluding(workflow)));
            variableClient.delete(AirflowVariableClient.specKey(workflow.getWorkflowKey()));
        } catch (RuntimeException ex) {
            throw new BusinessException(ErrorCode.INTERNAL_ERROR,
                    "Airflow에서 게시를 내리지 못했습니다: " + ex.getMessage());
        }
        log.info("워크플로우 게시 취소 - id={} key={}", id, workflow.getWorkflowKey());
    }

    /**
     * 선행 워크플로우들의 산출 Asset URI. 이게 있으면 이 DAG는 시간이 아니라
     * "선행이 끝났다는 신호"로 실행된다.
     */
    private List<String> upstreamAssetUris(EtlWorkflow workflow) {
        List<Long> ids = parseUpstreamIds(workflow.getUpstreamWorkflowIds());
        List<String> uris = new java.util.ArrayList<>();
        for (Long id : ids) {
            workflowRepository.findByIdAndDeletedAtIsNull(id).ifPresent(up -> {
                up.ensureAssetUri();           // 선행이 아직 URI가 없으면 여기서 확정
                workflowRepository.save(up);
                uris.add(up.getProducesAssetUri());
            });
        }
        return uris;
    }

    private List<Long> parseUpstreamIds(String json) {
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

    private List<String> publishedKeysIncluding(EtlWorkflow workflow) {
        List<String> keys = publishedKeys();
        if (!keys.contains(workflow.getWorkflowKey())) {
            keys = new java.util.ArrayList<>(keys);
            keys.add(workflow.getWorkflowKey());
            keys.sort(String::compareTo);
        }
        return keys;
    }

    private List<String> publishedKeysExcluding(EtlWorkflow workflow) {
        return publishedKeys().stream()
                .filter(key -> !key.equals(workflow.getWorkflowKey()))
                .toList();
    }

    /** 메타DB가 원장이다. Variable은 여기서 파생된 사본이라 언제든 재게시로 복구된다. */
    private List<String> publishedKeys() {
        return workflowRepository.findByDeletedAtIsNullAndPublishedAtIsNotNull().stream()
                .map(EtlWorkflow::getWorkflowKey)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new BusinessException(ErrorCode.INTERNAL_ERROR,
                    "spec을 직렬화하지 못했습니다: " + ex.getMessage());
        }
    }
}
