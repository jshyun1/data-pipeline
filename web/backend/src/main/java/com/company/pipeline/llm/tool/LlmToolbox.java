package com.company.pipeline.llm.tool;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.PermissionService;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.llm.context.LlmContextCollector;
import com.company.pipeline.llm.context.LlmContextCollector.CollectedTarget;
import com.company.pipeline.llm.context.LlmTargetType;
import com.company.pipeline.llm.context.SensitiveDataMasker;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * GPT 가 부를 수 있는 <b>읽기 전용</b> 조회 도구(2026-10-02, function calling).
 *
 * <p>도구의 결과는 기존 대상 수집기({@link LlmContextCollector})가 만드는 «대상 정보»와 같다 - 화면의 GPT 해석이 보는 것과
 * 같은 내용을, 모델이 필요할 때 골라서 가져가게 할 뿐이다. 그래서 보내면 안 되는 값(계정 · 비밀번호 · 키)이 섞이지 않는 규칙도
 * 수집기 쪽 그대로다. 결과는 길이를 자르고, 설정이 켜져 있으면 마스킹한다.
 *
 * <p>권한: 질문한 사용자가 그 대상을 볼 수 있어야 한다(대상 종류의 조회 권한 - 화면 GPT 와 같은 기준). 인가 스위치가 꺼진
 * 서버에서는 로그인만 본다.
 */
@Component
public class LlmToolbox {

    private static final Logger log = LoggerFactory.getLogger(LlmToolbox.class);
    /** 검색 결과로 돌려주는 최대 줄 수. */
    static final int SEARCH_LIMIT = 15;

    public static final String SEARCH = "search_targets";
    public static final String GET_ETL_JOB = "get_etl_job";
    public static final String GET_CDC_PIPELINE = "get_cdc_pipeline";
    public static final String GET_WORKFLOW = "get_workflow";

    private final Map<LlmTargetType, LlmContextCollector> collectors = new EnumMap<>(LlmTargetType.class);
    private final EtlJobRepository jobRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final EtlWorkflowRepository workflowRepository;
    private final PermissionService permissionService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${authz.enforcement.enabled:false}")
    private boolean enforcementEnabled;

    public LlmToolbox(List<LlmContextCollector> collectors, EtlJobRepository jobRepository,
                      PipelineDefinitionRepository pipelineRepository, EtlWorkflowRepository workflowRepository,
                      PermissionService permissionService) {
        collectors.forEach(collector -> this.collectors.put(collector.targetType(), collector));
        this.jobRepository = jobRepository;
        this.pipelineRepository = pipelineRepository;
        this.workflowRepository = workflowRepository;
        this.permissionService = permissionService;
    }

    /** 이번 질문을 한 사람과 결과 처리 규칙. */
    public record Caller(String userId, boolean masked, int resultChars) {
    }

    /** 도구 실행 결과. {@code label} 은 화면의 «조회 중» 줄에 쓴다. */
    public record Outcome(String label, String text) {
    }

    /** OpenAI 형식 도구 정의. */
    public List<Map<String, Object>> specs() {
        return List.of(
                spec(SEARCH, "이름 일부로 ETL job · CDC 파이프라인 · 워크플로우를 찾는다. 대상의 id 를 모를 때 먼저 쓴다.",
                        Map.of("keyword", Map.of("type", "string", "description", "이름 일부(예: tb_imp018m, dz_com)")), "keyword"),
                spec(GET_ETL_JOB, "ETL job(NiFi) 하나의 구성 · 단계(SQL · 대상 테이블) · 최근 실행을 조회한다.",
                        Map.of("job_id", Map.of("type", "integer", "description", "ETL job id")), "job_id"),
                spec(GET_CDC_PIPELINE, "CDC 파이프라인 하나의 구성(소스 · 타깃 · 스냅샷 · 커넥터) · 실행 상태 · 오류 · 최근 명령을 조회한다.",
                        Map.of("pipeline_id", Map.of("type", "integer", "description", "CDC 파이프라인 id")), "pipeline_id"),
                spec(GET_WORKFLOW, "워크플로우 하나의 구성(노드 · 스케줄 · 상하위) · 최근 실행을 조회한다.",
                        Map.of("workflow_id", Map.of("type", "integer", "description", "워크플로우 id")), "workflow_id"));
    }

    private static Map<String, Object> spec(String name, String description, Map<String, Object> properties, String required) {
        return Map.of("type", "function", "function", Map.of(
                "name", name,
                "description", description,
                "parameters", Map.of("type", "object", "properties", properties, "required", List.of(required))));
    }

    /** 도구 하나를 실행한다. 실패해도 예외를 던지지 않고 «조회하지 못했다»를 결과로 돌려준다 - 모델이 다른 길을 찾게. */
    public Outcome execute(String name, String argumentsJson, Caller caller) {
        try {
            JsonNode args = argumentsJson == null || argumentsJson.isBlank()
                    ? objectMapper.createObjectNode() : objectMapper.readTree(argumentsJson);
            return switch (name) {
                case SEARCH -> search(args.path("keyword").asText(""), caller);
                case GET_ETL_JOB -> describe(LlmTargetType.ETL_JOB, Map.of("jobId", idOf(args, "job_id")), caller);
                case GET_CDC_PIPELINE -> describe(LlmTargetType.CDC_PIPELINE, Map.of("pipelineId", idOf(args, "pipeline_id")), caller);
                case GET_WORKFLOW -> describe(LlmTargetType.WORKFLOW, Map.of("workflowId", idOf(args, "workflow_id")), caller);
                default -> new Outcome(name, "알 수 없는 도구입니다: " + name);
            };
        } catch (BusinessException ex) {
            return new Outcome(name, "조회하지 못했습니다: " + ex.getMessage());
        } catch (Exception ex) {
            log.warn("GPT 도구 실행 실패 tool={} args={}: {}", name, argumentsJson, ex.getMessage());
            return new Outcome(name, "조회하지 못했습니다(인자를 확인하십시오): " + ex.getMessage());
        }
    }

    /**
     * 대상 하나의 상세(«대상 정보»와 같은 내용). 질문 속 대상 상세 붙이기(①)도 이것을 쓴다.
     *
     * @param target 수집기가 받는 식별자 맵(jobId · pipelineId · workflowId)
     */
    public Outcome describe(LlmTargetType type, Map<String, String> target, Caller caller) {
        if (!allowed(type, caller.userId())) {
            return new Outcome(typeLabel(type), "이 대상을 조회할 권한이 없습니다.");
        }
        LlmContextCollector collector = collectors.get(type);
        if (collector == null) {
            return new Outcome(typeLabel(type), "지원하지 않는 대상입니다.");
        }
        CollectedTarget collected = collector.collect(target, null);
        String text = collected.context().assemble(caller.resultChars()).text();
        return new Outcome(typeLabel(type) + " «" + collected.label() + "» 조회", caller.masked() ? SensitiveDataMasker.mask(text) : text);
    }

    private Outcome search(String keyword, Caller caller) {
        String needle = keyword.strip().toLowerCase(Locale.ROOT);
        if (needle.length() < 2) {
            return new Outcome("검색", "검색어를 두 글자 이상 주십시오.");
        }
        List<String> lines = new ArrayList<>();
        if (allowed(LlmTargetType.ETL_JOB, caller.userId())) {
            for (EtlJob job : jobRepository.findByDeletedAtIsNullOrderByJobNameAsc()) {
                if (contains(job.getJobName(), needle)) {
                    lines.add("ETL job · job_id=" + job.getId() + " · " + job.getJobName());
                }
            }
        }
        if (allowed(LlmTargetType.CDC_PIPELINE, caller.userId())) {
            for (PipelineDefinition pipeline : pipelineRepository.findAll()) {
                if (contains(pipeline.getName(), needle) || contains(pipeline.getSourceTable(), needle)
                        || contains(pipeline.getTargetTable(), needle)) {
                    lines.add("CDC 파이프라인 · pipeline_id=" + pipeline.getId() + " · " + pipeline.getName()
                            + " (" + pipeline.getSourceSchema() + "." + pipeline.getSourceTable() + " → " + pipeline.getTargetTable() + ")");
                }
            }
        }
        if (allowed(LlmTargetType.WORKFLOW, caller.userId())) {
            for (EtlWorkflow workflow : workflowRepository.findByDeletedAtIsNullOrderByNameAsc()) {
                if (contains(workflow.getName(), needle) || contains(workflow.getWorkflowKey(), needle)) {
                    lines.add("워크플로우 · workflow_id=" + workflow.getId() + " · " + workflow.getName());
                }
            }
        }
        if (lines.isEmpty()) {
            return new Outcome("«" + keyword + "» 검색", "«" + keyword + "» 에 맞는 대상이 없습니다.");
        }
        String body = String.join("\n", lines.subList(0, Math.min(lines.size(), SEARCH_LIMIT)))
                + (lines.size() > SEARCH_LIMIT ? "\n… 외 " + (lines.size() - SEARCH_LIMIT) + "건(검색어를 더 구체적으로)" : "");
        return new Outcome("«" + keyword + "» 검색", body);
    }

    boolean allowed(LlmTargetType type, String userId) {
        if (!enforcementEnabled) {
            return true;
        }
        return userId != null && permissionService.check(userId, type.getRequiredSystem(), AccessBits.READ);
    }

    private static boolean contains(String value, String needle) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(needle);
    }

    private static String idOf(JsonNode args, String key) {
        JsonNode node = args.path(key);
        if (node.isMissingNode() || node.isNull() || node.asText().isBlank()) {
            throw new IllegalArgumentException(key + " 가 필요합니다");
        }
        return String.valueOf(node.asLong());
    }

    static String typeLabel(LlmTargetType type) {
        return switch (type) {
            case ETL_JOB -> "ETL job";
            case CDC_PIPELINE -> "CDC 파이프라인";
            case WORKFLOW -> "워크플로우";
            default -> type.name();
        };
    }
}
