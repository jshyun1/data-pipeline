package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.SchemaDiscoveryService;
import com.company.pipeline.connection.dto.ColumnMetadataResponse;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.company.pipeline.pipeline.dto.PipelineUpdateRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

/**
 * 파이프라인 정의 수정(2026-10-12 요청 - CDC 관리 «수정» 버튼, 생성 위자드를 수정 모드로 연다).
 *
 * <p><b>운영 중인 파이프라인에 영향을 주지 않는 것</b>이 설계의 중심이다:
 * <ul>
 *   <li>저장은 정의(DB) 값만 바꾼다. 등록된 커넥터는 건드리지 않는다 - 바뀐 커넥터 설정은 다음 준비(배포) 때 들어간다.
 *       그때까지는 {@link PipelineDeployService#pendingConfigItems} 가 «설정 변경 미반영»으로 알린다.</li>
 *   <li>고칠 수 있는 값은 커넥터 이름 · 토픽 · 읽던 위치(오프셋) · Postgres 슬롯에 묶이지 않은 것뿐이다:
 *       이름 · 설명 · 그룹(실행 영향 없음), 제외 · 마스킹 컬럼 · DELETE 반영(커넥터 설정만 바뀜).</li>
 *   <li>그 밖의 값(연결 · 소스/타깃 스키마 · 테이블 · Topic Prefix · 적재 방식 · 구분컬럼 · 스냅샷 모드)은 잠근다 - 바꾸면 처음부터
 *       다시 적재하는 새 파이프라인이 되므로 «새로 생성»으로 해야 한다. 화면이 지금 값을 보내면 같은지 확인하고, 다르면 거절한다.</li>
 *   <li>준비 · 시작 · 정지와 같은 파이프라인 잠금 안에서 저장하고, 준비(배포) 중이면 막는다.</li>
 *   <li>컬럼 규칙(PK 제외 · 마스킹 금지, 마스킹 불가 형식, 제외 · 마스킹 겹침)은 화면만이 아니라 서버도 소스 DB 메타데이터로 확인한다.</li>
 *   <li>무엇을 무엇에서 무엇으로 바꿨는지 명령 이력 {@code DEFINITION_UPDATE} 로 남긴다.</li>
 * </ul>
 */
@Service
public class PipelineEditService {

    static final String HISTORY_COMMAND = "DEFINITION_UPDATE";
    private static final int HISTORY_MESSAGE_MAX = 2000;

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final PipelineDeployService deployService;
    private final PipelineGroupService groupService;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final PipelineCommandHistoryRecorder commandHistoryRecorder;

    public PipelineEditService(PipelineDefinitionRepository pipelineDefinitionRepository,
                               PipelineConnectorRepository pipelineConnectorRepository,
                               PipelineDeployService deployService,
                               PipelineGroupService groupService,
                               SchemaDiscoveryService schemaDiscoveryService,
                               PipelineCommandHistoryRecorder commandHistoryRecorder) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.deployService = deployService;
        this.groupService = groupService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.commandHistoryRecorder = commandHistoryRecorder;
    }

    public PipelineResponse update(Long pipelineId, PipelineUpdateRequest request, String actor) {
        return deployService.runLocked(pipelineId, () -> {
            PipelineDefinition pipeline = pipelineDefinitionRepository.findById(pipelineId)
                    .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
            if (pipeline.getStatus() == PipelineStatus.DEPLOYING) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "준비(배포) 중인 파이프라인은 수정할 수 없습니다. 끝난 뒤 다시 시도하세요.");
            }
            rejectLockedChanges(pipeline, request);

            List<String> changes = new ArrayList<>();
            applyBasics(pipeline, request, changes);
            if (isTableCdc(pipeline)) {
                applyCdcOptions(pipeline, request, changes);
            }
            if (changes.isEmpty()) {
                return PipelineResponse.from(pipeline, pipelineConnectorRepository.findByPipelineId(pipelineId));
            }
            pipelineDefinitionRepository.save(pipeline);
            String summary = String.join(" · ", changes);
            commandHistoryRecorder.record(pipelineId, HISTORY_COMMAND, "SUCCESS",
                    summary.length() > HISTORY_MESSAGE_MAX ? summary.substring(0, HISTORY_MESSAGE_MAX) : summary, actor);
            return PipelineResponse.from(pipeline, pipelineConnectorRepository.findByPipelineId(pipelineId));
        });
    }

    /** 파이프라인별 «설정 변경 미반영» 항목. 비어 있는 파이프라인은 뺀다. */
    public Map<Long, List<String>> pendingConfigAll() {
        Map<Long, List<String>> out = new LinkedHashMap<>();
        for (PipelineDefinition pipeline : pipelineDefinitionRepository.findAll()) {
            List<String> items = deployService.pendingConfigItems(pipeline);
            if (!items.isEmpty()) {
                out.put(pipeline.getId(), items);
            }
        }
        return out;
    }

    public List<String> pendingConfig(Long pipelineId) {
        PipelineDefinition pipeline = pipelineDefinitionRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        return deployService.pendingConfigItems(pipeline);
    }

    // ------------------------------------------------------------------ 잠긴 값

    private void rejectLockedChanges(PipelineDefinition pipeline, PipelineUpdateRequest request) {
        lock("소스 연결", pipeline.getSourceConnectionId(), request.sourceConnectionId());
        lock("타깃 연결", pipeline.getTargetConnectionId(), request.targetConnectionId());
        lock("소스 스키마", pipeline.getSourceSchema(), trim(request.sourceSchema()));
        lock("소스 테이블", pipeline.getSourceTable(), trim(request.sourceTable()));
        lock("타깃 스키마", pipeline.getTargetSchema(), trim(request.targetSchema()));
        lock("타깃 테이블", pipeline.getTargetTable(), trim(request.targetTable()));
        if (!isTableCdc(pipeline)) {
            return;
        }
        if (request.topicPrefix() != null) {
            lock("Topic Prefix", topicPrefixOf(pipeline), request.topicPrefix().trim());
        }
        PipelineLoadMode loadMode = PipelineLoadMode.from(pipeline.getLoadMode());
        if (request.loadMode() != null) {
            lock("적재 방식", loadMode.name(), PipelineLoadMode.from(request.loadMode()).name());
        }
        if (request.deltaOpColumn() != null && loadMode.isDelta()) {
            lock("구분컬럼", pipeline.getDeltaOpColumn(), PipelineLoadMode.normalizeDeltaOpColumn(request.deltaOpColumn()));
        }
        if (request.snapshotMode() != null) {
            lock("스냅샷 모드", PipelineSnapshotMode.from(pipeline.getSnapshotMode()).name(),
                    PipelineSnapshotMode.from(request.snapshotMode()).name());
        }
    }

    private static void lock(String label, Object current, Object requested) {
        if (requested != null && !Objects.equals(current, requested)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "«" + label + "»은(는) 수정할 수 없습니다 - 바꾸면 커넥터 이름 · 토픽 · "
                    + "읽던 위치(오프셋)가 달라져 처음부터 다시 적재하는 새 파이프라인이 됩니다. 새 파이프라인으로 생성하세요.");
        }
    }

    /** 저장된 토픽 이름(prefix.schema.table)에서 prefix 를 되찾는다. 모양이 다르면 토픽 이름 전체를 돌려줘 비교가 실패하게 둔다. */
    static String topicPrefixOf(PipelineDefinition pipeline) {
        String topic = pipeline.getTopicName();
        if (topic == null || pipeline.getSourceSchema() == null || pipeline.getSourceTable() == null) {
            return topic;
        }
        String suffix = "." + pipeline.getSourceSchema() + "." + pipeline.getSourceTable();
        return topic.toLowerCase(Locale.ROOT).endsWith(suffix.toLowerCase(Locale.ROOT))
                ? topic.substring(0, topic.length() - suffix.length()) : topic;
    }

    // ------------------------------------------------------------------ 고칠 수 있는 값

    private void applyBasics(PipelineDefinition pipeline, PipelineUpdateRequest request, List<String> changes) {
        String name = request.name().trim();
        if (!name.equals(pipeline.getName())) {
            if (pipelineDefinitionRepository.existsByName(name)) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이미 사용 중인 파이프라인명입니다: " + name);
            }
            changes.add("이름 " + pipeline.getName() + " → " + name);
            pipeline.setName(name);
        }
        String description = request.description() == null || request.description().isBlank() ? null : request.description().trim();
        if (!Objects.equals(description, blankToNull(pipeline.getDescription()))) {
            changes.add("설명 변경");
            pipeline.setDescription(description);
        }
        if (!Objects.equals(request.groupId(), pipeline.getGroupId())) {
            groupService.requireExists(request.groupId());
            changes.add("그룹 " + orDash(pipeline.getGroupId()) + " → " + orDash(request.groupId()));
            pipeline.setGroupId(request.groupId());
        }
    }

    private void applyCdcOptions(PipelineDefinition pipeline, PipelineUpdateRequest request, List<String> changes) {
        String excluded = request.excludedColumns() == null ? pipeline.getExcludedColumns() : joinColumns(request.excludedColumns());
        String masked = request.maskedColumns() == null ? pipeline.getMaskedColumns() : joinColumns(request.maskedColumns());
        boolean excludedChanged = !Objects.equals(excluded, blankToNull(pipeline.getExcludedColumns()));
        boolean maskedChanged = !Objects.equals(masked, blankToNull(pipeline.getMaskedColumns()));
        if (excludedChanged || maskedChanged) {
            validateColumns(pipeline, split(excluded), split(masked));
        }
        if (excludedChanged) {
            changes.add("제외 컬럼 " + orNone(pipeline.getExcludedColumns()) + " → " + orNone(excluded));
            pipeline.setExcludedColumns(excluded);
        }
        if (maskedChanged) {
            changes.add("마스킹 컬럼 " + orNone(pipeline.getMaskedColumns()) + " → " + orNone(masked));
            pipeline.setMaskedColumns(masked);
        }

        if (request.deleteEnabled() != null && !request.deleteEnabled().equals(pipeline.getDeleteEnabled())) {
            if (PipelineLoadMode.from(pipeline.getLoadMode()).isDelta() && request.deleteEnabled()) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "델타 적재는 DELETE 를 행으로 쌓습니다 - «DELETE 반영»을 켤 수 없습니다.");
            }
            changes.add("DELETE 반영 " + onOff(pipeline.getDeleteEnabled()) + " → " + onOff(request.deleteEnabled()));
            pipeline.setDeleteEnabled(request.deleteEnabled());
        }
    }

    /**
     * 생성 화면과 같은 컬럼 규칙을 서버에서도 확인한다. PK 를 빼거나 가리면 싱크의 upsert 키가 깨지고, 마스킹 불가 형식은
     * 커넥터가 거부한다. 소스 DB 에 닿지 않으면 확인할 수 없으므로 저장하지 않는다.
     */
    private void validateColumns(PipelineDefinition pipeline, Set<String> excluded, Set<String> masked) {
        Set<String> overlap = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        overlap.addAll(excluded);
        overlap.retainAll(masked);
        if (!overlap.isEmpty()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "같은 컬럼을 제외와 마스킹에 동시에 지정할 수 없습니다: " + String.join(", ", overlap));
        }
        List<ColumnMetadataResponse> columns;
        try {
            columns = schemaDiscoveryService.listColumns(pipeline.getSourceConnectionId(), pipeline.getSourceSchema(), pipeline.getSourceTable());
        } catch (RuntimeException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "소스 DB 에서 컬럼 정보를 읽지 못해 제외 · 마스킹 컬럼을 확인할 수 없습니다. 연결 상태를 확인한 뒤 다시 저장하세요. (" + ex.getMessage() + ")");
        }
        Map<String, ColumnMetadataResponse> byName = columns.stream().collect(Collectors.toMap(
                column -> column.name().toLowerCase(Locale.ROOT), Function.identity(), (a, b) -> a));
        for (String column : excluded) {
            ColumnMetadataResponse meta = byName.get(column.toLowerCase(Locale.ROOT));
            if (meta == null) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "소스 테이블에 없는 컬럼입니다: " + column);
            }
            if (meta.primaryKey()) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "PK 컬럼은 제외할 수 없습니다: " + column);
            }
        }
        for (String column : masked) {
            ColumnMetadataResponse meta = byName.get(column.toLowerCase(Locale.ROOT));
            if (meta == null) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "소스 테이블에 없는 컬럼입니다: " + column);
            }
            if (meta.primaryKey()) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "PK 컬럼은 마스킹할 수 없습니다: " + column);
            }
            if (!meta.maskable()) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "마스킹할 수 없는 형식의 컬럼입니다: " + column + " (" + meta.dataType() + ")");
            }
        }
    }

    // ------------------------------------------------------------------ 작은 도우미

    /** 생성(PipelineService.buildDefinition)과 같은 저장 모양: 앞뒤 공백 제거 · 중복 제거 · 정렬 · 쉼표로 잇기, 없으면 null. */
    static String joinColumns(List<String> columns) {
        return columns.stream().filter(Objects::nonNull).map(String::trim).filter(value -> !value.isEmpty())
                .distinct().sorted().reduce((left, right) -> left + "," + right).orElse(null);
    }

    private static Set<String> split(String joined) {
        if (joined == null || joined.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(joined.split(",")).map(String::trim).filter(value -> !value.isEmpty())
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    private static boolean isTableCdc(PipelineDefinition pipeline) {
        return !"LOG_FILE".equals(pipeline.getPipelineType());
    }

    private static String trim(String value) {
        return value == null ? null : value.trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String orNone(String value) {
        return value == null || value.isBlank() ? "없음" : value;
    }

    private static String orDash(Object value) {
        return value == null ? "미지정" : String.valueOf(value);
    }

    private static String onOff(Boolean value) {
        return Boolean.TRUE.equals(value) ? "사용" : "사용 안 함";
    }
}
