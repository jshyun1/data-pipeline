package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.monitoring.RealtimePipelineMetricService;
import com.company.pipeline.monitoring.dto.RealtimePipelineMetricResponse;
import com.company.pipeline.pipeline.PipelineCommandHistory;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineGroup;
import com.company.pipeline.pipeline.PipelineGroupRepository;
import com.company.pipeline.pipeline.PipelineRuntimeStatusService;
import com.company.pipeline.pipeline.dto.PipelineRuntimeStatusResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CDC 로그 오류 분석 · CDC 속성창 질문의 정보 수집(대상 {@code CDC_PIPELINE}).
 *
 * <p>화면이 보내는 것: {@code pipelineId} 와 (로그에서 부를 때) 펼친 줄의 메시지. 서버가 붙이는 것:
 * 소스 → 타깃 테이블 · 적재 방식, 커넥터 실측 상태, <b>실패한 커넥터 태스크의 오류 추적(trace)</b>, 최근 제어 명령 이력.
 *
 * <p>실패 정책을 «멈춤»(errors.tolerance=none)으로 바꾸면 CDC 실패 정보는 DLQ 가 아니라 커넥터 태스크가 멈춘 원인이 된다
 * (docs/2026-09-09-cdc-error-policy-dlq.md) - 그래서 태스크 trace 를 Kafka Connect 에서 직접 읽어 붙인다.
 *
 * <p>연결정보(접속 호스트 · 계정 · 비밀번호)와 커넥터 설정 원문(JDBC URL · 비밀번호가 들어 있다)은 보내지 않는다.
 */
@Component
public class CdcPipelineContextCollector implements LlmContextCollector {

    private static final Logger log = LoggerFactory.getLogger(CdcPipelineContextCollector.class);
    private static final int RECENT_COMMANDS = 8;
    /** Java 스택 추적은 수백 줄이다. 앞부분(최초 예외)과 «Caused by» 줄이면 원인은 다 나온다. */
    private static final int TRACE_HEAD_LINES = 12;
    private static final int MAX_CAUSE_LINES = 10;
    private static final int MAX_GROUP_DEPTH = 10;

    private final PipelineDefinitionRepository pipelineRepository;
    private final PipelineGroupRepository groupRepository;
    private final PipelineConnectorRepository connectorRepository;
    private final PipelineCommandHistoryRepository commandHistoryRepository;
    private final PipelineRuntimeStatusService runtimeStatusService;
    private final KafkaConnectClient kafkaConnectClient;
    private final RealtimePipelineMetricService metricService;

    public CdcPipelineContextCollector(PipelineDefinitionRepository pipelineRepository, PipelineGroupRepository groupRepository,
                                       PipelineConnectorRepository connectorRepository,
                                       PipelineCommandHistoryRepository commandHistoryRepository,
                                       PipelineRuntimeStatusService runtimeStatusService,
                                       KafkaConnectClient kafkaConnectClient,
                                       RealtimePipelineMetricService metricService) {
        this.pipelineRepository = pipelineRepository;
        this.groupRepository = groupRepository;
        this.connectorRepository = connectorRepository;
        this.commandHistoryRepository = commandHistoryRepository;
        this.runtimeStatusService = runtimeStatusService;
        this.kafkaConnectClient = kafkaConnectClient;
        this.metricService = metricService;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.CDC_PIPELINE;
    }

    @Override
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String rawId = ContextText.required(target, "pipelineId");
        long pipelineId;
        try {
            pipelineId = Long.parseLong(rawId);
        } catch (NumberFormatException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "파이프라인 id 형식이 올바르지 않습니다: " + rawId);
        }
        PipelineDefinition pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND,
                        "CDC 파이프라인을 찾을 수 없습니다: " + pipelineId));
        List<PipelineConnector> connectors = connectorRepository.findByPipelineId(pipelineId);

        LlmContext context = new LlmContext();
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);
        context.add("CDC 파이프라인", identity(pipeline), LlmContext.PRIORITY_IDENTITY);
        context.add("실행 상태", runtime(pipelineId), LlmContext.PRIORITY_IDENTITY);
        context.add("배포된 커넥터", connectorList(connectors), LlmContext.PRIORITY_IDENTITY);
        context.add("오프셋 · 지연", offsets(pipelineId), LlmContext.PRIORITY_IDENTITY);
        context.add("커넥터 태스크 오류 추적", connectorTraces(connectors), LlmContext.PRIORITY_DETAIL);
        context.add("최근 제어 명령 (최신순)", recentCommands(pipelineId), LlmContext.PRIORITY_HISTORY);
        return new CollectedTarget(String.valueOf(pipelineId), pipeline.getName(), context);
    }

    private String identity(PipelineDefinition p) {
        List<String> lines = new ArrayList<>();
        lines.add("이름: " + p.getName());
        List<String> groups = groupPath(p.getGroupId());
        if (!groups.isEmpty()) {
            lines.add("그룹: " + String.join(" > ", groups));
        }
        lines.add("유형: " + ContextText.orDash(p.getPipelineType()));
        lines.add("소스: " + ContextText.orDash(p.getSourceDbType()) + " " + qualified(p.getSourceSchema(), p.getSourceTable()));
        lines.add("타깃: " + ContextText.orDash(p.getTargetDbType()) + " " + qualified(p.getTargetSchema(), p.getTargetTable()));
        lines.add("Kafka Topic: " + ContextText.orDash(p.getTopicName()));
        lines.add("적재 방식: " + ContextText.orDash(p.getLoadMode())
                + (p.getDeltaOpColumn() != null ? " (변경 구분 컬럼 " + p.getDeltaOpColumn() + ")" : ""));
        lines.add("반영 이벤트: INSERT " + yesNo(p.getInsertEnabled()) + " · UPDATE " + yesNo(p.getUpdateEnabled())
                + " · DELETE " + yesNo(p.getDeleteEnabled()));
        lines.add("초기 적재(snapshot) 방식: " + ContextText.orDash(p.getSnapshotMode()));
        if (p.getExcludedColumns() != null && !p.getExcludedColumns().isBlank()) {
            lines.add("제외 컬럼: " + p.getExcludedColumns());
        }
        if (p.getMaskedColumns() != null && !p.getMaskedColumns().isBlank()) {
            lines.add("마스킹 컬럼: " + p.getMaskedColumns());
        }
        lines.add("저장된 상태: " + ContextText.orDash(p.getStatus()));
        // 탭을 열자마자 나가는 요약이 «누가 · 언제 만들었고 언제 고쳤는지»까지 말할 수 있게 한다(2026-09-27).
        lines.add("등록자: " + ContextText.orDash(p.getCreatedBy())
                + " · 등록 시각: " + ContextText.time(p.getCreatedAt())
                + " · 최종 수정: " + ContextText.time(p.getUpdatedAt()));
        return String.join("\n", lines);
    }

    private String runtime(long pipelineId) {
        try {
            PipelineRuntimeStatusResponse s = runtimeStatusService.get(pipelineId);
            List<String> lines = new ArrayList<>();
            lines.add("실측 상태: " + ContextText.orDash(s.runtimeStatus())
                    + (s.statusMismatch() ? " (저장된 상태 " + s.storedStatus() + " 와 다름)" : ""));
            if (s.runtimeStatusReason() != null) {
                lines.add("판정 사유: " + s.runtimeStatusReason());
            }
            lines.add("소스 커넥터: " + ContextText.orDash(s.sourceConnectorState()) + " / 태스크 " + s.sourceTaskStates());
            lines.add("싱크 커넥터: " + ContextText.orDash(s.sinkConnectorState()) + " / 태스크 " + s.sinkTaskStates());
            lines.add("확인 시각: " + ContextText.time(s.runtimeCheckedAt()));
            return String.join("\n", lines);
        } catch (RuntimeException ex) {
            log.debug("CDC 실행 상태 조회 실패 pipelineId={}: {}", pipelineId, ex.getMessage());
            return "(Kafka Connect 에서 실행 상태를 읽지 못했다: " + ContextText.cut(ex.getMessage(), 200) + ")";
        }
    }

    /**
     * 배포된 커넥터 목록. 이름 규칙이 {@code source-{id}-…} · {@code sink-{id}-…} 라 이름만 봐도 어느 쪽인지 알 수 있고,
     * 운영자가 Kafka Connect 에서 직접 찾을 때 쓰는 열쇠이기도 하다.
     */
    private String connectorList(List<PipelineConnector> connectors) {
        if (connectors.isEmpty()) {
            return "(배포된 커넥터 없음 - 아직 배포하지 않았거나 삭제됐다)";
        }
        List<String> lines = new ArrayList<>();
        for (PipelineConnector c : connectors) {
            lines.add(ContextText.orDash(c.getConnectorRole()) + ": " + ContextText.orDash(c.getConnectorName())
                    + " · 저장된 상태 " + ContextText.orDash(c.getStatus())
                    + " · 배포 " + ContextText.time(c.getDeployedAt()));
        }
        return String.join("\n", lines);
    }

    /**
     * Topic 오프셋과 지연. «지금 얼마나 밀렸는지»는 커넥터 상태(RUNNING)만으로는 알 수 없다 -
     * 커넥터가 돌아도 소비가 못 따라가면 타깃은 계속 뒤처진다.
     */
    private String offsets(long pipelineId) {
        try {
            RealtimePipelineMetricResponse m = metricService.getMetric(pipelineId);
            if (m == null || "NO_DATA".equals(m.collectionStatus())) {
                return "(수집된 지표 없음 - 아직 한 번도 수집되지 않았거나 파이프라인이 멈춰 있다)";
            }
            List<String> lines = new ArrayList<>();
            lines.add("파티션 수: " + ContextText.orDash(String.valueOf(m.partitionCount())));
            lines.add("Topic 끝 오프셋: " + ContextText.orDash(String.valueOf(m.endOffset()))
                    + " · 소비(커밋) 오프셋: " + ContextText.orDash(String.valueOf(m.committedOffset())));
            lines.add("소비 지연(lag): " + ContextText.orDash(String.valueOf(m.consumerLag()))
                    + (m.estimatedRecoverySeconds() != null ? " · 이 속도면 해소까지 약 " + m.estimatedRecoverySeconds() + "초" : ""));
            lines.add("처리량: 초당 " + String.format("%.1f", m.throughputPerSecond()) + "건");
            lines.add("마지막 처리 시각: " + ContextText.time(m.lastProgressAt()));
            lines.add("지표 수집 시각: " + ContextText.time(m.collectedAt()));
            return String.join("\n", lines);
        } catch (RuntimeException ex) {
            log.debug("CDC 지표 조회 실패 pipelineId={}: {}", pipelineId, ex.getMessage());
            return "(지표를 읽지 못했다: " + ContextText.cut(ex.getMessage(), 200) + ")";
        }
    }

    private String connectorTraces(List<PipelineConnector> connectors) {
        List<String> blocks = new ArrayList<>();
        for (PipelineConnector connector : connectors) {
            ConnectorStatusResponse status;
            try {
                status = kafkaConnectClient.getStatus(connector.getConnectorName());
            } catch (RuntimeException ex) {
                continue;   // 실행 상태 구역이 이미 «읽지 못했다»를 알린다
            }
            if (status == null || status.tasks() == null) {
                continue;
            }
            for (ConnectorTaskStatus task : status.tasks()) {
                if (task.trace() != null && !task.trace().isBlank()) {
                    blocks.add("### " + connector.getConnectorRole() + " 커넥터 태스크 " + task.id() + " (" + task.state() + ")\n"
                            + summarizeTrace(task.trace()));
                }
            }
        }
        return blocks.isEmpty() ? null : String.join("\n\n", blocks);
    }

    /** 스택 추적에서 앞 몇 줄과 «Caused by» 줄만 남긴다. 프레임(at …) 수백 줄은 원인 파악에 쓰이지 않는다. */
    static String summarizeTrace(String trace) {
        List<String> lines = Arrays.asList(trace.split("\\R"));
        List<String> head = lines.stream().limit(TRACE_HEAD_LINES).collect(Collectors.toList());
        List<String> causes = lines.stream().skip(TRACE_HEAD_LINES)
                .filter(line -> line.stripLeading().startsWith("Caused by:"))
                .limit(MAX_CAUSE_LINES)
                .toList();
        List<String> out = new ArrayList<>(head);
        if (lines.size() > TRACE_HEAD_LINES) {
            out.add("\t…");
            out.addAll(causes);
        }
        return String.join("\n", out);
    }

    private String recentCommands(long pipelineId) {
        List<PipelineCommandHistory> commands = commandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(pipelineId);
        if (commands.isEmpty()) {
            return null;
        }
        return commands.stream().limit(RECENT_COMMANDS).map(c -> {
            String line = ContextText.time(c.getRequestedAt()) + " | " + c.getCommand() + " | "
                    + ContextText.orDash(c.getResult());
            return c.getMessage() == null ? line : line + " | " + ContextText.cut(c.getMessage(), 300);
        }).collect(Collectors.joining("\n"));
    }

    private List<String> groupPath(Long groupId) {
        List<String> path = new ArrayList<>();
        Long current = groupId;
        for (int depth = 0; current != null && depth < MAX_GROUP_DEPTH; depth++) {
            PipelineGroup group = groupRepository.findById(current).orElse(null);
            if (group == null) {
                break;
            }
            path.add(group.getName());
            current = group.getParentId();
        }
        Collections.reverse(path);
        return path;
    }

    private static String qualified(String schema, String table) {
        if (table == null) {
            return "-";
        }
        return schema == null || schema.isBlank() ? table : schema + "." + table;
    }

    private static String yesNo(Boolean value) {
        return Boolean.FALSE.equals(value) ? "끔" : "켬";
    }
}
