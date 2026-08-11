package com.company.pipeline.monitoring;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowResponse.LabelEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorEntity;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiLabelEntity;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * NiFi 원본 캔버스에 프로세스 그룹별 마지막 처리 상태 라벨을 직접 동기화한다.
 *
 * <p>대상은 "바로 하위에 processor가 있는 process group"이다. 라벨은 해당 그룹을 담고
 * 있는 부모 캔버스에 만들어서, 프로세스 그룹 박스 오른쪽에 붙인다. 사람이 보는 라벨에는
 * 식별자를 넣지 않고, metadata-db의 nifi_canvas_status_label에 group_id -> label_id를
 * 저장해 다음 주기에 같은 라벨을 갱신한다.
 */
@Component
public class NifiCanvasStatusOverlayService {

    private static final Logger log = LoggerFactory.getLogger(NifiCanvasStatusOverlayService.class);
    private static final String ROOT_GROUP_ID = "root";
    private static final Set<String> LOAD_PROCESSOR_TYPES = Set.of("PutDatabaseRecord", "ExecuteGroovyScript");
    private static final double LABEL_X_OFFSET = 420.0d;
    private static final double LABEL_Y_OFFSET = 6.0d;
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("MM-dd HH:mm", Locale.KOREA);

    private final NifiClient nifiClient;
    private final NifiProcessorRunRepository runRepository;
    private final NifiExecutionLogEntryRepository executionLogRepository;
    private final NifiCanvasStatusLabelRepository labelRepository;

    public NifiCanvasStatusOverlayService(
            NifiClient nifiClient,
            NifiProcessorRunRepository runRepository,
            NifiExecutionLogEntryRepository executionLogRepository,
            NifiCanvasStatusLabelRepository labelRepository) {
        this.nifiClient = nifiClient;
        this.runRepository = runRepository;
        this.executionLogRepository = executionLogRepository;
        this.labelRepository = labelRepository;
    }

    @Scheduled(fixedRateString = "${pipeline.nifi.canvas-status.interval-millis:60000}",
            initialDelayString = "${pipeline.nifi.canvas-status.initial-delay-millis:90000}")
    @Transactional
    public void syncCanvasStatusLabels() {
        try {
            syncGroup(ROOT_GROUP_ID);
        } catch (RuntimeException ex) {
            log.warn("NiFi 캔버스 상태 라벨 동기화 실패(다음 주기에 재시도): {}", ex.getMessage());
        }
    }

    private void syncGroup(String parentGroupId) {
        NifiFlowResponse parentFlow = nifiClient.getFlow(parentGroupId);
        var flow = flowOf(parentFlow);
        if (flow == null || flow.processGroups() == null || flow.processGroups().isEmpty()) {
            return;
        }

        Map<String, LabelEntity> labelsById = labelsById(flow.labels());
        for (ProcessGroupEntity child : flow.processGroups()) {
            var component = child.component();
            String childGroupId = component != null && component.id() != null ? component.id() : child.id();
            if (childGroupId == null) {
                continue;
            }

            NifiFlowResponse childFlow = nifiClient.getFlow(childGroupId);
            var childContents = flowOf(childFlow);
            List<ProcessorEntity> directProcessors =
                    childContents == null || childContents.processors() == null
                            ? List.of() : childContents.processors();
            if (!directProcessors.isEmpty() && component != null && component.position() != null) {
                upsertLabel(parentGroupId, childGroupId, component, labelsById,
                        summarize(childGroupId, directProcessors));
            }

            syncGroup(childGroupId);
        }
    }

    private void upsertLabel(String parentGroupId, String groupId,
            NifiFlowResponse.ProcessGroupComponent group,
            Map<String, LabelEntity> labelsById,
            StatusSummary summary) {
        String text = labelText(summary);
        double x = valueOrZero(group.position().x()) + LABEL_X_OFFSET;
        double y = valueOrZero(group.position().y()) + LABEL_Y_OFFSET;
        Map<String, String> style = labelStyle(summary.status());

        Optional<NifiCanvasStatusLabel> mapping = labelRepository.findById(groupId);
        LabelEntity existingLabel = mapping.map(NifiCanvasStatusLabel::getLabelId).map(labelsById::get).orElse(null);
        if (existingLabel == null) {
            NifiLabelEntity created = nifiClient.createLabel(parentGroupId, text, x, y, style);
            String labelId = labelId(created);
            if (labelId != null) {
                NifiCanvasStatusLabel entity = mapping.orElseGet(
                        () -> new NifiCanvasStatusLabel(groupId, parentGroupId, labelId));
                entity.update(parentGroupId, labelId);
                labelRepository.save(entity);
            }
            return;
        }

        long version = existingLabel.revision() == null || existingLabel.revision().version() == null
                ? 0L : existingLabel.revision().version();
        var component = existingLabel.component();
        if (component != null
                && Objects.equals(component.label(), text)
                && samePosition(component.position(), x, y)
                && Objects.equals(component.style(), style)) {
            return;
        }
        nifiClient.updateLabel(existingLabel.id(), version, text, x, y, style);
    }

    private StatusSummary summarize(String groupId, List<ProcessorEntity> directProcessors) {
        ProcessorRuntime runtime = runtimeOf(groupId);
        Optional<NifiProcessorRun> latestRun = runRepository.findTopByGroupIdOrderByStartedAtDesc(groupId);
        Optional<NifiExecutionLogEntry> latestFailure = executionLogRepository
                .findTopByGroupIdAndStatusOrderByOccurredAtDesc(groupId, NifiExecutionLogEntry.STATUS_FAILED);

        OverlayStatus status = statusOf(directProcessors, runtime, latestRun, latestFailure);
        LocalDateTime lastTime = latestRun
                .map(run -> run.getEndedAt() != null ? run.getEndedAt() : run.getLastSeenAt())
                .orElse(null);
        long count = latestRun.map(NifiProcessorRun::getInsertedCount).orElse(0L);
        return new StatusSummary(status, lastTime, count);
    }

    private OverlayStatus statusOf(List<ProcessorEntity> directProcessors, ProcessorRuntime runtime,
            Optional<NifiProcessorRun> latestRun, Optional<NifiExecutionLogEntry> latestFailure) {
        if (isLatestFailure(latestRun, latestFailure)) {
            return OverlayStatus.FAILED;
        }
        if (runtime.activeThreadCount() > 0 || latestRun.map(run -> run.getEndedAt() == null).orElse(false)) {
            return OverlayStatus.RUNNING;
        }
        List<ProcessorEntity> loadProcessors = directProcessors.stream()
                .filter(this::isLoadProcessor)
                .toList();
        if (!loadProcessors.isEmpty() && loadProcessors.stream().allMatch(this::isStopped)) {
            return OverlayStatus.STOPPED;
        }
        if (!directProcessors.isEmpty() && directProcessors.stream().allMatch(this::isRunning)
                && runtime.activeThreadCount() == 0) {
            return OverlayStatus.WAITING;
        }
        return latestRun.isPresent() ? OverlayStatus.SUCCESS : OverlayStatus.WAITING;
    }

    private boolean isLatestFailure(Optional<NifiProcessorRun> latestRun, Optional<NifiExecutionLogEntry> latestFailure) {
        if (latestFailure.isEmpty()) {
            return false;
        }
        if (latestRun.isEmpty()) {
            return true;
        }
        return !latestFailure.get().getOccurredAt().isBefore(latestRun.get().getStartedAt());
    }

    private ProcessorRuntime runtimeOf(String groupId) {
        NifiFlowStatusResponse status = nifiClient.getRootFlowStatus();
        List<NifiFlowStatusResponse.ProcessGroupStatusSnapshot> stack = new ArrayList<>();
        var root = status == null || status.processGroupStatus() == null
                ? null : status.processGroupStatus().aggregateSnapshot();
        if (root != null && root.processGroupStatusSnapshots() != null) {
            root.processGroupStatusSnapshots().forEach(entry -> {
                if (entry.processGroupStatusSnapshot() != null) {
                    stack.add(entry.processGroupStatusSnapshot());
                }
            });
        }
        while (!stack.isEmpty()) {
            NifiFlowStatusResponse.ProcessGroupStatusSnapshot group = stack.remove(stack.size() - 1);
            if (groupId.equals(group.id())) {
                int activeThreads = group.processorStatusSnapshots() == null ? 0 : group.processorStatusSnapshots()
                        .stream()
                        .map(NifiFlowStatusResponse.ProcessorStatusEntry::processorStatusSnapshot)
                        .filter(Objects::nonNull)
                        .mapToInt(NifiFlowStatusResponse.ProcessorStatus::activeThreads)
                        .sum();
                return new ProcessorRuntime(activeThreads);
            }
            if (group.processGroupStatusSnapshots() != null) {
                group.processGroupStatusSnapshots().forEach(entry -> {
                    if (entry.processGroupStatusSnapshot() != null) {
                        stack.add(entry.processGroupStatusSnapshot());
                    }
                });
            }
        }
        return new ProcessorRuntime(0);
    }

    private String labelText(StatusSummary summary) {
        String time = summary.lastTime() == null ? "-" : summary.lastTime().format(TIME_FORMATTER);
        return "● " + summary.status().label()
                + "\n마지막 실행 " + time + " · " + String.format("%,d", summary.count()) + "건";
    }

    private Map<String, String> labelStyle(OverlayStatus status) {
        return Map.of(
                "font-size", "15px",
                "font-family", "Arial",
                "color", status.color(),
                "text-align", "left"
        );
    }

    private Map<String, LabelEntity> labelsById(List<LabelEntity> labels) {
        Map<String, LabelEntity> result = new HashMap<>();
        if (labels != null) {
            labels.forEach(label -> result.put(label.id(), label));
        }
        return result;
    }

    private NifiFlowResponse.Flow flowOf(NifiFlowResponse response) {
        return response == null || response.processGroupFlow() == null ? null : response.processGroupFlow().flow();
    }

    private boolean isLoadProcessor(ProcessorEntity processor) {
        var component = processor.component();
        return component != null && LOAD_PROCESSOR_TYPES.contains(component.shortType());
    }

    private boolean isStopped(ProcessorEntity processor) {
        var component = processor.component();
        return component != null && "STOPPED".equalsIgnoreCase(component.state());
    }

    private boolean isRunning(ProcessorEntity processor) {
        var component = processor.component();
        return component != null && "RUNNING".equalsIgnoreCase(component.state());
    }

    private boolean samePosition(NifiFlowResponse.Position position, double x, double y) {
        return position != null
                && Math.abs(valueOrZero(position.x()) - x) < 0.1d
                && Math.abs(valueOrZero(position.y()) - y) < 0.1d;
    }

    private String labelId(NifiLabelEntity entity) {
        if (entity == null) {
            return null;
        }
        return entity.component() != null && entity.component().id() != null
                ? entity.component().id() : entity.id();
    }

    private double valueOrZero(Double value) {
        return value == null ? 0.0d : value;
    }

    private record StatusSummary(OverlayStatus status, LocalDateTime lastTime, long count) {
    }

    private record ProcessorRuntime(int activeThreadCount) {
    }

    private enum OverlayStatus {
        RUNNING("실행 중", "#1677ff"),
        SUCCESS("완료", "#1f2937"),
        FAILED("실패", "#cf1322"),
        WAITING("대기", "#8c8c8c"),
        STOPPED("중지", "#595959");

        private final String label;
        private final String color;

        OverlayStatus(String label, String color) {
            this.label = label;
            this.color = color;
        }

        private String label() {
            return label;
        }

        private String color() {
            return color;
        }
    }
}
