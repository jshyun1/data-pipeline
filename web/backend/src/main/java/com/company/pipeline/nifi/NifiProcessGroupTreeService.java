package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class NifiProcessGroupTreeService {

    private static final Logger log = LoggerFactory.getLogger(NifiProcessGroupTreeService.class);

    private static final String ROOT_GROUP_ID = "root";
    private static final long CACHE_TTL_MS = 30_000L;
    private static final long CACHE_WARMUP_DELAY_MS = 5_000L;

    private final NifiClient nifiClient;
    private final AtomicReference<CachedTree> cache = new AtomicReference<>();
    private final ReentrantLock refreshLock = new ReentrantLock();

    public NifiProcessGroupTreeService(NifiClient nifiClient) {
        this.nifiClient = nifiClient;
    }

    public NifiProcessGroupTreeResponse getTree() {
        CachedTree cached = cache.get();
        if (cached != null) {
            return cached.tree();
        }

        refreshLock.lock();
        try {
            cached = cache.get();
            if (cached != null) {
                return cached.tree();
            }
            return refreshCache().tree();
        } finally {
            refreshLock.unlock();
        }
    }

    public NifiProcessGroupTreeResponse refreshNow() {
        refreshLock.lock();
        try {
            return refreshCache().tree();
        } finally {
            refreshLock.unlock();
        }
    }

    public void refreshAfterMutation() {
        try {
            refreshNow();
        } catch (RuntimeException ex) {
            cache.set(null);
            log.debug("NiFi 프로세스 그룹 트리 캐시 즉시 갱신 실패, 캐시를 무효화합니다: {}", ex.getMessage());
        }
    }

    @Scheduled(fixedDelay = CACHE_TTL_MS, initialDelay = CACHE_WARMUP_DELAY_MS)
    public void warmTreeCache() {
        CachedTree cached = cache.get();
        if (cached != null && cached.isFresh()) {
            return;
        }
        if (!refreshLock.tryLock()) {
            return;
        }
        try {
            refreshCache();
        } catch (RuntimeException ex) {
            log.debug("NiFi 프로세스 그룹 트리 캐시 예열 실패: {}", ex.getMessage());
        } finally {
            refreshLock.unlock();
        }
    }

    private CachedTree refreshCache() {
        CachedTree refreshed = new CachedTree(buildTree(), System.currentTimeMillis());
        cache.set(refreshed);
        return refreshed;
    }

    private NifiProcessGroupTreeResponse buildTree() {
        return toNode(ROOT_GROUP_ID, "ETL Root", null, new HashSet<>(), collectGroupStatusSnapshots());
    }

    private NifiProcessGroupTreeResponse toNode(String groupId, String fallbackName, String fallbackComments, Set<String> visited,
                                                Map<String, NifiFlowStatusResponse.ProcessGroupStatusSnapshot> statusSnapshots) {
        if (!visited.add(groupId)) {
            return new NifiProcessGroupTreeResponse(groupId, displayName(fallbackName, groupId),
                    fallbackComments,
                    "EMPTY", "WAITING", 0, 0, 0, 0, 0, 0, 0, 0, 0, List.of());
        }

        NifiFlowResponse flow = nifiClient.getFlow(groupId);
        var processGroupFlow = flow == null ? null : flow.processGroupFlow();
        var contents = processGroupFlow == null ? null : processGroupFlow.flow();
        String nodeId = groupId;
        if (!ROOT_GROUP_ID.equals(groupId) && processGroupFlow != null && StringUtils.hasText(processGroupFlow.id())) {
            nodeId = processGroupFlow.id();
        }

        List<NifiProcessGroupTreeResponse> children = contents == null || contents.processGroups() == null
                ? List.of()
                : contents.processGroups().stream()
                        .map(child -> {
                            var component = child.component();
                            String childId = component != null && StringUtils.hasText(component.id())
                                    ? component.id() : child.id();
                            String childName = component == null ? childId : component.name();
                            String childComments = component == null ? null : component.comments();
                            return StringUtils.hasText(childId)
                                    ? toNode(childId, childName, childComments, visited, statusSnapshots)
                                    : null;
                        })
                        .filter(node -> node != null)
                        .sorted(Comparator.comparing(NifiProcessGroupTreeResponse::name, String.CASE_INSENSITIVE_ORDER))
                        .toList();

        DirectJobStatus directJob = DirectJobStatus.from(
                contents == null ? null : contents.processors(),
                contents == null ? null : contents.connections(),
                statusSnapshots.get(nodeId)
        );
        boolean hasDirectProcessors = directJob.total() > 0;
        String groupType = hasDirectProcessors ? "JOB" : children.isEmpty() ? "EMPTY" : "GROUPING";
        JobCounts ownCounts = hasDirectProcessors ? JobCounts.one(directJob.status()) : JobCounts.empty();
        JobCounts totalCounts = children.stream()
                .map(JobCounts::from)
                .reduce(ownCounts, JobCounts::plus);
        int totalJobCount = (hasDirectProcessors ? 1 : 0) + children.stream()
                .mapToInt(NifiProcessGroupTreeResponse::processorCount)
                .sum();
        String jobStatus = hasDirectProcessors ? directJob.status() : totalCounts.status();

        return new NifiProcessGroupTreeResponse(
                nodeId,
                displayName(fallbackName, nodeId),
                fallbackComments,
                groupType,
                jobStatus,
                totalJobCount,
                totalCounts.running(),
                totalCounts.stopped(),
                totalCounts.failed(),
                0,
                directJob.activeThreadCount(),
                directJob.flowFilesQueued(),
                directJob.sourceInputCount(),
                directJob.terminalInputCount(),
                children
        );
    }

    private Map<String, NifiFlowStatusResponse.ProcessGroupStatusSnapshot> collectGroupStatusSnapshots() {
        Map<String, NifiFlowStatusResponse.ProcessGroupStatusSnapshot> snapshots = new HashMap<>();
        try {
            NifiFlowStatusResponse status = nifiClient.getRootFlowStatus();
            var root = status == null || status.processGroupStatus() == null
                    ? null
                    : status.processGroupStatus().aggregateSnapshot();
            if (root == null || root.processGroupStatusSnapshots() == null) {
                return snapshots;
            }
            ArrayDeque<NifiFlowStatusResponse.ProcessGroupStatusEntry> queue = new ArrayDeque<>(root.processGroupStatusSnapshots());
            while (!queue.isEmpty()) {
                var entry = queue.removeFirst();
                var snapshot = entry == null ? null : entry.processGroupStatusSnapshot();
                if (snapshot == null || !StringUtils.hasText(snapshot.id())) {
                    continue;
                }
                snapshots.put(snapshot.id(), snapshot);
                if (snapshot.processGroupStatusSnapshots() != null) {
                    queue.addAll(snapshot.processGroupStatusSnapshots());
                }
            }
        } catch (RuntimeException ex) {
            log.debug("NiFi 상태 스냅샷 조회 실패, 구성 정보만으로 트리를 만듭니다: {}", ex.getMessage());
        }
        return snapshots;
    }

    private String displayName(String name, String fallback) {
        return StringUtils.hasText(name) ? name : fallback;
    }

    private record CachedTree(NifiProcessGroupTreeResponse tree, long refreshedAtMillis) {

        private boolean isFresh() {
            return System.currentTimeMillis() - refreshedAtMillis < CACHE_TTL_MS;
        }
    }

    private record JobCounts(int total, int running, int waiting, int failed, int stopped) {

        private static JobCounts empty() {
            return new JobCounts(0, 0, 0, 0, 0);
        }

        private static JobCounts one(String status) {
            return switch (status) {
                case "STOPPED" -> new JobCounts(1, 0, 0, 0, 1);
                case "FAILED" -> new JobCounts(1, 0, 0, 1, 0);
                case "RUNNING" -> new JobCounts(1, 1, 0, 0, 0);
                default -> new JobCounts(1, 0, 1, 0, 0);
            };
        }

        private static JobCounts from(NifiProcessGroupTreeResponse node) {
            if (node == null) {
                return empty();
            }
            int total = node.processorCount();
            int running = node.runningCount();
            int failed = node.invalidCount();
            int stopped = node.stoppedCount();
            int waiting = Math.max(total - running - failed - stopped, 0);
            return new JobCounts(total, running, waiting, failed, stopped);
        }

        private String status() {
            if (stopped > 0) {
                return "STOPPED";
            }
            if (failed > 0) {
                return "FAILED";
            }
            if (running > 0) {
                return "RUNNING";
            }
            return "WAITING";
        }

        private JobCounts plus(JobCounts other) {
            return new JobCounts(
                    total + other.total(),
                    running + other.running(),
                    waiting + other.waiting(),
                    failed + other.failed(),
                    stopped + other.stopped()
            );
        }
    }

    private record DirectJobStatus(int total, String status, int activeThreadCount, int flowFilesQueued,
                                   int sourceInputCount, int terminalInputCount) {

        private static DirectJobStatus empty() {
            return new DirectJobStatus(0, "WAITING", 0, 0, 0, 0);
        }

        private static DirectJobStatus from(List<NifiFlowResponse.ProcessorEntity> processors,
                                            List<NifiFlowResponse.ConnectionEntity> connections,
                                            NifiFlowStatusResponse.ProcessGroupStatusSnapshot snapshot) {
            if (processors == null || processors.isEmpty()) {
                return empty();
            }

            Map<String, ProcessorStatus> processorStatuses = new HashMap<>();
            Set<String> destinationIds = new HashSet<>();
            Set<String> sourceIds = new HashSet<>();
            for (NifiFlowResponse.ProcessorEntity entity : processors) {
                var component = entity == null ? null : entity.component();
                if (component == null) {
                    continue;
                }
                String processorId = StringUtils.hasText(component.id()) ? component.id() : entity.id();
                if (StringUtils.hasText(processorId)) {
                    processorStatuses.put(processorId, ProcessorStatus.from(component));
                }
            }
            if (processorStatuses.isEmpty()) {
                return empty();
            }

            if (connections != null) {
                for (NifiFlowResponse.ConnectionEntity connection : connections) {
                    var component = connection == null ? null : connection.component();
                    if (component == null || component.source() == null || component.destination() == null) {
                        continue;
                    }
                    if (processorStatuses.containsKey(component.source().id())
                            && processorStatuses.containsKey(component.destination().id())) {
                        sourceIds.add(component.source().id());
                        destinationIds.add(component.destination().id());
                    }
                }
            }

            Map<String, RuntimeStatus> runtimeStatuses = RuntimeStatus.from(snapshot);
            int groupActiveThreadCount = snapshot == null ? 0 : snapshot.activeThreads();
            int processorActiveThreadCount = 0;
            int flowFilesQueued = snapshot == null ? 0 : snapshot.queuedFlowFiles();
            boolean stopped = false;
            boolean failed = false;
            boolean allRunning = true;
            for (var entry : processorStatuses.entrySet()) {
                ProcessorStatus status = entry.getValue();
                RuntimeStatus runtime = runtimeStatuses.get(entry.getKey());
                stopped = stopped || status.stopped();
                failed = failed || status.failed() || (runtime != null && runtime.failed());
                allRunning = allRunning && (status.running() || (runtime != null && runtime.running()));
                processorActiveThreadCount += runtime == null ? 0 : runtime.activeThreadCount();
            }
            int activeThreadCount = runtimeStatuses.isEmpty() ? groupActiveThreadCount : processorActiveThreadCount;

            List<String> firstProcessorIds = processorStatuses.keySet().stream()
                    .filter(id -> !destinationIds.contains(id))
                    .toList();
            List<String> terminalProcessorIds = processorStatuses.keySet().stream()
                    .filter(id -> !sourceIds.contains(id))
                    .toList();
            int sourceInputCount = inputCount(firstProcessorIds, runtimeStatuses);
            int terminalInputCount = inputCount(terminalProcessorIds, runtimeStatuses);

            String status;
            if (stopped) {
                status = "STOPPED";
            } else if (failed) {
                status = "FAILED";
            } else if (allRunning && activeThreadCount == 0 && flowFilesQueued == 0
                    && sourceInputCount == 0 && terminalInputCount == 0) {
                status = "WAITING";
            } else if (allRunning || activeThreadCount > 0 || flowFilesQueued > 0) {
                status = "RUNNING";
            } else {
                status = "FAILED";
            }
            return new DirectJobStatus(1, status, activeThreadCount, flowFilesQueued, sourceInputCount, terminalInputCount);
        }

        private static int inputCount(List<String> processorIds, Map<String, RuntimeStatus> runtimeStatuses) {
            return processorIds.stream()
                    .map(runtimeStatuses::get)
                    .mapToInt(runtime -> runtime == null ? 0 : runtime.flowFilesReceived())
                    .sum();
        }
    }

    private record ProcessorStatus(boolean running, boolean stopped, boolean failed) {

        private static ProcessorStatus from(NifiFlowResponse.ProcessorComponent processor) {
            String state = normalized(processor.state());
            String validationStatus = normalized(processor.validationStatus());
            boolean invalid = "INVALID".equals(state) || "INVALID".equals(validationStatus);
            boolean running = "RUNNING".equals(state);
            boolean stopped = "STOPPED".equals(state) || "DISABLED".equals(state);
            return new ProcessorStatus(running, stopped, invalid);
        }

        private static String normalized(String value) {
            return value == null ? "" : value.trim().toUpperCase();
        }
    }

    private record RuntimeStatus(boolean running, boolean failed, int activeThreadCount, int flowFilesReceived) {

        private static Map<String, RuntimeStatus> from(NifiFlowStatusResponse.ProcessGroupStatusSnapshot snapshot) {
            if (snapshot == null || snapshot.processorStatusSnapshots() == null) {
                return Map.of();
            }
            Map<String, RuntimeStatus> statuses = new HashMap<>();
            for (var entry : snapshot.processorStatusSnapshots()) {
                var processor = entry == null ? null : entry.processorStatusSnapshot();
                if (processor == null || !StringUtils.hasText(processor.id())) {
                    continue;
                }
                String runStatus = ProcessorStatus.normalized(processor.runStatus());
                statuses.put(processor.id(), new RuntimeStatus(
                        "RUNNING".equals(runStatus),
                        "INVALID".equals(runStatus),
                        processor.activeThreads(),
                        processor.flowFilesReceived() == null ? 0 : processor.flowFilesReceived()
                ));
            }
            return statuses;
        }
    }
}
