package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiFlowResponse;
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
        return toNode(ROOT_GROUP_ID, "ETL Root", new HashSet<>());
    }

    private NifiProcessGroupTreeResponse toNode(String groupId, String fallbackName, Set<String> visited) {
        if (!visited.add(groupId)) {
            return new NifiProcessGroupTreeResponse(groupId, displayName(fallbackName, groupId),
                    0, 0, 0, 0, 0, List.of());
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
                            return StringUtils.hasText(childId)
                                    ? toNode(childId, childName, visited)
                                    : null;
                        })
                        .filter(node -> node != null)
                        .sorted(Comparator.comparing(NifiProcessGroupTreeResponse::name, String.CASE_INSENSITIVE_ORDER))
                        .toList();

        JobCounts directCounts = JobCounts.from(contents == null ? null : contents.processors(),
                contents == null ? null : contents.connections());
        JobCounts totalCounts = children.stream()
                .map(JobCounts::from)
                .reduce(directCounts, JobCounts::plus);
        int totalJobCount = directCounts.total() + children.stream()
                .mapToInt(NifiProcessGroupTreeResponse::processorCount)
                .sum();

        return new NifiProcessGroupTreeResponse(
                nodeId,
                displayName(fallbackName, nodeId),
                totalJobCount,
                totalCounts.running(),
                totalCounts.stopped(),
                totalCounts.failed(),
                0,
                children
        );
    }

    private String displayName(String name, String fallback) {
        return StringUtils.hasText(name) ? name : fallback;
    }

    private record CachedTree(NifiProcessGroupTreeResponse tree, long refreshedAtMillis) {

        private boolean isFresh() {
            return System.currentTimeMillis() - refreshedAtMillis < CACHE_TTL_MS;
        }
    }

    private record JobCounts(int total, int running, int completed, int failed, int stopped) {

        private static JobCounts empty() {
            return new JobCounts(0, 0, 0, 0, 0);
        }

        private static JobCounts from(List<NifiFlowResponse.ProcessorEntity> processors,
                                      List<NifiFlowResponse.ConnectionEntity> connections) {
            if (processors == null || processors.isEmpty()) {
                return empty();
            }

            Map<String, ProcessorStatus> processorStatuses = new HashMap<>();
            Map<String, Set<String>> adjacency = new HashMap<>();
            for (NifiFlowResponse.ProcessorEntity entity : processors) {
                var component = entity == null ? null : entity.component();
                if (component == null) {
                    continue;
                }
                String processorId = StringUtils.hasText(component.id()) ? component.id() : entity.id();
                if (!StringUtils.hasText(processorId)) {
                    continue;
                }
                processorStatuses.put(processorId, ProcessorStatus.from(component));
                adjacency.put(processorId, new HashSet<>());
            }

            if (connections != null) {
                for (NifiFlowResponse.ConnectionEntity connection : connections) {
                    var component = connection == null ? null : connection.component();
                    if (component == null || component.source() == null || component.destination() == null) {
                        continue;
                    }
                    String sourceId = component.source().id();
                    String destinationId = component.destination().id();
                    if (processorStatuses.containsKey(sourceId) && processorStatuses.containsKey(destinationId)) {
                        adjacency.get(sourceId).add(destinationId);
                        adjacency.get(destinationId).add(sourceId);
                    }
                }
            }

            JobCounts result = empty();
            Set<String> visited = new HashSet<>();
            for (String processorId : processorStatuses.keySet()) {
                if (visited.contains(processorId)) {
                    continue;
                }
                result = result.plus(countConnectedJob(processorId, processorStatuses, adjacency, visited));
            }
            return result;
        }

        private static JobCounts countConnectedJob(String firstProcessorId,
                                                   Map<String, ProcessorStatus> processorStatuses,
                                                   Map<String, Set<String>> adjacency,
                                                   Set<String> visited) {
            ArrayDeque<String> queue = new ArrayDeque<>();
            queue.add(firstProcessorId);
            boolean failed = false;
            boolean running = false;
            boolean allStopped = true;

            while (!queue.isEmpty()) {
                String processorId = queue.removeFirst();
                if (!visited.add(processorId)) {
                    continue;
                }
                ProcessorStatus status = processorStatuses.get(processorId);
                if (status != null) {
                    failed = failed || status.failed();
                    running = running || status.running();
                    allStopped = allStopped && status.stopped();
                }
                for (String next : adjacency.getOrDefault(processorId, Set.of())) {
                    if (!visited.contains(next)) {
                        queue.add(next);
                    }
                }
            }

            if (failed) {
                return new JobCounts(1, 0, 0, 1, 0);
            }
            if (running) {
                return new JobCounts(1, 1, 0, 0, 0);
            }
            if (allStopped) {
                return new JobCounts(1, 0, 0, 0, 1);
            }
            return new JobCounts(1, 0, 1, 0, 0);
        }

        private static JobCounts from(NifiProcessGroupTreeResponse node) {
            if (node == null) {
                return empty();
            }
            int total = node.processorCount();
            int running = node.runningCount();
            int failed = node.invalidCount();
            int stopped = node.stoppedCount();
            int completed = Math.max(total - running - failed - stopped, 0);
            return new JobCounts(total, running, completed, failed, stopped);
        }

        private JobCounts plus(JobCounts other) {
            return new JobCounts(
                    total + other.total(),
                    running + other.running(),
                    completed + other.completed(),
                    failed + other.failed(),
                    stopped + other.stopped()
            );
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
}
