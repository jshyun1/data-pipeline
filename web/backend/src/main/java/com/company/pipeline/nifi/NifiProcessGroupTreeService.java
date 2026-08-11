package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class NifiProcessGroupTreeService {

    private static final String ROOT_GROUP_ID = "root";

    private final NifiClient nifiClient;

    public NifiProcessGroupTreeService(NifiClient nifiClient) {
        this.nifiClient = nifiClient;
    }

    public NifiProcessGroupTreeResponse getTree() {
        return toNode(ROOT_GROUP_ID, "NiFi Flow", new HashSet<>());
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

        int directProcessorCount = contents == null || contents.processors() == null ? 0 : contents.processors().size();
        int totalProcessorCount = directProcessorCount + children.stream()
                .mapToInt(NifiProcessGroupTreeResponse::processorCount)
                .sum();
        GroupCounts directCounts = GroupCounts.from(contents == null ? null : contents.processors());
        GroupCounts totalCounts = children.stream()
                .map(GroupCounts::from)
                .reduce(directCounts, GroupCounts::plus);

        return new NifiProcessGroupTreeResponse(
                nodeId,
                displayName(fallbackName, nodeId),
                totalProcessorCount,
                totalCounts.running(),
                totalCounts.stopped(),
                totalCounts.invalid(),
                totalCounts.disabled(),
                children
        );
    }

    private String displayName(String name, String fallback) {
        return StringUtils.hasText(name) ? name : fallback;
    }

    private record GroupCounts(int running, int stopped, int invalid, int disabled) {

        private static GroupCounts empty() {
            return new GroupCounts(0, 0, 0, 0);
        }

        private static GroupCounts from(List<NifiFlowResponse.ProcessorEntity> processors) {
            if (processors == null) {
                return empty();
            }
            return processors.stream()
                    .map(processor -> processor == null ? null : processor.component())
                    .map(GroupCounts::from)
                    .reduce(empty(), GroupCounts::plus);
        }

        private static GroupCounts from(NifiFlowResponse.ProcessorComponent processor) {
            if (processor == null) {
                return empty();
            }
            String state = normalized(processor.state());
            String validationStatus = normalized(processor.validationStatus());
            boolean invalid = "INVALID".equals(state) || "INVALID".equals(validationStatus);
            if (invalid) {
                return new GroupCounts(0, 0, 1, 0);
            }
            if ("RUNNING".equals(state)) {
                return new GroupCounts(1, 0, 0, 0);
            }
            if ("DISABLED".equals(state)) {
                return new GroupCounts(0, 1, 0, 1);
            }
            if ("STOPPED".equals(state)) {
                return new GroupCounts(0, 1, 0, 0);
            }
            return empty();
        }

        private static GroupCounts from(NifiProcessGroupTreeResponse node) {
            if (node == null) {
                return empty();
            }
            return new GroupCounts(node.runningCount(), node.stoppedCount(), node.invalidCount(), node.disabledCount());
        }

        private GroupCounts plus(GroupCounts other) {
            return new GroupCounts(
                    running + other.running(),
                    stopped + other.stopped(),
                    invalid + other.invalid(),
                    disabled + other.disabled()
            );
        }

        private static String normalized(String value) {
            return value == null ? "" : value.trim().toUpperCase();
        }
    }
}
