package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowEdge;
import com.company.pipeline.workflow.EtlWorkflowEdgeRepository;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** 워크플로우 설계(노드 · 연결 · 상위 경로)를 글로 만든다. 워크플로우 task · 노드 수집기가 같이 쓴다. */
@Component
public class WorkflowGraphText {

    private static final int MAX_PARENT_DEPTH = 5;

    private final EtlWorkflowNodeRepository nodeRepository;
    private final EtlWorkflowEdgeRepository edgeRepository;
    private final EtlJobRepository jobRepository;

    public WorkflowGraphText(EtlWorkflowNodeRepository nodeRepository, EtlWorkflowEdgeRepository edgeRepository,
                             EtlJobRepository jobRepository) {
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
        this.jobRepository = jobRepository;
    }

    public record Neighbors(List<String> predecessors, List<String> successors) {
    }

    /** 노드 키로 선행 · 후행 노드 이름을 찾는다(저장된 설계 기준). */
    public Neighbors neighbors(Long workflowId, String nodeKey, List<EtlWorkflowNode> nodes, Map<Long, EtlWorkflow> workflowById) {
        Map<String, EtlWorkflowNode> byKey = nodes.stream()
                .collect(Collectors.toMap(EtlWorkflowNode::getNodeKey, Function.identity(), (a, b) -> a));
        List<EtlWorkflowEdge> edges = edgeRepository.findByWorkflowId(workflowId);
        List<String> predecessors = edges.stream().filter(e -> nodeKey.equals(e.getToNodeKey()))
                .map(e -> label(byKey.get(e.getFromNodeKey()), e.getFromNodeKey(), workflowById) + condition(e))
                .toList();
        List<String> successors = edges.stream().filter(e -> nodeKey.equals(e.getFromNodeKey()))
                .map(e -> label(byKey.get(e.getToNodeKey()), e.getToNodeKey(), workflowById) + condition(e))
                .toList();
        return new Neighbors(predecessors, successors);
    }

    private static String condition(EtlWorkflowEdge edge) {
        return edge.getConditionType() == null || "SUCCESS".equals(edge.getConditionType())
                ? "" : " (연결 조건 " + edge.getConditionType() + ")";
    }

    /** 노드를 사람이 읽는 이름으로: job 이름 · 하위 워크플로우 이름 · 그 밖엔 «키 (유형)». */
    public String label(EtlWorkflowNode node, String key, Map<Long, EtlWorkflow> workflowById) {
        if (node == null) {
            return key;
        }
        if (node.getJobId() != null) {
            return jobRepository.findById(node.getJobId()).map(EtlJob::getJobName).orElse(key);
        }
        if (node.getSubWorkflowId() != null && workflowById.containsKey(node.getSubWorkflowId())) {
            return workflowById.get(node.getSubWorkflowId()).getName();
        }
        return key + " (" + node.getNodeType() + ")";
    }

    /** 이 워크플로우를 하위로 부르는 상위 워크플로우 이름들(가장 위부터). 여러 곳에서 부르면 첫 경로만. */
    public List<String> parentPath(Long workflowId, Map<Long, EtlWorkflow> workflowById) {
        List<String> path = new ArrayList<>();
        Long current = workflowId;
        for (int depth = 0; depth < MAX_PARENT_DEPTH; depth++) {
            Optional<Long> parent = nodeRepository.findBySubWorkflowIdAndDeletedAtIsNull(current).stream()
                    .map(EtlWorkflowNode::getWorkflowId)
                    .filter(workflowById::containsKey)
                    .findFirst();
            if (parent.isEmpty() || path.contains(workflowById.get(parent.get()).getName())) {
                break;
            }
            path.add(workflowById.get(parent.get()).getName());
            current = parent.get();
        }
        Collections.reverse(path);
        return path;
    }

    /** 워크플로우 안의 노드들을 한 줄씩(하위 워크플로우 노드 설명용). */
    public String nodeList(Long workflowId, Map<Long, EtlWorkflow> workflowById) {
        List<EtlWorkflowNode> nodes = nodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflowId);
        if (nodes.isEmpty()) {
            return "(노드 없음)";
        }
        return nodes.stream()
                .filter(n -> n.getJobId() != null || n.getSubWorkflowId() != null)
                .map(n -> {
                    Neighbors neighbors = neighbors(workflowId, n.getNodeKey(), nodes, workflowById);
                    String kind = n.getSubWorkflowId() != null ? "하위 워크플로우" : "job";
                    return "- " + label(n, n.getNodeKey(), workflowById) + " (" + kind + ")"
                            + (neighbors.predecessors().isEmpty() ? "" : " ← 선행: " + String.join(", ", neighbors.predecessors()));
                })
                .collect(Collectors.joining("\n"));
    }
}
