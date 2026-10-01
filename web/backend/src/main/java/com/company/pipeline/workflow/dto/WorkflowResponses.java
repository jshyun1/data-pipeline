package com.company.pipeline.workflow.dto;

import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowEdge;
import com.company.pipeline.workflow.EtlWorkflowNode;
import java.time.LocalDateTime;
import java.util.List;

/** 워크플로우 화면에 내려주는 응답 묶음. */
public final class WorkflowResponses {

    private WorkflowResponses() {
    }

    /** 목록용 요약. 게시 여부가 곧 "DAG가 존재하는가"다. */
    public record WorkflowSummary(
            Long id,
            String workflowKey,
            String dagId,
            String name,
            String description,
            String scheduleCron,
            String timezone,
            int upstreamCount,
            boolean published,
            LocalDateTime publishedAt,
            String publishedBy,
            int nodeCount,
            /** 캔버스에 놓인 JOB 노드 수. nodeCount 는 하위 워크플로우 노드까지 포함한 값이라 다르다. */
            int jobCount,
            /** 이 워크플로우 캔버스에 노드로 놓인 하위 워크플로우 id. 목록 화면이 트리를 세울 때 쓴다. */
            List<Long> childWorkflowIds,
            /**
             * CDC 노드 수. 0 보다 크면 «CDC 워크플로우»다(DAG 팩토리와 같은 기준). 설계 화면이 이 워크플로우를
             * 하위 노드로 놓지 못하게 막는 근거다 - 하위로 넣으면 상위가 돌 때마다 CDC 시작 지시가 나간다.
             */
            int cdcNodeCount,
            /**
             * CDC 노드(노드 키 · 파이프라인 id). CDC 는 워크플로우로만 제어한다(2026-10-01) - 화면이 «이 파이프라인이
             * 어느 워크플로우의 어느 노드인가»를 찾아 그 노드에만 지시를 보낸다.
             */
            List<CdcNodeRef> cdcNodes,
            LocalDateTime updatedAt) {

        public static WorkflowSummary from(EtlWorkflow w, int nodeCount, int upstreamCount) {
            return from(w, nodeCount, upstreamCount, 0, List.of(), List.of());
        }

        public static WorkflowSummary from(EtlWorkflow w, int nodeCount, int upstreamCount,
                                           int jobCount, List<Long> childWorkflowIds, List<CdcNodeRef> cdcNodes) {
            return new WorkflowSummary(
                    w.getId(), w.getWorkflowKey(), w.dagId(), w.getName(), w.getDescription(),
                    w.getScheduleCron(), w.getTimezone(),
                    upstreamCount, w.isPublished(), w.getPublishedAt(), w.getPublishedBy(),
                    nodeCount, jobCount, childWorkflowIds, cdcNodes.size(), cdcNodes, w.getUpdatedAt());
        }
    }

    /** 워크플로우 안의 CDC 노드 하나. */
    public record CdcNodeRef(String nodeKey, Long pipelineId) {
    }

    /** 이 워크플로우를 품고 있는 상위 워크플로우 한 줄. */
    public record ParentRef(Long id, String workflowKey, String name, String dagId,
                            String scheduleCron, boolean published) {
    }

    /** 캔버스 로드용 상세. */
    public record WorkflowDetail(
            Long id,
            String workflowKey,
            String dagId,
            String name,
            String description,
            String scheduleCron,
            String timezone,
            boolean catchup,
            int maxActiveRuns,
            boolean suspendOnError,
            /** 선행 워크플로우 id 목록. 있으면 스케줄 대신 선행 완료로 실행된다. */
            List<Long> upstreamWorkflowIds,
            String upstreamMode,
            /** 캔버스·속성창 공용 메모. */
            String memo,
            /**
             * 이 워크플로우를 «노드로 품고 있는» 워크플로우들.
             *
             * 상위가 있으면 자체 스케줄은 돌지 않는다(상위가 지시할 때만 돈다). 화면이 그
             * 사실과 «누가 상위인지»를 같이 보여줘야 사용자가 스케줄을 헛설정하지 않는다.
             */
            List<ParentRef> parents,
            boolean published,
            LocalDateTime publishedAt,
            String publishedBy,
            /** 게시 후 캔버스를 고쳤는지. 화면이 "게시본과 다름" 배지를 띄우는 근거다. */
            boolean dirty,
            List<NodeView> nodes,
            List<EdgeView> edges,
            LocalDateTime updatedAt) {

        public static WorkflowDetail of(EtlWorkflow w, List<NodeView> nodes, List<EdgeView> edges,
                                        boolean dirty, List<Long> upstreamWorkflowIds,
                                        List<ParentRef> parents) {
            return new WorkflowDetail(
                    w.getId(), w.getWorkflowKey(), w.dagId(), w.getName(), w.getDescription(),
                    w.getScheduleCron(), w.getTimezone(),
                    w.isCatchup(), w.getMaxActiveRuns(), w.isSuspendOnError(),
                    upstreamWorkflowIds, w.getUpstreamMode(), w.getMemo(), parents,
                    w.isPublished(), w.getPublishedAt(), w.getPublishedBy(), dirty,
                    nodes, edges, w.getUpdatedAt());
        }
    }

    /** 노드 + 참조 job의 표시 정보(팔레트에서 끌어온 뒤에도 이름이 보이도록). */
    public record NodeView(
            String nodeKey,
            String nodeType,
            Long jobId,
            String jobName,
            /** 소속 그룹. 체인을 자식 PG로 나누면 잡 이름이 겹쳐서(DW/DZ 아래 COM001M)
             *  캔버스에서 어느 그룹 것인지 이것 없이는 구분할 수 없다. */
            String parentGroupName,
            String nifiPgId,
            Long subWorkflowId,
            String subWorkflowName,
            /** CDC 노드가 가리키는 파이프라인(nodeType=CDC 일 때만). 이름은 캔버스 노드에 적는다. */
            Long pipelineId,
            String pipelineName,
            String triggerRule,
            String branchExpr,
            int retries,
            int retryDelaySec,
            Double displayX,
            Double displayY,
            /** 참조하던 job(또는 CDC 파이프라인)이 사라졌으면 화면에서 빨갛게 표시한다(게시도 막힌다). */
            boolean jobMissing) {

        public static NodeView of(EtlWorkflowNode n, String jobName, String parentGroupName,
                                  String nifiPgId, String subWorkflowName, String pipelineName,
                                  boolean jobMissing) {
            return new NodeView(
                    n.getNodeKey(), n.getNodeType(), n.getJobId(), jobName, parentGroupName, nifiPgId,
                    n.getSubWorkflowId(), subWorkflowName, n.getPipelineId(), pipelineName,
                    n.getTriggerRule(), n.getBranchExpr(), n.getRetries(), n.getRetryDelaySec(),
                    n.getDisplayX(), n.getDisplayY(), jobMissing);
        }
    }

    public record EdgeView(
            String fromNodeKey,
            String toNodeKey,
            String conditionType,
            String conditionExpr) {

        public static EdgeView from(EtlWorkflowEdge e) {
            return new EdgeView(e.getFromNodeKey(), e.getToNodeKey(),
                    e.getConditionType(), e.getConditionExpr());
        }
    }
}
