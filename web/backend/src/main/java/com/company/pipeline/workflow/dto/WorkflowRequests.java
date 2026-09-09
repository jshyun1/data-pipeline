package com.company.pipeline.workflow.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.List;

/** 워크플로우 화면이 보내는 요청 묶음. */
public final class WorkflowRequests {

    private WorkflowRequests() {
    }

    /**
     * 생성 요청.
     *
     * <p>{@code workflowKey}는 dag_id의 축이라 만든 뒤 바꿀 수 없다. 사용자가 직접 짓게 하면
     * 그룹이 다른데 같은 이름을 쓰거나(DW·DZ 둘 다 COM001M이 있다) 규칙을 어긴 값이 들어와
     * 게시 단계에서야 터진다. 그래서 비워 보내면 서버가 그룹·이름에서 만들어 채운다.
     * 값을 보내면 그대로 쓰되 형식은 여전히 검사한다(레거시 dag_id 승계용).
     */
    public record CreateWorkflow(
            @Pattern(regexp = "^[a-z0-9_]{2,80}$",
                    message = "workflowKey는 영문 소문자·숫자·밑줄 2~80자여야 합니다.")
            String workflowKey,
            @NotBlank String name,
            String description,
            String scheduleCron,
            String timezone,
            Boolean catchup,
            @Min(1) Integer maxActiveRuns,
            Boolean suspendOnError) {
    }

    /** 속성 수정. workflowKey는 대상에서 제외한다(이력 연속성). */
    public record UpdateWorkflow(
            @NotBlank String name,
            String description,
            String scheduleCron,
            String timezone,
            Boolean catchup,
            @Min(1) Integer maxActiveRuns,
            Boolean suspendOnError,
            /** 선행 워크플로우 id 목록. 지정하면 스케줄 대신 선행 완료로 실행된다. */
            List<Long> upstreamWorkflowIds,
            /** ALL=모두 완료 후, ANY=하나라도 완료되면. */
            String upstreamMode,
            /** 캔버스·속성창 공용 메모. */
            String memo) {
    }

    /** 캔버스 통째 저장(draft). 노드·엣지를 전량 교체한다. */
    public record SaveGraph(
            List<NodeRequest> nodes,
            List<EdgeRequest> edges) {
    }

    public record NodeRequest(
            @NotBlank String nodeKey,
            String nodeType,
            Long jobId,
            Long subWorkflowId,
            String triggerRule,
            String branchExpr,
            Integer retries,
            Integer retryDelaySec,
            Double displayX,
            Double displayY) {
    }

    public record EdgeRequest(
            @NotBlank String fromNodeKey,
            @NotBlank String toNodeKey,
            String conditionType,
            String conditionExpr) {
    }
}
