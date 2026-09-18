package com.company.pipeline.workflow;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EtlWorkflowNodeRepository extends JpaRepository<EtlWorkflowNode, Long> {

    List<EtlWorkflowNode> findByWorkflowIdAndDeletedAtIsNull(Long workflowId);

    /** 이 워크플로우를 노드로 품고 있는 곳들(상위 워크플로우 찾기). */
    List<EtlWorkflowNode> findBySubWorkflowIdAndDeletedAtIsNull(Long subWorkflowId);

    /** job 삭제 가드용 - 이 job을 참조 중인 노드가 있으면 지우지 못하게 한다. */
    List<EtlWorkflowNode> findByJobIdAndDeletedAtIsNull(Long jobId);

    /** CDC 파이프라인 삭제 가드용 - job 쪽과 같은 이유다(가리키던 노드가 허공을 가리키게 된다). */
    List<EtlWorkflowNode> findByPipelineIdAndDeletedAtIsNull(Long pipelineId);

    /**
     * 파이프라인을 지우기 전에 남은 참조를 비운다. 워크플로우는 소프트 삭제라 지운 워크플로우의 노드가
     * 그대로 남아 이 파이프라인을 가리킨다 - 비우지 않으면 FK 위반으로 삭제가 500 이 된다(2026-09-17 검증 중 발견).
     * 살아 있는 워크플로우의 참조는 이 전에 삭제 가드가 막는다.
     */
    @Modifying
    @Query("UPDATE EtlWorkflowNode n SET n.pipelineId = null WHERE n.pipelineId = :pipelineId")
    int clearPipelineReference(@Param("pipelineId") Long pipelineId);

    void deleteByWorkflowId(Long workflowId);
}
