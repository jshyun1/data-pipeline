package com.company.pipeline.workflow;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlWorkflowNodeRepository extends JpaRepository<EtlWorkflowNode, Long> {

    List<EtlWorkflowNode> findByWorkflowIdAndDeletedAtIsNull(Long workflowId);

    /** 이 워크플로우를 노드로 품고 있는 곳들(상위 워크플로우 찾기). */
    List<EtlWorkflowNode> findBySubWorkflowIdAndDeletedAtIsNull(Long subWorkflowId);

    /** job 삭제 가드용 - 이 job을 참조 중인 노드가 있으면 지우지 못하게 한다. */
    List<EtlWorkflowNode> findByJobIdAndDeletedAtIsNull(Long jobId);

    void deleteByWorkflowId(Long workflowId);
}
