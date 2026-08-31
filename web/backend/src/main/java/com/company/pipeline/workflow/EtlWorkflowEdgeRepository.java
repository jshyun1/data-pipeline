package com.company.pipeline.workflow;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlWorkflowEdgeRepository extends JpaRepository<EtlWorkflowEdge, Long> {

    List<EtlWorkflowEdge> findByWorkflowId(Long workflowId);

    void deleteByWorkflowId(Long workflowId);
}
