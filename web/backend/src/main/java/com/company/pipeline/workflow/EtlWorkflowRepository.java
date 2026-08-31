package com.company.pipeline.workflow;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlWorkflowRepository extends JpaRepository<EtlWorkflow, Long> {

    List<EtlWorkflow> findByDeletedAtIsNullOrderByNameAsc();

    Optional<EtlWorkflow> findByIdAndDeletedAtIsNull(Long id);

    Optional<EtlWorkflow> findByWorkflowKeyAndDeletedAtIsNull(String workflowKey);

    /** 팩토리가 읽을 게시본 목록(= 현재 살아있는 DAG 집합). */
    List<EtlWorkflow> findByDeletedAtIsNullAndPublishedAtIsNotNull();
}
