package com.company.pipeline.workflow;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlWorkflowRepository extends JpaRepository<EtlWorkflow, Long> {

    List<EtlWorkflow> findByDeletedAtIsNullOrderByNameAsc();

    Optional<EtlWorkflow> findByIdAndDeletedAtIsNull(Long id);

    Optional<EtlWorkflow> findByWorkflowKeyAndDeletedAtIsNull(String workflowKey);

    /**
     * 지운 워크플로우까지 포함해 이 키를 쓴 적이 있는가. 키는 한 번 쓰면 다시 쓰지 않는다 - dag_id(etl_wf_{키})·
     * 실행 원장(etl_job_run.workflow_key)·알림 판정이 키로 묶여서, 재사용하면 지운 워크플로우의 실행 이력이 새 워크플로우에 이어 붙는다.
     */
    boolean existsByWorkflowKey(String workflowKey);

    /** 팩토리가 읽을 게시본 목록(= 현재 살아있는 DAG 집합). */
    List<EtlWorkflow> findByDeletedAtIsNullAndPublishedAtIsNotNull();
}
