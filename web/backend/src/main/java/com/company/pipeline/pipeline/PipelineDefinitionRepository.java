package com.company.pipeline.pipeline;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineDefinitionRepository extends JpaRepository<PipelineDefinition, Long> {

    boolean existsByName(String name);

    /** 그룹 삭제 전에 «비어 있는가»를 확인할 때 쓴다. */
    long countByGroupId(Long groupId);

    List<PipelineDefinition> findByStatus(PipelineStatus status);

    List<PipelineDefinition> findByStatusIn(List<PipelineStatus> statuses);

    List<PipelineDefinition> findBySourceConnectionIdOrTargetConnectionId(
            Long sourceConnectionId, Long targetConnectionId);
}
