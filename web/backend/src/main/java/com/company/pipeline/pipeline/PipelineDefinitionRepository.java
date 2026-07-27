package com.company.pipeline.pipeline;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineDefinitionRepository extends JpaRepository<PipelineDefinition, Long> {

    List<PipelineDefinition> findByStatus(PipelineStatus status);

    List<PipelineDefinition> findByStatusIn(List<PipelineStatus> statuses);

    List<PipelineDefinition> findBySourceConnectionIdOrTargetConnectionId(
            Long sourceConnectionId, Long targetConnectionId);
}
