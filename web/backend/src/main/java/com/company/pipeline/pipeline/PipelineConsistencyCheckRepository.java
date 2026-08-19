package com.company.pipeline.pipeline;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineConsistencyCheckRepository extends JpaRepository<PipelineConsistencyCheck, Long> {
    List<PipelineConsistencyCheck> findTop20ByPipelineIdOrderByCheckedAtDesc(Long pipelineId);
}
