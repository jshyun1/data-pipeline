package com.company.pipeline.logpipeline;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LogPipelineSourceRepository extends JpaRepository<LogPipelineSource, Long> {

    Optional<LogPipelineSource> findByPipelineId(Long pipelineId);
}
