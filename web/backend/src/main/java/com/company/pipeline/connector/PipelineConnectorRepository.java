package com.company.pipeline.connector;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineConnectorRepository extends JpaRepository<PipelineConnector, Long> {

    List<PipelineConnector> findByPipelineId(Long pipelineId);

    Optional<PipelineConnector> findByPipelineIdAndConnectorRole(Long pipelineId, String connectorRole);
}
