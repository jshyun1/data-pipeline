package com.company.pipeline.connection;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ConnectionRepository extends JpaRepository<PipelineConnection, Long> {
}
