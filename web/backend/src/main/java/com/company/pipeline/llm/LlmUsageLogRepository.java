package com.company.pipeline.llm;

import org.springframework.data.jpa.repository.JpaRepository;

public interface LlmUsageLogRepository extends JpaRepository<LlmUsageLog, Long> {
}
