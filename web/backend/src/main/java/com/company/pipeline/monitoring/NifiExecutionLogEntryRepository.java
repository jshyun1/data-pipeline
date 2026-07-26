package com.company.pipeline.monitoring;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NifiExecutionLogEntryRepository extends JpaRepository<NifiExecutionLogEntry, Long> {

    List<NifiExecutionLogEntry> findByOccurredAtBetweenOrderByOccurredAtDesc(LocalDateTime from, LocalDateTime to);
}
