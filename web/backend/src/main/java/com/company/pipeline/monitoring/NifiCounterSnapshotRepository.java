package com.company.pipeline.monitoring;

import org.springframework.data.jpa.repository.JpaRepository;

public interface NifiCounterSnapshotRepository extends JpaRepository<NifiCounterSnapshot, String> {
}
