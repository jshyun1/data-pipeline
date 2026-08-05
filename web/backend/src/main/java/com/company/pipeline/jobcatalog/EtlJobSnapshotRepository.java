package com.company.pipeline.jobcatalog;

import java.time.LocalDateTime;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EtlJobSnapshotRepository extends JpaRepository<EtlJobSnapshot, Long> {

    Optional<EtlJobSnapshot> findFirstByJobIdOrderByCapturedAtDesc(Long jobId);

    @Modifying
    @Query("delete from EtlJobSnapshot s where s.capturedAt < :threshold")
    int deleteOlderThan(@Param("threshold") LocalDateTime threshold);
}
