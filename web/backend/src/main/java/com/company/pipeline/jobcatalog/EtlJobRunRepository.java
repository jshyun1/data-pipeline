package com.company.pipeline.jobcatalog;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobRunRepository extends JpaRepository<EtlJobRun, Long> {

    Optional<EtlJobRun> findByJobIdAndEndedAtIsNull(Long jobId);

    List<EtlJobRun> findByEndedAtIsNull();

    List<EtlJobRun> findTop20ByJobIdOrderByStartedAtDesc(Long jobId);
}
