package com.company.pipeline.jobcatalog;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobStepRepository extends JpaRepository<EtlJobStep, Long> {

    Optional<EtlJobStep> findByNifiProcessorId(String nifiProcessorId);

    List<EtlJobStep> findByNifiProcessorIdIn(List<String> nifiProcessorIds);

    List<EtlJobStep> findByJobIdAndDeletedAtIsNull(Long jobId);

    List<EtlJobStep> findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(Long jobId);

    List<EtlJobStep> findByDbcpServiceIdAndDeletedAtIsNull(String dbcpServiceId);
}
