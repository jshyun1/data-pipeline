package com.company.pipeline.jobcatalog;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobLinkRepository extends JpaRepository<EtlJobLink, Long> {

    Optional<EtlJobLink> findByNifiConnectionId(String nifiConnectionId);

    List<EtlJobLink> findByJobIdAndDeletedAtIsNull(Long jobId);
}
