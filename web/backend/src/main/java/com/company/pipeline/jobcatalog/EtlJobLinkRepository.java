package com.company.pipeline.jobcatalog;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobLinkRepository extends JpaRepository<EtlJobLink, Long> {

    List<EtlJobLink> findByJobIdAndDeletedAtIsNull(Long jobId);
}
