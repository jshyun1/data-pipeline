package com.company.pipeline.jobcatalog;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobParamRepository extends JpaRepository<EtlJobParam, Long> {

    List<EtlJobParam> findByJobIdAndDeletedAtIsNull(Long jobId);
}
