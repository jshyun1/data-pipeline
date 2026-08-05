package com.company.pipeline.jobcatalog;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobRepository extends JpaRepository<EtlJob, Long> {

    Optional<EtlJob> findByNifiPgId(String nifiPgId);

    List<EtlJob> findByDeletedAtIsNullOrderByJobNameAsc();

    long countByDeletedAtIsNull();
}
