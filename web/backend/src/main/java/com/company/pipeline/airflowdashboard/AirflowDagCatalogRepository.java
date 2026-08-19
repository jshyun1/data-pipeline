package com.company.pipeline.airflowdashboard;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AirflowDagCatalogRepository extends JpaRepository<AirflowDagCatalog, Long> {

    List<AirflowDagCatalog> findByEnabledTrueOrderByBusinessGroupAscBusinessFolderAscSortOrderAscDisplayNameAsc();

    List<AirflowDagCatalog> findByEnabledTrueAndMonitoringEnabledTrue();

    Optional<AirflowDagCatalog> findByDagId(String dagId);
}
