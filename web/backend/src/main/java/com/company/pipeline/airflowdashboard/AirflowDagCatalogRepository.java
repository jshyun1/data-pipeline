package com.company.pipeline.airflowdashboard;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AirflowDagCatalogRepository extends JpaRepository<AirflowDagCatalog, Long> {

    List<AirflowDagCatalog> findByEnabledTrueOrderByBusinessGroupAscBusinessFolderAscSortOrderAscDisplayNameAsc();
}
