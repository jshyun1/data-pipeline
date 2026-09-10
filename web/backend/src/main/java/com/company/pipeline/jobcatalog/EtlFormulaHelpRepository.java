package com.company.pipeline.jobcatalog;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlFormulaHelpRepository extends JpaRepository<EtlFormulaHelp, Long> {

    List<EtlFormulaHelp> findByEnabledTrueOrderBySortOrderAscFunctionNameAsc();
}
