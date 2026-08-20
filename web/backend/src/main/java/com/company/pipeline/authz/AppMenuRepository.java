package com.company.pipeline.authz;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppMenuRepository extends JpaRepository<AppMenu, String> {
    List<AppMenu> findByUseYnOrderBySortOrdAsc(String useYn);
}
