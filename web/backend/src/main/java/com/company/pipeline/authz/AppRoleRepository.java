package com.company.pipeline.authz;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRoleRepository extends JpaRepository<AppRole, String> {
    List<AppRole> findAllByOrderByRoleIdAsc();
}
