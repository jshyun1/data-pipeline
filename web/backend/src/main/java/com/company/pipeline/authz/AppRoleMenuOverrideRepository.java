package com.company.pipeline.authz;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRoleMenuOverrideRepository
        extends JpaRepository<AppRoleMenuOverride, AppRoleMenuOverrideId> {

    List<AppRoleMenuOverride> findByRoleIdIn(Collection<String> roleIds);

    List<AppRoleMenuOverride> findByRoleId(String roleId);
}
