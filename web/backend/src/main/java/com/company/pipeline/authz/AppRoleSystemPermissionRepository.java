package com.company.pipeline.authz;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRoleSystemPermissionRepository
        extends JpaRepository<AppRoleSystemPermission, AppRoleSystemPermissionId> {

    List<AppRoleSystemPermission> findByRoleId(String roleId);

    List<AppRoleSystemPermission> findByRoleIdIn(Collection<String> roleIds);
}
