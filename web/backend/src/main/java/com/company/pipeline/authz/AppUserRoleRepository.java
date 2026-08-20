package com.company.pipeline.authz;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppUserRoleRepository extends JpaRepository<AppUserRole, AppUserRoleId> {
    List<AppUserRole> findByUserId(String userId);

    List<AppUserRole> findByRoleId(String roleId);

    List<AppUserRole> findByUserIdIn(Collection<String> userIds);

    long countByRoleId(String roleId);

    void deleteByUserId(String userId);
}
