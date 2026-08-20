package com.company.pipeline.authz;

import java.io.Serializable;
import java.util.Objects;

/** {@link AppRoleSystemPermission} 복합키(role_id, system_code). */
public class AppRoleSystemPermissionId implements Serializable {

    private String roleId;
    private String systemCode;

    public AppRoleSystemPermissionId() {
    }

    public AppRoleSystemPermissionId(String roleId, String systemCode) {
        this.roleId = roleId;
        this.systemCode = systemCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppRoleSystemPermissionId that)) {
            return false;
        }
        return Objects.equals(roleId, that.roleId) && Objects.equals(systemCode, that.systemCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleId, systemCode);
    }
}
