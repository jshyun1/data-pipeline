package com.company.pipeline.authz;

import java.io.Serializable;
import java.util.Objects;

/** {@link AppUserRole} 복합키(user_id, role_id). */
public class AppUserRoleId implements Serializable {

    private String userId;
    private String roleId;

    public AppUserRoleId() {
    }

    public AppUserRoleId(String userId, String roleId) {
        this.userId = userId;
        this.roleId = roleId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppUserRoleId that)) {
            return false;
        }
        return Objects.equals(userId, that.userId) && Objects.equals(roleId, that.roleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, roleId);
    }
}
