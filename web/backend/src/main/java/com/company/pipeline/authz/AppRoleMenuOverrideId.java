package com.company.pipeline.authz;

import java.io.Serializable;
import java.util.Objects;

/** {@link AppRoleMenuOverride} 복합키(role_id, menu_id). */
public class AppRoleMenuOverrideId implements Serializable {

    private String roleId;
    private String menuId;

    public AppRoleMenuOverrideId() {
    }

    public AppRoleMenuOverrideId(String roleId, String menuId) {
        this.roleId = roleId;
        this.menuId = menuId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppRoleMenuOverrideId that)) {
            return false;
        }
        return Objects.equals(roleId, that.roleId) && Objects.equals(menuId, that.menuId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleId, menuId);
    }
}
