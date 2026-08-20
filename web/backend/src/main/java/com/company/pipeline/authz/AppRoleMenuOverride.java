package com.company.pipeline.authz;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 역할별 메뉴 노출 재정의. V47(app_role_menu_override) 매핑. 비워두면 시스템 권한에서
 * 자동 계산된다(설계서 §4.5). "권한은 주되 메뉴는 감추고 싶다" 같은 예외에만 행이 생긴다.
 */
@Entity
@Table(name = "app_role_menu_override")
@IdClass(AppRoleMenuOverrideId.class)
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AppRoleMenuOverride {

    @Id
    @Column(name = "role_id", length = 40)
    private String roleId;

    @Id
    @Column(name = "menu_id", length = 40)
    private String menuId;

    @Column(name = "visible", nullable = false)
    private boolean visible;

    public AppRoleMenuOverride(String roleId, String menuId, boolean visible) {
        this.roleId = roleId;
        this.menuId = menuId;
        this.visible = visible;
    }
}
