package com.company.pipeline.authz;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 역할 → 시스템 권한. V47(app_role_system_permission) 매핑. access_bits 는 1=READ/2=WRITE/
 * 4=EXECUTE 비트마스크(설계서 §4.3), 2단계 UI에서는 0/1/7 만 저장된다.
 */
@Entity
@Table(name = "app_role_system_permission")
@IdClass(AppRoleSystemPermissionId.class)
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AppRoleSystemPermission {

    @Id
    @Column(name = "role_id", length = 40)
    private String roleId;

    @Id
    @Column(name = "system_code", length = 20)
    private String systemCode;

    @Column(name = "access_bits", nullable = false)
    private int accessBits = 0;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "updated_by", length = 100)
    private String updatedBy;

    public AppRoleSystemPermission(String roleId, String systemCode, int accessBits, String updatedBy) {
        this.roleId = roleId;
        this.systemCode = systemCode;
        this.accessBits = accessBits;
        this.updatedBy = updatedBy;
    }

    @PrePersist
    @PreUpdate
    void touch() {
        updatedAt = LocalDateTime.now();
    }
}
