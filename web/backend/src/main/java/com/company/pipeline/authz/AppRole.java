package com.company.pipeline.authz;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 역할. V47(app_role) 매핑. {@code built_in} 역할은 마이그레이션이 심은 것으로 삭제/개명을
 * 막는다(운영 중 지워지면 전원이 권한을 잃는다).
 */
@Entity
@Table(name = "app_role")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AppRole {

    @Id
    @Column(name = "role_id", length = 40)
    private String roleId;

    @Column(name = "role_nm", nullable = false, length = 100)
    private String roleNm;

    @Column(name = "role_desc", length = 300)
    private String roleDesc;

    @Column(name = "built_in", nullable = false)
    private boolean builtIn = false;

    @Column(name = "use_yn", nullable = false, length = 1)
    private String useYn = "Y";

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "created_by", length = 100)
    private String createdBy;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "updated_by", length = 100)
    private String updatedBy;

    public AppRole(String roleId, String roleNm, String roleDesc, String createdBy) {
        this.roleId = roleId;
        this.roleNm = roleNm;
        this.roleDesc = roleDesc;
        this.createdBy = createdBy;
        this.updatedBy = createdBy;
    }

    @PrePersist
    void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
