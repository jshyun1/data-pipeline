package com.company.pipeline.authz;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 권한 감사 로그. V47(permission_audit_log) 매핑. "언제 누가 누구에게 무슨 권한을 줬나"와
 * 로그인/거부를 남긴다(설계서 §7.8). 서비스 호출은 actor_id='svc:airflow'/'svc:portal'.
 */
@Entity
@Table(name = "permission_audit_log")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PermissionAuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private LocalDateTime occurredAt;

    @Column(name = "actor_id", nullable = false, length = 255)
    private String actorId;

    @Column(name = "action", nullable = false, length = 40)
    private String action;

    @Column(name = "target_type", length = 20)
    private String targetType;

    @Column(name = "target_id", length = 255)
    private String targetId;

    @Column(name = "before_value", length = 200)
    private String beforeValue;

    @Column(name = "after_value", length = 200)
    private String afterValue;

    @Column(name = "detail")
    private String detail;

    @Column(name = "client_ip", length = 45)
    private String clientIp;

    @PrePersist
    void onCreate() {
        if (occurredAt == null) {
            occurredAt = LocalDateTime.now();
        }
    }
}
