package com.company.pipeline.authz;

import java.time.LocalDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PermissionAuditLogRepository extends JpaRepository<PermissionAuditLog, Long> {

    @Modifying
    @Query("DELETE FROM PermissionAuditLog p WHERE p.occurredAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") LocalDateTime cutoff);
}
