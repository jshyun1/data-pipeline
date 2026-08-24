package com.company.pipeline.authz;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PermissionAuditLogRepository extends JpaRepository<PermissionAuditLog, Long> {

    @Modifying
    @Query("DELETE FROM PermissionAuditLog p WHERE p.occurredAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") LocalDateTime cutoff);

    /** 기간 내 감사 로그를 최신순으로. 세부 필터(동작/검색어)는 프런트에서 처리한다(다른 로그 화면과 동일). */
    List<PermissionAuditLog> findByOccurredAtBetweenOrderByOccurredAtDesc(
            LocalDateTime from, LocalDateTime to, Pageable pageable);
}
