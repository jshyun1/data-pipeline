package com.company.pipeline.authz;

import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 감사 로그 보존정책(설계서 §7.8 현행화). 로그인/거부 이벤트는 양이 많아 방치하면 무한 증가하므로
 * 보존 기간을 넘긴 행을 하루 한 번 정리한다. 보존 일수는 설정으로 조정하고, 0 이하면 무제한(정리 안 함).
 */
@Service
public class PermissionAuditRetentionService {

    private static final Logger log = LoggerFactory.getLogger(PermissionAuditRetentionService.class);

    private final PermissionAuditLogRepository repository;

    @Value("${authz.audit.retention-days:90}")
    private int retentionDays;

    public PermissionAuditRetentionService(PermissionAuditLogRepository repository) {
        this.repository = repository;
    }

    @Scheduled(cron = "${authz.audit.retention-cron:0 30 3 * * *}")
    @Transactional
    public void purgeExpired() {
        if (retentionDays <= 0) {
            return;   // 무제한 보존
        }
        LocalDateTime cutoff = LocalDateTime.now().minusDays(retentionDays);
        int deleted = repository.deleteOlderThan(cutoff);
        if (deleted > 0) {
            log.info("권한 감사 로그 {}건 정리(보존 {}일 초과)", deleted, retentionDays);
        }
    }
}
