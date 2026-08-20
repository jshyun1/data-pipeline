package com.company.pipeline.authz;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 권한 감사 로그 기록(설계서 §7.8). "언제 누가 누구에게 무슨 권한을 줬나"를 남긴다.
 * 실패해도 본 작업을 막지 않도록 호출부는 필요 시 별도 트랜잭션에서 부른다.
 */
@Service
public class PermissionAuditService {

    private final PermissionAuditLogRepository repository;

    public PermissionAuditService(PermissionAuditLogRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void record(String actorId, String action, String targetType, String targetId,
                       String beforeValue, String afterValue, String detail, String clientIp) {
        PermissionAuditLog log = new PermissionAuditLog();
        log.setActorId(actorId == null || actorId.isBlank() ? "system" : actorId);
        log.setAction(action);
        log.setTargetType(targetType);
        log.setTargetId(clip(targetId, 255));
        log.setBeforeValue(clip(beforeValue, 200));
        log.setAfterValue(clip(afterValue, 200));
        log.setDetail(detail);
        log.setClientIp(clientIp);
        repository.save(log);
    }

    public void record(String actorId, String action, String targetType, String targetId, String detail) {
        record(actorId, action, targetType, targetId, null, null, detail, null);
    }

    private String clip(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() > max ? value.substring(0, max) : value;
    }
}
