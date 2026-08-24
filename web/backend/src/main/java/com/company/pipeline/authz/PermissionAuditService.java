package com.company.pipeline.authz;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

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
        // 호출부가 IP 를 안 넘겼으면(관리/역할/동기화 등) 현재 요청에서 직접 알아낸다 - 한 곳에서
        // 채워 두면 대부분의 사용자 액션에 작업 PC IP 가 남는다.
        log.setClientIp(StringUtils.hasText(clientIp) ? clientIp : currentRequestIp());
        repository.save(log);
    }

    public void record(String actorId, String action, String targetType, String targetId, String detail) {
        record(actorId, action, targetType, targetId, null, null, detail, null);
    }

    /** 현재 HTTP 요청의 클라이언트 IP. 프록시 헤더(X-Forwarded-For→X-Real-IP)를 우선, 없으면 소켓 주소. */
    private String currentRequestIp() {
        if (!(RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attrs)) {
            return null;   // 요청 컨텍스트 밖(스케줄러 등) - IP 없음
        }
        HttpServletRequest request = attrs.getRequest();
        String forwarded = request.getHeader("X-Forwarded-For");
        if (StringUtils.hasText(forwarded)) {
            return forwarded.split(",")[0].trim();   // 체인 맨 앞 = 원 클라이언트
        }
        String realIp = request.getHeader("X-Real-IP");
        if (StringUtils.hasText(realIp)) {
            return realIp.trim();
        }
        return request.getRemoteAddr();
    }

    private String clip(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() > max ? value.substring(0, max) : value;
    }
}
