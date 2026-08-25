package com.company.pipeline.authz;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import jakarta.servlet.http.HttpServletRequest;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * {@link RequirePermission} 강제(설계서 §7.1, P4). {@code authz.enforcement.enabled=true} 일 때만
 * 동작하고, 기본(off)에서는 아무 것도 하지 않는다 - 그래서 이 코드가 배포돼 있어도 스위치를
 * 켜기 전까지는 기존 permitAll 동작이 그대로다(MSA 서버 공유 pipeline-api 보호).
 *
 * <p>권한 계산:
 * <ul>
 *   <li><b>메서드 레벨</b> 애노테이션: 명시한 {@code bits} 를 그대로 요구.</li>
 *   <li><b>클래스 레벨</b> 애노테이션: HTTP 동사로 결정 - 조회(GET/HEAD)는 READ, 변경(POST/PUT/
 *       DELETE/PATCH)은 WRITE. 컨트롤러 하나에 애노테이션 하나로 읽기/쓰기가 정확히 갈린다.</li>
 *   <li>{@code COMMON} 은 쓰기 계층이 없다(설계서 §4.3) - 항상 READ 로 취급.</li>
 * </ul>
 *
 * <p>서비스 토큰(X-Service-Token)이 일치하면 사람 신원 없이도 통과시킨다(설계서 §7.6, Airflow DAG).
 */
@Aspect
@Component
public class PermissionAspect {

    private static final Logger log = LoggerFactory.getLogger(PermissionAspect.class);

    // URL 끝 세그먼트가 이 동작 동사면 액션명에 그대로 쓴다(세분화). 예: .../{id}/deploy → KAFKA_DEPLOY.
    private static final java.util.Set<String> ACTION_SEGMENTS = java.util.Set.of(
            "deploy", "start", "stop", "pause", "restart", "resume", "run", "trigger",
            "schedule", "sync", "approve", "reject", "unlock", "enable", "disable",
            "retry", "replay", "cancel", "rollback", "dismiss-drift", "sync-identities");

    // 고빈도·무의미 엔드포인트(편집잠금 heartbeat, 수동 스냅샷 등)는 감사 소음이라 제외한다.
    private static final java.util.Set<String> SKIP_SEGMENTS = java.util.Set.of(
            "heartbeat", "snapshot");

    private final PermissionService permissionService;
    private final PermissionAuditService auditService;

    @Value("${authz.enforcement.enabled:false}")
    private boolean enforcementEnabled;

    @Value("${authz.service-token:}")
    private String serviceToken;

    public PermissionAspect(PermissionService permissionService, PermissionAuditService auditService) {
        this.permissionService = permissionService;
        this.auditService = auditService;
    }

    /** 메서드 레벨 애노테이션(클래스 기본값을 오버라이드) - 명시 비트를 요구. */
    @Before("@annotation(require)")
    public void enforceMethod(JoinPoint joinPoint, RequirePermission require) {
        enforce(joinPoint, require, false);
    }

    /** 클래스 레벨 애노테이션(기본값) - 메서드 자체 애노테이션이 있으면 그쪽이 우선한다. */
    @Before("@within(require) && !@annotation(com.company.pipeline.authz.RequirePermission)")
    public void enforceClass(JoinPoint joinPoint, RequirePermission require) {
        enforce(joinPoint, require, true);
    }

    private void enforce(JoinPoint joinPoint, RequirePermission require, boolean classLevel) {
        if (!enforcementEnabled) {
            return;   // 스위치 off: 순수 선언, 강제 안 함(기본값)
        }

        HttpServletRequest request = currentRequest();

        if (StringUtils.hasText(serviceToken) && request != null
                && serviceToken.equals(request.getHeader("X-Service-Token"))) {
            return;   // 서비스 토큰 우회(DAG 등 비-사람 호출자)
        }

        int required;
        if (classLevel) {
            required = isMutating(request) ? AccessBits.WRITE : AccessBits.READ;
        } else {
            required = require.bits();
        }
        if (require.system() == SystemCode.COMMON) {
            required = AccessBits.READ;   // COMMON 은 쓰기 계층 없음
        }

        AppUser principal = currentPrincipal();
        if (principal == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED);
        }
        if (!permissionService.check(principal.getUserId(), require.system(), required)) {
            String target = joinPoint.getSignature().toShortString();
            try {
                auditService.record(principal.getUserId(), "DENIED", "API", target,
                        null, null, require.system() + " bits=" + required,
                        request == null ? null : clientIp(request));
            } catch (RuntimeException ex) {
                log.debug("DENIED 감사 기록 실패: {}", ex.getMessage());
            }
            throw new BusinessException(ErrorCode.FORBIDDEN);
        }
    }

    // ----- 성공한 변경요청 자동 감사 (설계서 §7.8 확장) ------------------------
    // 인가를 통과해 정상 반환된 POST/PUT/PATCH/DELETE 를 감사 로그에 남긴다. ETL/CDC/Airflow 등
    // 운영 컨트롤러의 생성·수정·삭제가 별도 코드 없이 전부 기록된다("그 외 모든 작업"). 계정/역할처럼
    // 자체 상세 감사를 하는 컨트롤러는 @RequirePermission(audit=false) 로 제외한다.

    @AfterReturning("@annotation(require)")
    public void auditMethod(JoinPoint joinPoint, RequirePermission require) {
        autoAudit(joinPoint, require);
    }

    @AfterReturning("@within(require) && !@annotation(com.company.pipeline.authz.RequirePermission)")
    public void auditClass(JoinPoint joinPoint, RequirePermission require) {
        autoAudit(joinPoint, require);
    }

    private void autoAudit(JoinPoint joinPoint, RequirePermission require) {
        // 인가 강제(enforce)와 달리 감사는 스위치와 무관하게 항상 남긴다.
        //
        // 예전엔 enforcementEnabled 를 같이 봤는데, 그러면 "막지는 않지만 누가 뭘 했는지는
        // 남겨야 하는" 기간에 감사가 통째로 비었다. MSA 포털 연동이 끝날 때까지 서버는
        // 인가를 못 켜는데, 바로 그 기간이 무인증 호출이 그대로 통과하는 때라 감사가 가장
        // 필요하다(2026-08-25: ETL Job 생성이 감사 로그에 남지 않는 것을 발견).
        //
        // 소음은 아래에서 걸러진다 - 조회(GET/HEAD)·서비스 토큰 호출(DAG)·heartbeat 류 제외.
        // 인증 안 된 호출은 actor="unknown" 으로 남아, 오히려 무인증 접근을 드러낸다.
        if (!require.audit()) {
            return;
        }
        HttpServletRequest request = currentRequest();
        if (request == null) {
            return;
        }
        String verb = mutationVerb(request.getMethod());
        if (verb == null) {
            return;   // 조회(GET/HEAD/OPTIONS)는 감사하지 않는다
        }
        // 서비스 토큰(비-사람 자동 호출: DAG 등)은 감사 소음이라 제외한다.
        if (StringUtils.hasText(serviceToken) && serviceToken.equals(request.getHeader("X-Service-Token"))) {
            return;
        }
        String lastSegment = lastSegment(request.getRequestURI());
        if (SKIP_SEGMENTS.contains(lastSegment)) {
            return;   // heartbeat 등 고빈도·무의미 엔드포인트는 감사 소음이라 제외
        }
        AppUser principal = currentPrincipal();
        String actor = principal == null ? "unknown" : principal.getUserId();
        // 세분화 액션명: URL 끝이 동작 동사면 그걸 액션으로(KAFKA_DEPLOY·AIRFLOW_RUN·AIRFLOW_SCHEDULE …),
        // 아니면 HTTP 동사 기반(KAFKA_CREATE·NIFI_DELETE …).
        String suffix = ACTION_SEGMENTS.contains(lastSegment)
                ? lastSegment.toUpperCase().replace('-', '_')
                : verb;
        String action = require.system().name() + "_" + suffix;
        try {
            auditService.record(actor, action, "API", request.getRequestURI(),
                    null, null, joinPoint.getSignature().toShortString(), clientIp(request));
        } catch (RuntimeException ex) {
            log.debug("자동 감사 기록 실패: {}", ex.getMessage());
        }
    }

    /** URI 의 마지막 경로 세그먼트(소문자). 쿼리스트링은 무시. */
    private String lastSegment(String uri) {
        if (uri == null || uri.isBlank()) {
            return "";
        }
        String path = uri;
        int q = path.indexOf('?');
        if (q >= 0) {
            path = path.substring(0, q);
        }
        while (path.endsWith("/") && path.length() > 1) {
            path = path.substring(0, path.length() - 1);
        }
        int slash = path.lastIndexOf('/');
        return (slash >= 0 ? path.substring(slash + 1) : path).toLowerCase();
    }

    /** 변경 동사(감사 액션 접미사). 조회면 null. */
    private String mutationVerb(String method) {
        if ("POST".equalsIgnoreCase(method)) {
            return "CREATE";
        }
        if ("PUT".equalsIgnoreCase(method) || "PATCH".equalsIgnoreCase(method)) {
            return "UPDATE";
        }
        if ("DELETE".equalsIgnoreCase(method)) {
            return "DELETE";
        }
        return null;
    }

    private boolean isMutating(HttpServletRequest request) {
        if (request == null) {
            return true;   // 요청 컨텍스트가 없으면 안전측으로 쓰기로 간주
        }
        String method = request.getMethod();
        return !("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)
                || "OPTIONS".equalsIgnoreCase(method));
    }

    private AppUser currentPrincipal() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof AppUser user) {
            return user;
        }
        return null;
    }

    private HttpServletRequest currentRequest() {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attrs) {
            return attrs.getRequest();
        }
        return null;
    }

    private String clientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (StringUtils.hasText(forwarded)) {
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
