package com.company.pipeline.authz;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.user.AppUser;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * 감사 기록은 인가 강제 스위치와 무관하게 항상 남아야 한다.
 *
 * <p>인가(막을지 말지)와 감사(누가 뭘 했는지)는 목적이 다르다. MSA 포털 연동이 끝날 때까지
 * 서버는 authz.enforcement.enabled 를 켤 수 없는데, 바로 그 기간이 무인증 호출이 그대로
 * 통과하는 때라 감사가 가장 필요하다. 예전엔 둘이 한 스위치에 묶여 있어서 그 기간의
 * API 액션이 통째로 기록되지 않았다(2026-08-25 발견).
 */
@ExtendWith(MockitoExtension.class)
class PermissionAspectAuditTest {

    @Mock
    private PermissionService permissionService;
    @Mock
    private PermissionAuditService auditService;
    @Mock
    private JoinPoint joinPoint;
    @Mock
    private Signature signature;

    @RequirePermission(system = SystemCode.NIFI)
    private static final class AuditedController {
    }

    @RequirePermission(system = SystemCode.NIFI, audit = false)
    private static final class SilentController {
    }

    private static RequirePermission annotationOf(Class<?> type) {
        return type.getAnnotation(RequirePermission.class);
    }

    @AfterEach
    void clearRequest() {
        RequestContextHolder.resetRequestAttributes();
    }

    private PermissionAspect aspect() {
        lenient().when(joinPoint.getSignature()).thenReturn(signature);
        lenient().when(signature.toShortString()).thenReturn("EtlJobController.create(..)");
        // enforcementEnabled / serviceToken 은 @Value 주입 필드라 테스트에서는 기본값
        // (false / "") 그대로 둔다 - 그 상태에서도 감사가 남는지가 이 테스트의 핵심이다.
        return new PermissionAspect(permissionService, auditService);
    }

    private void bindRequest(String method, String uri) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        request.setRemoteAddr("192.168.101.40");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    @Test
    void 인가가_꺼져_있어도_변경요청은_감사에_남는다() {
        bindRequest("POST", "/api/etl/jobs");

        aspect().auditClass(joinPoint, annotationOf(AuditedController.class));

        ArgumentCaptor<String> action = ArgumentCaptor.forClass(String.class);
        verify(auditService).record(eq("unknown"), action.capture(), eq("API"), eq("/api/etl/jobs"),
                any(), any(), any(), any());
        org.assertj.core.api.Assertions.assertThat(action.getValue()).isEqualTo("NIFI_CREATE");
    }

    @Test
    void 조회는_감사하지_않는다() {
        bindRequest("GET", "/api/etl/jobs");

        aspect().auditClass(joinPoint, annotationOf(AuditedController.class));

        verify(auditService, never()).record(any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void audit_false_로_꺼둔_컨트롤러는_자동감사하지_않는다() {
        bindRequest("POST", "/api/admin/users");

        aspect().auditClass(joinPoint, annotationOf(SilentController.class));

        verify(auditService, never()).record(any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void 로그인한_사용자는_actor_로_기록된다() {
        bindRequest("DELETE", "/api/etl/jobs/7");
        AppUser user = new AppUser("cktnqhd15");
        org.springframework.security.core.context.SecurityContextHolder.getContext().setAuthentication(
                new org.springframework.security.authentication.UsernamePasswordAuthenticationToken(
                        user, null, java.util.List.of()));
        try {
            aspect().auditClass(joinPoint, annotationOf(AuditedController.class));

            verify(auditService).record(eq("cktnqhd15"), eq("NIFI_DELETE"), eq("API"), eq("/api/etl/jobs/7"),
                    any(), any(), any(), any());
        } finally {
            org.springframework.security.core.context.SecurityContextHolder.clearContext();
        }
    }

    @Test
    void heartbeat_류_고빈도_엔드포인트는_감사_소음이라_제외한다() {
        bindRequest("POST", "/api/health/heartbeat");

        aspect().auditClass(joinPoint, annotationOf(AuditedController.class));

        verify(auditService, never()).record(any(), any(), any(), any(), any(), any(), any(), any());
    }
}
