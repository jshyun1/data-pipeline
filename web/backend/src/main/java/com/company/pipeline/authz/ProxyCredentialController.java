package com.company.pipeline.authz;

import com.company.pipeline.user.security.PipelineJwtService;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * NiFi/Airflow 콘솔(iframe)의 per-user 프록시 인증을 위한 nginx {@code auth_request} 종단점(설계서
 * §7.7, P5b). nginx 서브요청이 브라우저 쿠키를 실어 이걸 부르면, 사용자를 식별하고 해당 시스템
 * 접근 권한을 확인해서:
 * <ul>
 *   <li>허용 → 200 + 응답헤더 {@code X-Proxied-Entity: <userId>} (nginx가 NiFi로 갈 때
 *       {@code X-ProxiedEntitiesChain: <userId>} 로 감싼다 - 스파이크에서 실증한 신원 치환)</li>
 *   <li>미인증 → 401 / 권한 없음 → 403 (nginx가 요청 자체를 차단)</li>
 * </ul>
 *
 * <p>콘솔 "진입"은 READ 로 판정한다(NiFi/Airflow 가 view vs modify 를 자체 정책으로 세밀 강제).
 * 이 종단점은 어느 플래그와 무관하게 항상 정확히 판정한다 - 실제 per-user 전환 여부는 nginx
 * 설정과 로그인 세션쿠키(authz.proxy.enabled)가 좌우한다.
 */
@RestController
@RequestMapping("/api/authz")
public class ProxyCredentialController {

    private final PipelineJwtService jwtService;
    private final PermissionService permissionService;
    private final com.company.pipeline.authz.provisioning.AirflowUserSyncService airflowSync;

    @Value("${authz.proxy.session-cookie:cetl_session}")
    private String sessionCookie;

    public ProxyCredentialController(PipelineJwtService jwtService, PermissionService permissionService,
                                     com.company.pipeline.authz.provisioning.AirflowUserSyncService airflowSync) {
        this.jwtService = jwtService;
        this.permissionService = permissionService;
        this.airflowSync = airflowSync;
    }

    @GetMapping("/proxy-credential")
    public ResponseEntity<Void> proxyCredential(
            @RequestParam("system") String systemParam,
            @RequestHeader(value = "X-Original-Method", required = false) String originalMethod,
            HttpServletRequest request) {

        SystemCode system = parseSystem(systemParam);
        if (system == null) {
            return ResponseEntity.badRequest().build();
        }
        String userId = resolveUser(request);
        if (userId == null) {
            return ResponseEntity.status(401).build();
        }
        // 콘솔 진입 = READ. 세부(수정/실행)는 NiFi/Airflow 자체 정책이 최종 강제한다.
        if (!permissionService.check(userId, system, AccessBits.READ)) {
            return ResponseEntity.status(403).build();
        }
        if (system == SystemCode.AIRFLOW) {
            // Airflow 는 대행헤더가 아니라 사용자별 세션(_token 쿠키)로 동작한다.
            String token = airflowSync.userSessionToken(userId);
            if (token == null) {
                return ResponseEntity.status(502).build();   // 세션 발급 실패
            }
            return ResponseEntity.ok().header("X-Airflow-Token", token).build();
        }
        // NiFi: 대행 신원을 돌려주면 nginx 가 X-ProxiedEntitiesChain 으로 감싼다.
        return ResponseEntity.ok().header("X-Proxied-Entity", userId).build();
    }

    private String resolveUser(HttpServletRequest request) {
        // 1) iframe 요청은 Authorization 헤더를 못 실으므로 세션쿠키(JWT)로 식별한다.
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (sessionCookie.equals(cookie.getName())) {
                    String sub = trySubject(cookie.getValue());
                    if (sub != null) {
                        return sub;
                    }
                }
            }
        }
        // 2) Authorization Bearer(있으면) 폴백.
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return trySubject(header.substring(7));
        }
        return null;
    }

    private String trySubject(String token) {
        try {
            return jwtService.parse(token).getSubject();
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private SystemCode parseSystem(String value) {
        try {
            return SystemCode.valueOf(value);
        } catch (IllegalArgumentException | NullPointerException ex) {
            return null;
        }
    }
}
