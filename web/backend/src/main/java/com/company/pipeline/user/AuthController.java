package com.company.pipeline.user;

import com.company.pipeline.authz.PermissionAuditService;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.LoginRequest;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.UserResponse;
import com.company.pipeline.user.security.PipelineJwtService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.OffsetDateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * cerebroetl-ui 자체 로그인. 인증원은 authz.provider 에 따라 결정된다(U3, C9):
 * LOCAL=app_user(bcrypt) / EXTERNAL=사내 ST_USER. 토큰은 어느 쪽이든 이 앱이 직접 발급한다.
 * 로그인 성공/실패/잠금은 감사 로그에 남긴다(설계서 §7.8).
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final UserService userService;
    private final CredentialAuthenticator authenticator;
    private final PipelineJwtService jwtService;
    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final PermissionAuditService auditService;

    // NiFi/Airflow 콘솔 per-user 프록시(P5b)용 세션쿠키. 기본 off면 쿠키를 안 심어 동작 변화 없음.
    @Value("${authz.proxy.enabled:false}")
    private boolean proxyEnabled;
    @Value("${authz.proxy.session-cookie:cetl_session}")
    private String sessionCookie;
    @Value("${authz.proxy.same-site:Lax}")
    private String sameSite;
    @Value("${authz.proxy.cookie-max-age-seconds:2592000}")
    private long cookieMaxAgeSeconds;

    public AuthController(
            UserService userService,
            CredentialAuthenticator authenticator,
            PipelineJwtService jwtService,
            AppUserRepository userRepository,
            PasswordEncoder passwordEncoder,
            PermissionAuditService auditService) {
        this.userService = userService;
        this.authenticator = authenticator;
        this.jwtService = jwtService;
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.auditService = auditService;
    }

    public record ChangePasswordRequest(String currentPassword, String newPassword) {
    }

    @PostMapping("/login")
    public ApiResponse<LoginResponse> login(@RequestBody LoginRequest request, HttpServletRequest http,
                                            HttpServletResponse httpResponse) {
        String clientIp = clientIp(http);
        CredentialAuthenticator.AuthenticatedAccount account;
        try {
            account = authenticator.authenticate(request.userId(), request.password());
        } catch (BusinessException ex) {
            String action = ex.getErrorCode() == ErrorCode.ACCOUNT_LOCKED ? "ACCOUNT_LOCKED" : "LOGIN_FAIL";
            safeAudit(request.userId(), action, ex.getErrorCode().name(), clientIp);
            throw ex;
        }

        UserResponse profile = userService.provisionAndGet(account.userId(), account.userNm(), account.email());
        String token = jwtService.issue(account.userId(), account.userNm(), account.email());
        // NiFi/Airflow 콘솔(iframe)은 Authorization 헤더를 못 실으므로, per-user 프록시(P5b)가
        // 켜져 있으면 같은 JWT를 세션쿠키로도 내려 nginx auth_request 가 사용자를 식별하게 한다.
        if (proxyEnabled) {
            setSessionCookie(httpResponse, token);
        }
        safeAudit(account.userId(), "LOGIN_SUCCESS", null, clientIp);
        return ApiResponse.success(new LoginResponse(token, profile));
    }

    private void setSessionCookie(HttpServletResponse response, String token) {
        StringBuilder cookie = new StringBuilder();
        cookie.append(sessionCookie).append('=').append(token)
                .append("; Path=/; HttpOnly; Max-Age=").append(cookieMaxAgeSeconds)
                .append("; SameSite=").append(sameSite);
        // 교차 사이트 iframe(SameSite=None)은 Secure 가 필수다(§7.7 쿠키 주의).
        if ("None".equalsIgnoreCase(sameSite)) {
            cookie.append("; Secure");
        }
        response.addHeader("Set-Cookie", cookie.toString());
    }

    @GetMapping("/me")
    public ApiResponse<UserResponse> me(@AuthenticationPrincipal AppUser user) {
        if (user == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage());
        }
        return ApiResponse.success(UserResponse.from(user));
    }

    /** 본인 비밀번호 변경(설계서 §7.5). 현재 비밀번호 확인 후 새 비밀번호로 교체하고 강제변경 플래그를 내린다. */
    @PostMapping("/password")
    public ApiResponse<Void> changePassword(@AuthenticationPrincipal AppUser principal,
                                            @RequestBody ChangePasswordRequest req) {
        if (principal == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED);
        }
        if (req.newPassword() == null || req.newPassword().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "새 비밀번호는 필수입니다.");
        }
        AppUser user = userRepository.findById(principal.getUserId())
                .orElseThrow(() -> new BusinessException(ErrorCode.UNAUTHORIZED));
        if (user.getUserPw() == null || !passwordEncoder.matches(req.currentPassword(), user.getUserPw())) {
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS, "현재 비밀번호가 올바르지 않습니다.");
        }
        user.setUserPw(passwordEncoder.encode(req.newPassword()));
        user.setPwUpdatedAt(OffsetDateTime.now());
        user.setPwMustChange(false);
        userRepository.save(user);
        safeAudit(user.getUserId(), "PASSWORD_CHANGE", null, null);
        return ApiResponse.success(null);
    }

    private void safeAudit(String userId, String action, String detail, String clientIp) {
        try {
            auditService.record(userId, action, "USER", userId, null, null, detail, clientIp);
        } catch (RuntimeException ignored) {
            // 감사 기록 실패가 로그인/변경 자체를 막지 않는다.
        }
    }

    private String clientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (StringUtils.hasText(forwarded)) {
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
