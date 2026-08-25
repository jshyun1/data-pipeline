package com.company.pipeline.authz.portal;

import com.company.pipeline.authz.AppRoleRepository;
import com.company.pipeline.authz.AppUserRole;
import com.company.pipeline.authz.AppUserRoleRepository;
import com.company.pipeline.authz.PermissionAuditService;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import com.company.pipeline.user.UserService;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.UserResponse;
import com.company.pipeline.user.security.PipelineJwtService;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * MSA 포털(app02) 토큰 → Cerebro 토큰 교환 (P4.5).
 *
 * <p>배경: 포털은 pipeline-api 를 서버사이드에서 <b>무인증</b>으로 프록시한다. 인가 강제
 * ({@code authz.enforcement.enabled})를 켜면 포털 ETL 섹션 6개 도메인이 전부 401 이 된다.
 * 포털 JWT 는 발급자(issuer)와 서명키가 달라 Cerebro 에서 그대로 통하지 않는다.
 *
 * <p>그래서 app02 가 이 종단점으로 <b>포털 JWT 를 한 번 교환</b>해 Cerebro JWT 를 받고,
 * 그 토큰을 프록시 호출에 실어 보낸다. 신뢰 근거는 <b>내부망 + 공유 서명키</b>다.
 *
 * <p>프로필(userNm/email)은 포털 토큰의 claim 에서 받는다. Cerebro 가 사내 MySQL
 * {@code ST_USER} 에 직접 붙지 않으므로 <b>폐쇄망 단독 배포 조건이 유지</b>된다.
 *
 * <p>역할 주권 경계: 포털 역할은 "ETL 섹션에 들어올 수 있는가"까지만 판단한다. 리소스
 * read/write 세부 판단은 Cerebro RBAC 가 최종이다. 그래서 신규 사용자는 조회 전용
 * 기본 역할로만 프로비저닝하고, 쓰기 승격은 Cerebro 관리자가 한다.
 */
@Service
@ConditionalOnProperty(name = "portal.integration.enabled", havingValue = "true")
public class PortalTokenExchangeService {

    private static final Logger log = LoggerFactory.getLogger(PortalTokenExchangeService.class);
    private static final String BEARER = "Bearer ";

    private final SecretKey portalKey;
    private final String defaultRoleId;
    private final UserService userService;
    private final AppUserRepository userRepository;
    private final AppUserRoleRepository userRoleRepository;
    private final AppRoleRepository roleRepository;
    private final PipelineJwtService jwtService;
    private final PermissionAuditService auditService;

    public PortalTokenExchangeService(
            @Value("${portal.integration.jwt-secret:}") String portalSecret,
            @Value("${portal.integration.default-role:ROLE_ETL_VIEWER}") String defaultRoleId,
            UserService userService,
            AppUserRepository userRepository,
            AppUserRoleRepository userRoleRepository,
            AppRoleRepository roleRepository,
            PipelineJwtService jwtService,
            PermissionAuditService auditService) {
        if (!StringUtils.hasText(portalSecret)) {
            // 켜 놓고 키가 비면 "교환은 되는데 아무나 된다" 가 아니라 기동 자체를 막는다.
            throw new IllegalStateException(
                    "portal.integration.enabled=true 인데 portal.integration.jwt-secret 이 비어 있다. "
                            + "app02 의 jwt.secret 과 같은 값을 PORTAL_JWT_SECRET 으로 주입할 것.");
        }
        this.portalKey = Keys.hmacShaKeyFor(portalSecret.getBytes(StandardCharsets.UTF_8));
        this.defaultRoleId = defaultRoleId;
        this.userService = userService;
        this.userRepository = userRepository;
        this.userRoleRepository = userRoleRepository;
        this.roleRepository = roleRepository;
        this.jwtService = jwtService;
        this.auditService = auditService;
    }

    /**
     * 포털 JWT 를 검증하고 Cerebro JWT 를 발급한다. 계정이 없으면 조회 전용으로 생성한다.
     *
     * @param rawToken 포털 JWT. {@code Bearer } 접두사가 붙어 있어도 된다.
     */
    @Transactional
    public LoginResponse exchange(String rawToken) {
        Claims claims = verify(rawToken);

        String userId = claims.getSubject();
        if (!StringUtils.hasText(userId)) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, "포털 토큰에 사용자 식별자가 없습니다.");
        }
        String userNm = claims.get("userNm", String.class);
        String email = claims.get("email", String.class);

        // 비활성 계정은 되살리지 않는다. provisionAndGet 은 신규 생성 시에만 useYn 을 Y 로 두므로
        // 이 검사가 없으면 관리자가 막아 둔 계정이 포털 경유로 계속 토큰을 받게 된다.
        AppUser existing = userRepository.findById(userId).orElse(null);
        if (existing != null && !"Y".equals(existing.getUseYn())) {
            audit(userId, "PORTAL_EXCHANGE_DENIED", "비활성 계정");
            throw new BusinessException(ErrorCode.ACCOUNT_NOT_APPROVED);
        }
        boolean newAccount = existing == null;

        UserResponse profile = userService.provisionAndGet(userId, userNm, email);
        boolean roleGranted = grantDefaultRoleIfNone(userId);

        String token = jwtService.issue(profile.userId(), profile.userNm(), profile.email());
        audit(userId, "PORTAL_EXCHANGE",
                newAccount ? "신규 프로비저닝" + (roleGranted ? " + 기본역할 " + defaultRoleId : "")
                        : (roleGranted ? "기본역할 " + defaultRoleId : "기존 계정"));
        return new LoginResponse(token, profile);
    }

    private Claims verify(String rawToken) {
        String token = rawToken == null ? "" : rawToken.trim();
        if (token.regionMatches(true, 0, BEARER, 0, BEARER.length())) {
            token = token.substring(BEARER.length()).trim();
        }
        if (!StringUtils.hasText(token)) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, "포털 토큰이 비어 있습니다.");
        }
        try {
            return Jwts.parser().verifyWith(portalKey).build().parseSignedClaims(token).getPayload();
        } catch (JwtException e) {
            // 서명 불일치와 만료를 구분해 알려주지 않는다(탐색 힌트가 된다). 원인은 로그에만 남긴다.
            log.warn("포털 토큰 검증 실패: {}", e.getMessage());
            throw new BusinessException(ErrorCode.UNAUTHORIZED, "포털 토큰이 유효하지 않습니다.");
        }
    }

    /**
     * 역할이 하나도 없을 때만 기본 역할을 준다. 이미 역할이 있으면(관리자가 승격해 둔 경우 포함)
     * 손대지 않는다 - 매 교환마다 조회 전용으로 되돌리면 승격이 무의미해진다.
     */
    private boolean grantDefaultRoleIfNone(String userId) {
        if (!userRoleRepository.findByUserId(userId).isEmpty()) {
            return false;
        }
        if (!roleRepository.existsById(defaultRoleId)) {
            log.warn("기본 역할 {} 이 없어 역할 없이 프로비저닝한다(userId={})", defaultRoleId, userId);
            return false;
        }
        userRoleRepository.save(new AppUserRole(userId, defaultRoleId, "portal"));
        return true;
    }

    private void audit(String userId, String action, String detail) {
        auditService.record(userId, action, "USER", userId, detail);
    }
}
