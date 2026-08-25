package com.company.pipeline.authz.portal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** P4.5 포털 토큰 교환. 서명키가 다른 토큰을 받아 Cerebro 토큰으로 바꿔주는 신뢰 경계다. */
@ExtendWith(MockitoExtension.class)
class PortalTokenExchangeServiceTest {

    private static final String PORTAL_SECRET = "portal-secret-key-for-tests-0123456789abcdef";
    private static final String OTHER_SECRET = "some-other-secret-key-0123456789abcdefghij";
    private static final String USER_ID = "hong";

    @Mock
    private UserService userService;
    @Mock
    private AppUserRepository userRepository;
    @Mock
    private AppUserRoleRepository userRoleRepository;
    @Mock
    private AppRoleRepository roleRepository;
    @Mock
    private PipelineJwtService jwtService;
    @Mock
    private PermissionAuditService auditService;

    private PortalTokenExchangeService service() {
        return new PortalTokenExchangeService(PORTAL_SECRET, "ROLE_ETL_VIEWER", userService,
                userRepository, userRoleRepository, roleRepository, jwtService, auditService);
    }

    private static String portalToken(String secret, String subject, String userNm, String email) {
        SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return Jwts.builder()
                .subject(subject)
                .claim("userNm", userNm)
                .claim("email", email)
                .expiration(new Date(System.currentTimeMillis() + 60_000))
                .signWith(key)
                .compact();
    }

    private void stubProvisioning() {
        lenient().when(userService.provisionAndGet(anyString(), any(), any()))
                .thenReturn(new UserResponse(USER_ID, "홍길동", "hong@dw.kr", null, null, null, false));
        lenient().when(jwtService.issue(anyString(), any(), any())).thenReturn("cerebro-token");
    }

    @Test
    void 포털토큰을_교환하면_신규계정을_조회전용으로_만들고_cerebro토큰을_준다() {
        stubProvisioning();
        when(userRepository.findById(USER_ID)).thenReturn(Optional.empty());
        when(userRoleRepository.findByUserId(USER_ID)).thenReturn(List.of());
        when(roleRepository.existsById("ROLE_ETL_VIEWER")).thenReturn(true);

        LoginResponse response = service().exchange(portalToken(PORTAL_SECRET, USER_ID, "홍길동", "hong@dw.kr"));

        assertThat(response.token()).isEqualTo("cerebro-token");
        verify(userService).provisionAndGet(USER_ID, "홍길동", "hong@dw.kr");

        ArgumentCaptor<AppUserRole> granted = ArgumentCaptor.forClass(AppUserRole.class);
        verify(userRoleRepository).save(granted.capture());
        assertThat(granted.getValue().getRoleId()).isEqualTo("ROLE_ETL_VIEWER");
        assertThat(granted.getValue().getUserId()).isEqualTo(USER_ID);
    }

    @Test
    void Bearer_접두사가_붙어_있어도_교환된다() {
        stubProvisioning();
        when(userRepository.findById(USER_ID)).thenReturn(Optional.empty());
        when(userRoleRepository.findByUserId(USER_ID)).thenReturn(List.of());
        when(roleRepository.existsById("ROLE_ETL_VIEWER")).thenReturn(true);

        LoginResponse response =
                service().exchange("Bearer " + portalToken(PORTAL_SECRET, USER_ID, "홍길동", "hong@dw.kr"));

        assertThat(response.token()).isEqualTo("cerebro-token");
    }

    @Test
    void 이미_역할이_있으면_기본역할로_되돌리지_않는다() {
        stubProvisioning();
        AppUser existing = new AppUser(USER_ID);
        existing.setUseYn("Y");
        when(userRepository.findById(USER_ID)).thenReturn(Optional.of(existing));
        when(userRoleRepository.findByUserId(USER_ID))
                .thenReturn(List.of(new AppUserRole(USER_ID, "ROLE_ETL_ADMIN", "admin")));

        service().exchange(portalToken(PORTAL_SECRET, USER_ID, "홍길동", "hong@dw.kr"));

        verify(userRoleRepository, never()).save(any());
    }

    @Test
    void 서명키가_다르면_거부한다() {
        assertThatThrownBy(() -> service().exchange(portalToken(OTHER_SECRET, USER_ID, "홍길동", "hong@dw.kr")))
                .isInstanceOf(BusinessException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNAUTHORIZED);

        verify(userService, never()).provisionAndGet(anyString(), any(), any());
    }

    @Test
    void 만료된_토큰은_거부한다() {
        SecretKey key = Keys.hmacShaKeyFor(PORTAL_SECRET.getBytes(StandardCharsets.UTF_8));
        String expired = Jwts.builder()
                .subject(USER_ID)
                .expiration(new Date(System.currentTimeMillis() - 1000))
                .signWith(key)
                .compact();

        assertThatThrownBy(() -> service().exchange(expired))
                .isInstanceOf(BusinessException.class);
        verify(userService, never()).provisionAndGet(anyString(), any(), any());
    }

    @Test
    void 비활성_계정은_포털경유로도_되살아나지_않는다() {
        AppUser disabled = new AppUser(USER_ID);
        disabled.setUseYn("N");
        when(userRepository.findById(USER_ID)).thenReturn(Optional.of(disabled));

        assertThatThrownBy(() -> service().exchange(portalToken(PORTAL_SECRET, USER_ID, "홍길동", "hong@dw.kr")))
                .isInstanceOf(BusinessException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCode.ACCOUNT_NOT_APPROVED);

        verify(userService, never()).provisionAndGet(anyString(), any(), any());
    }

    @Test
    void 키가_비어있으면_기동을_막는다() {
        assertThatThrownBy(() -> new PortalTokenExchangeService("", "ROLE_ETL_VIEWER", userService,
                userRepository, userRoleRepository, roleRepository, jwtService, auditService))
                .isInstanceOf(IllegalStateException.class);
    }
}
