package com.company.pipeline.user;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.UserResponse;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 로그인/등록 엔드포인트는 없다 - 인증과 사용자 등록은 Keycloak이 담당한다.
 * 여기서는 Keycloak 토큰으로 인증된 사용자의 앱측 프로필만 제공한다.
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final UserService userService;

    public AuthController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/me")
    public ApiResponse<UserResponse> me(@AuthenticationPrincipal Jwt jwt) {
        if (jwt == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage());
        }
        String userId = jwt.getClaimAsString("preferred_username");
        String userNm = jwt.getClaimAsString("name");
        String email = jwt.getClaimAsString("email");
        boolean admin = hasRealmRole(jwt, "portal_admin");
        return ApiResponse.success(userService.provisionAndGet(userId, userNm, email, admin));
    }

    @SuppressWarnings("unchecked")
    private boolean hasRealmRole(Jwt jwt, String role) {
        Object realmAccess = jwt.getClaim("realm_access");
        if (realmAccess instanceof java.util.Map<?, ?> map && map.get("roles") instanceof java.util.Collection<?> roles) {
            return roles.contains(role);
        }
        return false;
    }
}
