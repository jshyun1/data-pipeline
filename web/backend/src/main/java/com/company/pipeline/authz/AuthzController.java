package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 로그인한 사용자 자신의 권한/메뉴 조회(설계서 §7.1). 프론트가 로그인 직후 이걸 읽어
 * 사이드바 메뉴와 버튼 활성/비활성을 그린다.
 *
 * <p>인가 강제 단계(P4) 전이라 SecurityConfig 는 건드리지 않는다. 대신 유효한 토큰이 없어
 * principal 이 없으면 401 로 응답한다(JWT 필터가 토큰이 있을 때만 principal 을 채운다).
 */
@RestController
@RequestMapping("/api/authz")
public class AuthzController {

    private final PermissionService permissionService;

    public AuthzController(PermissionService permissionService) {
        this.permissionService = permissionService;
    }

    public record MeResponse(String userId, String userNm, String email, boolean admin,
                             boolean pwMustChange, Set<String> roles, Map<String, Integer> systemBits) {
    }

    @GetMapping("/me")
    public ApiResponse<MeResponse> me(@AuthenticationPrincipal AppUser user) {
        if (user == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED);
        }
        UserPermissions up = permissionService.resolve(user.getUserId());
        Map<String, Integer> bits = new LinkedHashMap<>();
        up.systemBits().forEach((k, v) -> bits.put(k.name(), v));
        return ApiResponse.success(new MeResponse(
                user.getUserId(), user.getUserNm(), user.getEmail(), up.admin(),
                user.isPwMustChange(), up.roles(), bits));
    }

    @GetMapping("/menus")
    public ApiResponse<List<MenuNode>> menus(@AuthenticationPrincipal AppUser user) {
        if (user == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED);
        }
        return ApiResponse.success(permissionService.resolve(user.getUserId()).menus());
    }
}
