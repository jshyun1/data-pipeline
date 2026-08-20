package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 메뉴 카탈로그 조회 - 역할별 메뉴 노출 재정의 편집기(설계서 §7.5-(2))에서 쓴다.
 */
@RestController
@RequestMapping("/api/admin/menus")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.READ)
public class AdminMenuController {

    private final AuthzAdminService adminService;

    public AdminMenuController(AuthzAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public ApiResponse<List<AuthzAdminService.MenuView>> list() {
        return ApiResponse.success(adminService.listMenus());
    }
}
