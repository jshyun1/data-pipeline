package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.AppUser;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 역할 및 시스템 권한 관리(설계서 §7.5-(2)). 인가 강제는 P4에서 붙는다 - 현 단계는 CRUD 제공.
 */
@RestController
@RequestMapping("/api/admin/roles")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.WRITE)
public class AdminRoleController {

    private final AuthzAdminService adminService;

    public AdminRoleController(AuthzAdminService adminService) {
        this.adminService = adminService;
    }

    public record CreateRoleRequest(String roleId, String roleNm, String roleDesc) {
    }

    public record UpdateRoleRequest(String roleNm, String roleDesc) {
    }

    @GetMapping
    public ApiResponse<List<AuthzAdminService.RoleView>> list() {
        return ApiResponse.success(adminService.listRoles());
    }

    @GetMapping("/{roleId}")
    public ApiResponse<AuthzAdminService.RoleDetail> get(@PathVariable String roleId) {
        return ApiResponse.success(adminService.getRole(roleId));
    }

    @PostMapping
    public ApiResponse<Void> create(@RequestBody CreateRoleRequest req, @AuthenticationPrincipal AppUser actor) {
        adminService.createRole(req.roleId(), req.roleNm(), req.roleDesc(), actorId(actor));
        return ApiResponse.success(null);
    }

    @PutMapping("/{roleId}")
    public ApiResponse<Void> update(@PathVariable String roleId, @RequestBody UpdateRoleRequest req,
                                    @AuthenticationPrincipal AppUser actor) {
        adminService.updateRole(roleId, req.roleNm(), req.roleDesc(), actorId(actor));
        return ApiResponse.success(null);
    }

    @DeleteMapping("/{roleId}")
    public ApiResponse<Void> delete(@PathVariable String roleId, @AuthenticationPrincipal AppUser actor) {
        adminService.deleteRole(roleId, actorId(actor));
        return ApiResponse.success(null);
    }

    @PutMapping("/{roleId}/permissions")
    public ApiResponse<Void> setPermissions(@PathVariable String roleId, @RequestBody Map<String, Integer> systemBits,
                                            @AuthenticationPrincipal AppUser actor) {
        adminService.setRolePermissions(roleId, systemBits, actorId(actor));
        return ApiResponse.success(null);
    }

    @PutMapping("/{roleId}/menus")
    public ApiResponse<Void> setMenuOverrides(@PathVariable String roleId, @RequestBody Map<String, Boolean> overrides,
                                              @AuthenticationPrincipal AppUser actor) {
        adminService.setRoleMenuOverrides(roleId, overrides, actorId(actor));
        return ApiResponse.success(null);
    }

    private String actorId(AppUser actor) {
        return actor == null ? null : actor.getUserId();
    }
}
