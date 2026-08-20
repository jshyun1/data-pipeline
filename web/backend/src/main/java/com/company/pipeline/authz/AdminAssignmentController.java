package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.AppUser;
import java.util.List;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 사용자 ↔ 역할 배정(설계서 §7.5-(3)). 인가 강제는 P4에서 붙는다.
 */
@RestController
@RequestMapping("/api/admin/assignments")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.WRITE)
public class AdminAssignmentController {

    private final AuthzAdminService adminService;

    public AdminAssignmentController(AuthzAdminService adminService) {
        this.adminService = adminService;
    }

    public record SetRolesRequest(List<String> roleIds) {
    }

    @GetMapping
    public ApiResponse<List<AuthzAdminService.AssignmentView>> list() {
        return ApiResponse.success(adminService.listAssignments());
    }

    @PutMapping("/{userId}")
    public ApiResponse<Void> setRoles(@PathVariable String userId, @RequestBody SetRolesRequest req,
                                      @AuthenticationPrincipal AppUser actor) {
        adminService.setUserRoles(userId, req.roleIds(), actor == null ? null : actor.getUserId());
        return ApiResponse.success(null);
    }
}
