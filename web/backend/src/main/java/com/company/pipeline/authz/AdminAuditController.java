package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 감사 로그 조회(설계서 §7.8). 인가 강제는 P4에서 붙는다.
 */
@RestController
@RequestMapping("/api/admin/audit")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.READ)
public class AdminAuditController {

    private final AuthzAdminService adminService;

    public AdminAuditController(AuthzAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public ApiResponse<List<AuthzAdminService.AuditView>> list(
            @RequestParam(name = "limit", defaultValue = "200") int limit) {
        return ApiResponse.success(adminService.listAudit(limit));
    }
}
