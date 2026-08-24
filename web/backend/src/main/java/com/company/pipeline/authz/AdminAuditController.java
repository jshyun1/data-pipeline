package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 감사 로그 조회(설계서 §7.8). 인가 강제는 P4에서 붙는다.
 *
 * <p>기간(from~to)으로 서버에서 걸러 최신순으로 내려주고, 동작/검색어 필터와 페이징은 프런트에서
 * 처리한다(CDC 처리 로그 등 다른 로그 화면과 동일한 방식). from/to 미지정 시 최근 7일.
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
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
            @RequestParam(name = "limit", defaultValue = "2000") int limit) {
        LocalDateTime toValue = to != null ? to : LocalDateTime.now();
        LocalDateTime fromValue = from != null ? from : toValue.minusDays(7);
        return ApiResponse.success(adminService.listAudit(fromValue, toValue, limit));
    }
}
