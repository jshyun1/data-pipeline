package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.AppUser;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 사용자 ↔ 역할 배정(설계서 §7.5-(3)). 인가 강제는 P4에서 붙는다.
 */
@RestController
@RequestMapping("/api/admin/assignments")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.WRITE, audit = false)
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

    /**
     * 역할 배정 결과.
     *
     * @param syncWarnings NiFi/Airflow 개인계정 동기화 실패 사유. 비어 있으면 정상이다.
     *                     동기화는 best-effort 라 실패해도 역할 배정 자체는 성공하지만, 그
     *                     사용자는 콘솔이 안 열린다 - 화면에서 바로 알 수 있게 함께 내려준다.
     */
    public record SetRolesResponse(List<String> syncWarnings) {
    }

    @PutMapping("/{userId}")
    public ApiResponse<SetRolesResponse> setRoles(@PathVariable String userId, @RequestBody SetRolesRequest req,
                                                  @AuthenticationPrincipal AppUser actor) {
        List<String> warnings =
                adminService.setUserRoles(userId, req.roleIds(), actor == null ? null : actor.getUserId());
        return ApiResponse.success(new SetRolesResponse(warnings));
    }

    /**
     * 전체 사용자를 NiFi/Airflow 개인계정으로 일괄 재조정한다(P5b 도입 전 배정된 기존 사용자 catch-up).
     * identity-sync 기능이 off면 대상수만 세고 실제 동기화는 하지 않는다(성공수 0).
     */
    @PostMapping("/sync-identities")
    public ApiResponse<Map<String, Integer>> syncAll(@AuthenticationPrincipal AppUser actor) {
        return ApiResponse.success(adminService.syncAllIdentities(actor == null ? null : actor.getUserId()));
    }
}
