package com.company.pipeline.authz;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 계정 관리(설계서 §7.5-(1)). 상태(사용/비활성)·실패 잠금 해제·비밀번호 초기화까지 다룬다.
 * 비밀번호는 bcrypt 로만 저장하고 관리자에게도 노출하지 않는다. 인가 강제는 P4에서 붙는다.
 *
 * <p>삭제 대신 비활성화(use_yn='N')를 기본으로 둔다 - 실행 이력·감사 로그가 계정을 참조하기 때문.
 */
@RestController
@RequestMapping("/api/admin/accounts")
@RequirePermission(system = SystemCode.ADMIN, bits = AccessBits.WRITE)
public class AdminAccountController {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final PermissionAuditService auditService;

    public AdminAccountController(AppUserRepository userRepository, PasswordEncoder passwordEncoder,
                                  PermissionAuditService auditService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.auditService = auditService;
    }

    public record AccountView(String userId, String userNm, String email, String telNo, boolean admin,
                              String useYn, String lastLoginDt, boolean locked, String lockedUntil,
                              int loginFailCount) {
    }

    public record CreateAccountRequest(String userId, String userNm, String email, String telNo,
                                       String password, boolean admin) {
    }

    public record UpdateAccountRequest(String userNm, String email, String telNo) {
    }

    public record ResetPasswordRequest(String password) {
    }

    @GetMapping
    public ApiResponse<List<AccountView>> list() {
        return ApiResponse.success(userRepository.findAll().stream().map(AdminAccountController::toView).toList());
    }

    @PostMapping
    public ApiResponse<AccountView> create(@RequestBody CreateAccountRequest req, @AuthenticationPrincipal AppUser act) {
        if (req.userId() == null || req.userId().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "아이디는 필수입니다.");
        }
        if (req.password() == null || req.password().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "비밀번호는 필수입니다.");
        }
        if (userRepository.existsByUserId(req.userId())) {
            throw new BusinessException(ErrorCode.USER_ALREADY_EXISTS);
        }
        AppUser user = new AppUser(req.userId());
        user.setUserNm(req.userNm() != null && !req.userNm().isBlank() ? req.userNm() : req.userId());
        user.setEmail(req.email());
        user.setTelNo(req.telNo());
        user.setUserPw(passwordEncoder.encode(req.password()));
        user.setPwUpdatedAt(OffsetDateTime.now());
        user.setUseYn("Y");
        user.setAdminYn(req.admin() ? "Y" : "N");
        userRepository.save(user);
        auditService.record(actorId(act), "CREATE_USER", "USER", user.getUserId(), user.getUserNm());
        return ApiResponse.success(toView(user));
    }

    @PutMapping("/{userId}")
    public ApiResponse<AccountView> update(@PathVariable String userId, @RequestBody UpdateAccountRequest req,
                                           @AuthenticationPrincipal AppUser act) {
        AppUser user = find(userId);
        if (req.userNm() != null && !req.userNm().isBlank()) {
            user.setUserNm(req.userNm());
        }
        user.setEmail(req.email());
        user.setTelNo(req.telNo());
        userRepository.save(user);
        auditService.record(actorId(act), "UPDATE_USER", "USER", userId, user.getUserNm());
        return ApiResponse.success(toView(user));
    }

    @PutMapping("/{userId}/password")
    public ApiResponse<Void> resetPassword(@PathVariable String userId, @RequestBody ResetPasswordRequest req,
                                           @AuthenticationPrincipal AppUser act) {
        if (req.password() == null || req.password().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "비밀번호는 필수입니다.");
        }
        AppUser user = find(userId);
        user.setUserPw(passwordEncoder.encode(req.password()));
        user.setPwUpdatedAt(OffsetDateTime.now());
        user.setPwMustChange(true);   // 초기화 후 다음 로그인 때 변경 요구(설계서 §7.5-(1))
        user.setLoginFailCount(0);
        user.setLockedUntil(null);
        userRepository.save(user);
        auditService.record(actorId(act), "RESET_PASSWORD", "USER", userId, null);
        return ApiResponse.success(null);
    }

    @PutMapping("/{userId}/disable")
    public ApiResponse<Void> disable(@PathVariable String userId, @AuthenticationPrincipal AppUser act) {
        AppUser user = find(userId);
        user.setUseYn("N");
        userRepository.save(user);
        auditService.record(actorId(act), "DISABLE_USER", "USER", userId, null);
        return ApiResponse.success(null);
    }

    @PutMapping("/{userId}/enable")
    public ApiResponse<Void> enable(@PathVariable String userId, @AuthenticationPrincipal AppUser act) {
        AppUser user = find(userId);
        user.setUseYn("Y");
        userRepository.save(user);
        auditService.record(actorId(act), "ENABLE_USER", "USER", userId, null);
        return ApiResponse.success(null);
    }

    @PutMapping("/{userId}/unlock")
    public ApiResponse<Void> unlock(@PathVariable String userId, @AuthenticationPrincipal AppUser act) {
        AppUser user = find(userId);
        user.setLoginFailCount(0);
        user.setLockedUntil(null);
        userRepository.save(user);
        auditService.record(actorId(act), "UNLOCK", "USER", userId, null);
        return ApiResponse.success(null);
    }

    private AppUser find(String userId) {
        return userRepository.findById(userId).orElseThrow(() ->
                new BusinessException(ErrorCode.VALIDATION_ERROR, "사용자를 찾을 수 없습니다: " + userId));
    }

    private String actorId(AppUser actor) {
        return actor == null ? null : actor.getUserId();
    }

    private static AccountView toView(AppUser u) {
        boolean locked = u.getLockedUntil() != null && u.getLockedUntil().isAfter(OffsetDateTime.now());
        return new AccountView(
                u.getUserId(), u.getUserNm(), u.getEmail(), u.getTelNo(), u.isAdmin(),
                u.getUseYn(),
                u.getLastLoginDt() == null ? null : u.getLastLoginDt().toString(),
                locked,
                u.getLockedUntil() == null ? null : u.getLockedUntil().toString(),
                u.getLoginFailCount());
    }
}
