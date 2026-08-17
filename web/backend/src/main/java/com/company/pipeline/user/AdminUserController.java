package com.company.pipeline.user;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.UserResponse;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 로컬 사용자 관리 (U3). authz.provider=LOCAL 에서 ST_USER 없이 로그인 계정을 만들고 관리한다.
 * 역할·권한 부여는 이 문서 범위가 아니다(R1) - 여기서는 "로그인 가능한 계정" CRUD 만 한다.
 * 비밀번호는 bcrypt 로만 저장한다(평문 금지).
 */
@RestController
@RequestMapping("/api/admin/users")
public class AdminUserController {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public AdminUserController(AppUserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    public record CreateUserRequest(String userId, String userNm, String email, String password, String adminYn) {}

    public record ResetPasswordRequest(String password) {}

    @GetMapping
    public ApiResponse<List<UserResponse>> list() {
        return ApiResponse.success(userRepository.findAll().stream().map(UserResponse::from).toList());
    }

    @PostMapping
    public ApiResponse<UserResponse> create(@RequestBody CreateUserRequest req) {
        if (req.userId() == null || req.userId().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "아이디는 필수입니다.");
        }
        if (req.password() == null || req.password().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "비밀번호는 필수입니다.");
        }
        if (userRepository.existsByUserId(req.userId())) {
            throw new BusinessException(ErrorCode.USER_ALREADY_EXISTS);
        }
        AppUser user = new AppUser();
        user.setUserId(req.userId());
        user.setUserNm(req.userNm() != null && !req.userNm().isBlank() ? req.userNm() : req.userId());
        user.setEmail(req.email());
        user.setUserPw(passwordEncoder.encode(req.password()));
        user.setPwUpdatedAt(OffsetDateTime.now());
        user.setUseYn("Y");
        user.setAdminYn("Y".equalsIgnoreCase(req.adminYn()) ? "Y" : "N");
        userRepository.save(user);
        return ApiResponse.success(UserResponse.from(user));
    }

    @PutMapping("/{userId}/password")
    public ApiResponse<Void> resetPassword(@PathVariable String userId, @RequestBody ResetPasswordRequest req) {
        if (req.password() == null || req.password().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "비밀번호는 필수입니다.");
        }
        AppUser user = userRepository.findById(userId).orElseThrow(
                () -> new BusinessException(ErrorCode.VALIDATION_ERROR, "사용자를 찾을 수 없습니다: " + userId));
        user.setUserPw(passwordEncoder.encode(req.password()));
        user.setPwUpdatedAt(OffsetDateTime.now());
        user.setLoginFailCount(0);   // 비밀번호 재설정 시 실패 잠금도 해제한다.
        user.setLockedUntil(null);
        userRepository.save(user);
        return ApiResponse.success(null);
    }

    @DeleteMapping("/{userId}")
    public ApiResponse<Void> delete(@PathVariable String userId) {
        userRepository.deleteById(userId);
        return ApiResponse.success(null);
    }
}
