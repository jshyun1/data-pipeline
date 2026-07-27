package com.company.pipeline.user;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.LoginRequest;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.UserResponse;
import com.company.pipeline.user.security.PipelineJwtService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * cerebroetl-ui 자체 로그인. 인증원은 통합 계정 테이블(ST_USER, MySQL) - Keycloak 제거 후
 * web-client-svr(dw-app01-svr)과 동일한 계정 테이블을 보되, 토큰은 이 앱이 직접 발급한다.
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final UserService userService;
    private final AccountLookupService accountLookupService;
    private final PasswordEncoder passwordEncoder;
    private final PipelineJwtService jwtService;

    public AuthController(
            UserService userService,
            AccountLookupService accountLookupService,
            PasswordEncoder passwordEncoder,
            PipelineJwtService jwtService) {
        this.userService = userService;
        this.accountLookupService = accountLookupService;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
    }

    @PostMapping("/login")
    public ApiResponse<LoginResponse> login(@RequestBody LoginRequest request) {
        AccountLookupService.Account account = accountLookupService.findById(request.userId());
        if (account == null || account.userPw() == null
                || !passwordEncoder.matches(request.password(), account.userPw())) {
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
        }
        if (!account.isUsable()) {
            throw new BusinessException(ErrorCode.ACCOUNT_NOT_APPROVED);
        }

        UserResponse profile = userService.provisionAndGet(account.userId(), account.userNm(), account.email());
        String token = jwtService.issue(account.userId(), account.userNm(), account.email());
        return ApiResponse.success(new LoginResponse(token, profile));
    }

    @GetMapping("/me")
    public ApiResponse<UserResponse> me(@AuthenticationPrincipal AppUser user) {
        if (user == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage());
        }
        return ApiResponse.success(UserResponse.from(user));
    }
}
