package com.company.pipeline.user;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.LoginRequest;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.UserResponse;
import com.company.pipeline.user.security.PipelineJwtService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * cerebroetl-ui 자체 로그인. 인증원은 authz.provider 에 따라 결정된다(U3, C9):
 * LOCAL=app_user(bcrypt) / EXTERNAL=사내 ST_USER. 토큰은 어느 쪽이든 이 앱이 직접 발급한다.
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final UserService userService;
    private final CredentialAuthenticator authenticator;
    private final PipelineJwtService jwtService;

    public AuthController(
            UserService userService,
            CredentialAuthenticator authenticator,
            PipelineJwtService jwtService) {
        this.userService = userService;
        this.authenticator = authenticator;
        this.jwtService = jwtService;
    }

    @PostMapping("/login")
    public ApiResponse<LoginResponse> login(@RequestBody LoginRequest request) {
        CredentialAuthenticator.AuthenticatedAccount account =
                authenticator.authenticate(request.userId(), request.password());

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
