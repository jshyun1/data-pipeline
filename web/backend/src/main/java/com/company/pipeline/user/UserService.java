package com.company.pipeline.user;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.dto.LoginRequest;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.RegisterRequest;
import com.company.pipeline.user.dto.UserResponse;
import com.company.pipeline.user.security.JwtService;
import java.time.LocalDateTime;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;

    public UserService(AppUserRepository userRepository, PasswordEncoder passwordEncoder, JwtService jwtService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
    }

    @Transactional
    public LoginResponse login(LoginRequest request) {
        AppUser user = userRepository.findById(request.userId())
                // 아이디 존재 여부를 노출하지 않도록 비밀번호 불일치와 동일한 에러로 응답
                .orElseThrow(() -> new BusinessException(ErrorCode.INVALID_CREDENTIALS,
                        ErrorCode.INVALID_CREDENTIALS.getDefaultMessage()));

        if (!passwordEncoder.matches(request.password(), user.getUserPw())) {
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS,
                    ErrorCode.INVALID_CREDENTIALS.getDefaultMessage());
        }
        if (!user.isLoginAllowed(LocalDateTime.now())) {
            throw new BusinessException(ErrorCode.ACCOUNT_NOT_APPROVED,
                    ErrorCode.ACCOUNT_NOT_APPROVED.getDefaultMessage());
        }

        user.setLastLoginDt(LocalDateTime.now());
        String token = jwtService.issue(user.getUserId(), user.isAdmin());
        return new LoginResponse(token, jwtService.expirationMinutes(), UserResponse.from(user));
    }

    /** 등록신청: 관리자 승인 전까지 use_yn='N'으로 생성한다(승인은 관리자가 별도로 처리). */
    @Transactional
    public void register(RegisterRequest request) {
        if (userRepository.existsByUserId(request.userId())) {
            throw new BusinessException(ErrorCode.USER_ALREADY_EXISTS,
                    ErrorCode.USER_ALREADY_EXISTS.getDefaultMessage());
        }
        AppUser user = new AppUser();
        user.setUserId(request.userId());
        user.setUserNm(request.userNm());
        user.setUserPw(passwordEncoder.encode(request.password()));
        user.setEmail(request.email());
        user.setTelNo(request.telNo());
        user.setHqCd(request.hqCd());
        user.setPositionCd(request.positionCd());
        user.setAdminYn("N");
        user.setUseYn("N"); // 승인 대기
        userRepository.save(user);
    }

    @Transactional(readOnly = true)
    public UserResponse getByUserId(String userId) {
        AppUser user = userRepository.findById(userId)
                .orElseThrow(() -> new BusinessException(ErrorCode.UNAUTHORIZED,
                        ErrorCode.UNAUTHORIZED.getDefaultMessage()));
        return UserResponse.from(user);
    }
}
