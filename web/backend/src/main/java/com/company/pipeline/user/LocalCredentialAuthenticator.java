package com.company.pipeline.user;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * 로컬 인증 (U3, 설계서 C9). app_user.user_pw(bcrypt) 로 검증하고 실패 잠금을 관리한다.
 * ST_USER 가 없는 고객사가 부팅·로그인 하도록 하는 상용 기본 경로다. 연속 실패/잠금시간 임계는
 * SettingKey(auth.login.*)로 두어 하드코딩하지 않는다(원칙 D).
 */
@Component
@ConditionalOnProperty(name = "authz.provider", havingValue = "LOCAL", matchIfMissing = true)
public class LocalCredentialAuthenticator implements CredentialAuthenticator {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final SettingService settings;

    public LocalCredentialAuthenticator(AppUserRepository userRepository,
                                        PasswordEncoder passwordEncoder,
                                        SettingService settings) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.settings = settings;
    }

    // @Transactional 을 걸지 않는다: 실패 시 INVALID_CREDENTIALS(RuntimeException)를 던지는데,
    // 하나의 트랜잭션이면 그 예외가 방금 올린 login_fail_count 증가까지 롤백해 잠금이 영원히 안 걸린다.
    // 각 userRepository.save() 가 리포지토리 레벨에서 독립 커밋되도록 트랜잭션 경계를 두지 않는다.
    @Override
    public AuthenticatedAccount authenticate(String userId, String rawPassword) {
        AppUser user = userRepository.findById(userId).orElse(null);
        if (user == null) {
            // 존재하지 않는 계정도 동일 메시지로 응답한다(사용자 열거 방지). 잠글 대상이 없다.
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
        }
        if (user.getLockedUntil() != null && OffsetDateTime.now().isBefore(user.getLockedUntil())) {
            throw new BusinessException(ErrorCode.ACCOUNT_LOCKED);
        }
        if (user.getUserPw() == null || !passwordEncoder.matches(rawPassword, user.getUserPw())) {
            registerFailure(user);
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
        }
        if (!"Y".equalsIgnoreCase(user.getUseYn())) {
            throw new BusinessException(ErrorCode.ACCOUNT_NOT_APPROVED);
        }
        // 성공: 실패 카운터·잠금 해제 + 마지막 로그인 기록.
        user.setLoginFailCount(0);
        user.setLockedUntil(null);
        user.setLastLoginDt(LocalDateTime.now());
        userRepository.save(user);
        return new AuthenticatedAccount(user.getUserId(), user.getUserNm(), user.getEmail());
    }

    private void registerFailure(AppUser user) {
        int lockCount = settings.getInt(SettingKey.AUTH_LOGIN_FAIL_LOCK_COUNT);
        int lockMinutes = settings.getInt(SettingKey.AUTH_LOGIN_LOCK_MINUTES);
        int fails = user.getLoginFailCount() + 1;
        user.setLoginFailCount(fails);
        if (fails >= lockCount) {
            user.setLockedUntil(OffsetDateTime.now().plusMinutes(lockMinutes));
        }
        userRepository.save(user);
    }
}
