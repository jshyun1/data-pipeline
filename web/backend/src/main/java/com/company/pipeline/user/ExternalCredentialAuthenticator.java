package com.company.pipeline.user;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * 사내 ST_USER(MySQL) 기반 인증 (U3, EXTERNAL). Keycloak 제거 후의 기존 로그인 동작을 그대로
 * 유지한다 - 여러 시스템이 공유하는 실 사용자 디렉터리를 조회만 하고 실패 잠금은 두지 않는다
 * (ST_USER 에 잠금 컬럼이 없다 - C9의 미결을 LOCAL 쪽 app_user 4컬럼이 메운다).
 */
@Component
@ConditionalOnProperty(name = "authz.provider", havingValue = "EXTERNAL")
public class ExternalCredentialAuthenticator implements CredentialAuthenticator {

    private final AccountLookupService accountLookupService;
    private final PasswordEncoder passwordEncoder;

    public ExternalCredentialAuthenticator(AccountLookupService accountLookupService,
                                           PasswordEncoder passwordEncoder) {
        this.accountLookupService = accountLookupService;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public AuthenticatedAccount authenticate(String userId, String rawPassword) {
        AccountLookupService.Account account = accountLookupService.findById(userId);
        if (account == null || account.userPw() == null
                || !passwordEncoder.matches(rawPassword, account.userPw())) {
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
        }
        if (!account.isUsable()) {
            throw new BusinessException(ErrorCode.ACCOUNT_NOT_APPROVED);
        }
        return new AuthenticatedAccount(account.userId(), account.userNm(), account.email());
    }
}
