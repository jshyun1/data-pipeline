package com.company.pipeline.user;

/**
 * 로그인 자격증명 검증 추상화 (U3, 설계서 C9).
 *
 * <p>authz.provider 에 따라 정확히 하나의 구현이 빈으로 등록된다:
 * <ul>
 *   <li>LOCAL    → {@link LocalCredentialAuthenticator} (app_user bcrypt + 실패 잠금, 상용 기본)</li>
 *   <li>EXTERNAL → {@link ExternalCredentialAuthenticator} (사내 ST_USER, MySQL)</li>
 * </ul>
 * 검증 실패는 BusinessException 으로 던진다(INVALID_CREDENTIALS / ACCOUNT_NOT_APPROVED / ACCOUNT_LOCKED).
 */
public interface CredentialAuthenticator {

    /** 성공 시 인증된 계정 정보를 반환하고, 실패 시 BusinessException 을 던진다. */
    AuthenticatedAccount authenticate(String userId, String rawPassword);

    record AuthenticatedAccount(String userId, String userNm, String email) {}
}
