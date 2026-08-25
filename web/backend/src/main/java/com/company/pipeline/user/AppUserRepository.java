package com.company.pipeline.user;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AppUserRepository extends JpaRepository<AppUser, String> {
    boolean existsByUserId(String userId);

    /**
     * 이메일 중복 검사(자기 자신 제외). app_user 자체는 중복을 허용하지만 Airflow(FAB)는
     * 이메일이 유일값이라, 중복된 채로 두면 개인계정 동기화가 409 로 실패해 그 사용자만
     * 콘솔이 안 열린다(2026-08-25 실사례). 그래서 만들 때 막는다.
     */
    boolean existsByEmailIgnoreCaseAndUserIdNot(String email, String userId);
}
