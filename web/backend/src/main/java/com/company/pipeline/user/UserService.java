package com.company.pipeline.user;

import com.company.pipeline.user.dto.UserResponse;
import java.time.LocalDateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final AppUserRepository userRepository;

    public UserService(AppUserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * Keycloak으로 인증된 사용자의 앱측 프로필을 반환한다. app_user에 행이 없으면
     * (첫 로그인) Keycloak 신원 정보로 자동 프로비저닝한다 - app_user는 인증이 아니라
     * 본부/직함/관리자여부/사용기간 같은 "앱 프로필·권한"을 담는 저장소로만 쓴다.
     */
    @Transactional
    public UserResponse provisionAndGet(String userId, String userNm, String email, boolean admin) {
        AppUser user = userRepository.findById(userId).orElseGet(() -> {
            AppUser created = new AppUser();
            created.setUserId(userId);
            created.setUserNm(userNm != null ? userNm : userId);
            created.setEmail(email);
            created.setUseYn("Y"); // Keycloak이 이미 인증했으므로 활성 상태로 생성
            return created;
        });

        // Keycloak realm role 기준 관리자여부를 매 로그인마다 반영(권한의 단일 소스는 Keycloak)
        user.setAdminYn(admin ? "Y" : "N");
        if (email != null) {
            user.setEmail(email);
        }
        user.setLastLoginDt(LocalDateTime.now());
        user.setUpdDt(LocalDateTime.now());
        userRepository.save(user);
        return UserResponse.from(user);
    }
}
